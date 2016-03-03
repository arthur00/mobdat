'''
Created on Feb 19, 2016

@author: arthurvaladares
'''
from uuid import uuid4
from django.utils.unittest.compatibility import wraps
from flask import Flask, request
from flask.helpers import make_response
from flask_restful import Api, Resource, reqparse
import json
import logging, logging.handlers
import new
import os
from uuid import UUID

from cadis.language.schema import CADISEncoder, CADIS
from cadis.store.simplestore import SimpleStore
from mobdat.common.ValueTypes import Vector3
from mobdat.simulator.DataModel import *
from prime.PrimeDataModel import *

logger = None

parser = reqparse.RequestParser()
parser.add_argument('update_dict')
parser.add_argument('insert_list')
parser.add_argument('obj')

class FlaskConfig(object):
    RESTFUL_JSON = {}

    @staticmethod
    def init_app(app):
        app.config['RESTFUL_JSON']['cls'] = app.json_encoder = CADISEncoder

app = Flask(__name__)
app.config.from_object(FlaskConfig)
# app.json_encoder = CADISEncoder()
FlaskConfig.init_app(app)
api = Api(app)

def handle_exceptions(f):
    @wraps(f)
    def wrapped(*args, **kwds):
        try:
            ret = f(*args, **kwds)
        except Exception, e:
            logger.exception("Exception handling function %s:", f.func_name)
            raise
        return ret
    return wrapped


def create_obj(typeObj, data):
    # obj = typeObj.__new__()
    obj = CADIS()
    obj.__class__ = typeObj
    for dim in obj._dimensions:
        prop = getattr(obj, dim._name)
        if hasattr(prop, "__decode__"):
            prop = prop.__decode__(data[dim._name])
        else:
            prop = data[dim._name]
        setattr(obj, dim._name, prop)
    obj.ID = UUID(data["ID"])
    return obj

class GetUpdated(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        (new, updated, deleted) = FrameServer.Store.getupdated(typeObj, sim)
        ret = {}
        ret["new"] = new
        ret["updated"] = updated
        ret["deleted"] = deleted
        return ret

class GetPushType(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        objs = FrameServer.Store.get(typeObj)
        return objs

    @handle_exceptions
    def put(self, sim, t):
        typeObj = FrameServer.name2class[t]
        list_objs = json.loads(request.form["insert_list"])
        for o in list_objs:
            obj = create_obj(typeObj, o)
            FrameServer.Store.insert(obj, sim)
        return {}

    @handle_exceptions
    def post(self, sim, t):
        typeObj = FrameServer.name2class[t]
        args = parser.parse_args()
        # update dict is a dictionary of dictionaries: { primary_key : { property_name : property_value } }
        update_dict = json.loads(args["update_dict"])
        fixed_dict = {UUID(k): v for k, v in update_dict.items()}
        FrameServer.Store.update(typeObj, fixed_dict, sim)
        return {}

class GetInsertDeleteObject(Resource):
    @handle_exceptions
    def get(self, sim, t, uid):
        typeObj = FrameServer.name2class[t]
        obj = FrameServer.Store.get(typeObj, UUID(uid))
        return obj

    @handle_exceptions
    def put(self, sim, t, uid):
        # typeObj = FrameServer.name2class[t]
        typeObj = FrameServer.name2class[t]
        o = json.loads(request.form["obj"])
        obj = create_obj(typeObj, o)
        # TODO: Rebuild obj from json
        FrameServer.Store.insert(obj, sim)

    @handle_exceptions
    def delete(self, sim, t, uid):
        typeObj = FrameServer.name2class[t]
        FrameServer.Store.delete(typeObj, UUID(uid), sim)

class Register(Resource):
    @handle_exceptions
    def put(self, sim):
        FrameServer.Store.register(sim)

def SetupLoggers() :
    global logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logfile = filename = os.path.join(os.path.dirname(__file__), "../logs/frameserver.log")
    flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    flog.setLevel(logging.WARN)
    flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    # clog.addFilter(logging.Filter(name='mobdat'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)

class FrameServer(object):
    '''
    Store server for CADIS
    '''
    Store = SimpleStore()
    name2class = Store.name2class
    def __init__(self):
        # ## Test Code
        #         self.Store.register("TestSim")
        #         simnode = SimulationNode()
        #         simnode.ID = uuid4()
        #         simnode.Center = Vector3(1,2,3)
        #         self.Store.insert(simnode, "TestSim")
        # ##
        SetupLoggers()
        self.app = app
        self.api = api
        FrameServer.api = self.api
        self.api.add_resource(GetInsertDeleteObject, '/<string:sim>/<string:t>/<string:uid>')
        self.api.add_resource(GetPushType, '/<string:sim>/<string:t>')
        self.api.add_resource(GetUpdated, '/<string:sim>/updated/<string:t>')
        self.api.add_resource(Register, '/<string:sim>')
        self.app.run(port=12000, debug=False)


if __name__ == "__main__":
    FrameServer()
