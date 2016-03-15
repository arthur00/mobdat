#!/usr/bin/python
'''
Created on Feb 19, 2016

@author: arthurvaladares
'''
from django.utils.unittest.compatibility import wraps
from flask import Flask, request
from flask.helpers import make_response
from flask_restful import Api, Resource, reqparse
import json
import logging, logging.handlers
import new
import os
import os
import sys
from uuid import UUID
from uuid import uuid4
import platform
import time
import csv
from threading import Thread
import cProfile
from flask import request
import signal

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))

from mobdat.simulator.DataModel import Vehicle
from cadis.common import util
from cadis.common.util import Instrument
from cadis.language.schema import CADISEncoder, CADIS
from cadis.store.simplestore import SimpleStore, InstrumentedSimpleStore
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

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    server.shutdown()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

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
        (new, updated, deleted) = FrameServer.Store.getupdated(typeObj, sim, copy_objs=False)
        ret = {}
        ret["new"] = new
        ret["updated"] = updated
        ret["deleted"] = deleted
        return ret

class GetPushType(Resource):
    @handle_exceptions
    def get(self, sim, t):
        typeObj = FrameServer.name2class[t]
        objs = FrameServer.Store.get(typeObj, False)
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
    Store = InstrumentedSimpleStore()
    name2class = Store.name2class
    Shutdown = False
    def __init__(self, inst=False, profiling=False):
        global server
        # ## Test Code
        #         self.Store.register("TestSim")
        #         simnode = SimulationNode()
        #         simnode.ID = uuid4()
        #         simnode.Center = Vector3(1,2,3)
        #         self.Store.insert(simnode, "TestSim")
        # ##
        SetupLoggers()
        self.profiling = profiling
        if inst:
            headers = ['vehicles']
            headers.extend(FrameServer.Store.instruments.keys())
            self.benchmark = Instrument('frameserver', headers)
            Thread(target=WriteInstruments, args=(self.benchmark,)).start()

        if profiling:
            if not os.path.exists('stats'):
                os.mkdir('stats')
            self.profile = cProfile.Profile()
            self.profile.enable()
            print "starting profiler"
        self.app = app
        self.api = api
        FrameServer.app = app
        FrameServer.api = self.api
        self.api.add_resource(GetInsertDeleteObject, '/<string:sim>/<string:t>/<string:uid>')
        self.api.add_resource(GetPushType, '/<string:sim>/<string:t>')
        self.api.add_resource(GetUpdated, '/<string:sim>/updated/<string:t>')
        self.api.add_resource(Register, '/<string:sim>')
        server = self
        self.app.run(port=12000, debug=False)

    def shutdown(self):
        if self.profiling:
            strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.profile.disable()
            self.profile.create_stats()
            self.profile.dump_stats(os.path.join('stats', "%s_frameserver.ps" % (strtime)))
        FrameServer.Shutdown = True

def WriteInstruments(benchmark):
    while (not FrameServer.Shutdown):
        time.sleep(1)
        instruments = FrameServer.Store.collect_instruments()
        instruments['vehicles'] = FrameServer.Store.count(Vehicle)
        benchmark.add_instruments(instruments)
        benchmark.dump_stats()

if __name__ == "__main__":
    FrameServer(True, True)
