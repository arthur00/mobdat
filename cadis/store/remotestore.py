'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''
from copy import deepcopy
import json, sys
import requests
import time
import urllib2
from uuid import uuid4, UUID

from cadis.common.IStore import IStore
from cadis.language import schema
from cadis.language.schema import CADIS, CADISEncoder
import logging


# from test.test_smtplib import sim_auth
class RemoteStore(IStore):
    '''
    classdocs
    '''
    app = None
    address = ""
    __Logger = logging.getLogger(__name__)
    def __init__(self):
        '''
        Constructor
        '''
        self.address = "http://localhost:12000/"
        self.encoder = CADISEncoder()

    def insert(self, obj, sim=None):
        msg = self.encoder.encode(obj)
        req = urllib2.Request(self.address + obj._FULLNAME + '/')
        req.add_header('Content-Type', 'application/json')

        response = urllib2.urlopen(req, msg)
        return response

    def get(self, typeObj):
        jsonlist = json.load(urllib2.urlopen(self.address + typeObj._FULLNAME + "/"))
        objlist = []
        for data in jsonlist:
            obj = typeObj.__new__(typeObj)
            for dim in obj._dimensions:
                prop = getattr(obj, dim._name)
                if hasattr(prop, "__decode__"):
                    prop = prop.__decode__(data[dim._name])
                else:
                    prop = data[dim._name]
                setattr(obj, dim._name, prop)
            obj.ID = UUID(data["ID"])
            objlist.append(obj)
        return objlist

    def delete(self, typeObj, obj):
        pass

    def close(self):
        return

class PythonRemoteStore(IStore):
    __Logger = logging.getLogger(__name__)
    def __init__(self, address="http://localhost:12000"):
        '''
        address: includes port. e.g. http://localhost:9000
        sim: identifier name for the simulator using this store
        '''
        self.encoder = CADISEncoder()
        if not address.endswith('/'):
            address += '/'
        self.address = address

    def insert(self, obj, sim):
        jsonobj = self.encoder.encode(obj)
        response = requests.put("%s%s/%s" % (self.base_address, obj._FULLNAME, obj.ID), data={'obj' : jsonobj }, headers={'Content-Type':'application/json', 'InsertType':'Single'})
        return response

    def insert_all(self, t, list_obj, sim):
        objs = self.encoder.encode(list_obj)
        response = requests.put(self.base_address + t._FULLNAME, data={ 'insert_list' : objs }, headers={'InsertType':'Multiple'}, stream=True)
        return response

    def get(self, typeObj):
        resp = requests.get(self.base_address + typeObj._FULLNAME)
        jsonlist = json.loads(resp.text)
        objlist = []
        for data in jsonlist:
            # obj = typeObj.__new__(typeObj)
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
            objlist.append(obj)
        return objlist

    def register(self, sim):
        self.base_address = self.address + sim + '/'
        resp = requests.put(self.base_address[:-1])
        self.sim = sim
        return resp

    def update_all(self, pushlist, sim):
        for t in pushlist:
            if len(pushlist[t]) > 0:
                tmp = pushlist[t]
                updates = self.encoder.encode({str(k):v for k, v in tmp.items()})
                resp = requests.post(self.base_address + t._FULLNAME, data={'update_dict' : updates})
            pushlist[t] = {}

    def create_obj(self, typeObj, data):
        try:
            obj = typeObj.__new__(typeObj)
            for dim in obj._dimensions:
                prop = getattr(obj, dim._name)
                if hasattr(prop, "__decode__"):
                    prop = prop.__decode__(data[dim._name])
                else:
                    prop = data[dim._name]
                setattr(obj, dim._name, prop)
            obj.ID = UUID(data["ID"])
        except Exception, e:
            self.__Logger.exception("Failed to create object from data %s", data)
        return obj

    def getupdated(self, typeObj, sim):
        resp = requests.get(self.base_address + 'updated/' + typeObj._FULLNAME)
        jsonlist = json.loads(resp.text)
        (new, mod, deleted) = jsonlist['new'], jsonlist['updated'], jsonlist['deleted']
        if typeObj not in schema.subsets:
            newobjlist = []
            updatedobjlist = []
            deletedobjlist = []
            for data in new:
                obj = self.create_obj(typeObj, data)
                newobjlist.append(obj)
            for data in mod:
                obj = self.create_obj(typeObj, data)
                updatedobjlist.append(obj)
            for data in deleted:
                deletedobjlist = [UUID(v) for v in deleted]
            return (newobjlist, updatedobjlist, deletedobjlist)
        else:
            return ([UUID(v) for v in new], [UUID(v) for v in mod], [UUID(v) for v in deleted])

    def close(self):
        return True

    def delete(self, typeObj, primkey, sim):
        resp = requests.delete(self.base_address + typeObj._FULLNAME + '/%s' % primkey)
        return resp
