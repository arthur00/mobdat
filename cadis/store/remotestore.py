'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

from cadis.common.IStore import IStore;
from cadis.language import schema
from copy import deepcopy
import json, sys
import urllib2
from cadis.language.schema import CADISEncoder
from uuid import uuid4, UUID

class RemoteStore(IStore):
    '''
    classdocs
    '''
    app = None
    store = {}
    address = ""

    def __init__(self):
        '''
        Constructor
        '''
        self.store = {}
        self.address = "http://localhost:12000/"
        for t in schema.sets:
            self.store[t] = {}
        self.encoder = CADISEncoder()

    def insert(self, obj, sim = None):
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
