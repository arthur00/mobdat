'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

from cadis.common.IStore import IStore;
from cadis.language import schema
from copy import copy, deepcopy
import httplib
import sys
import logging
import threading

class FrameUpdate(object):
    def __init__(self, t, store):
        self.added = set()
        self.updated = set()
        self.deleted = set()
        self.objtype = t
        self.store = store

    def clear(self):
        self.added.clear()
        self.updated.clear()
        self.deleted.clear()

    def add(self, o):
        self.added.add(o._primarykey)

    def update(self, o):
        self.updated.add(o._primarykey)

    def delete(self, o):
        if o._primarykey in self.added:
            self.added.remove(o._primarkey)
        if o._primarykey in self.updated:
            self.updated.remove(o._primarkey)
        self.deleted.add(o._primarykey)

    def updatelist(self, clear=False):
        added = []
        updated = []
        deleted = []
        for key in self.added:
            added.append(copy(self.store[self.objtype][key]))
        for key in self.updated:
            updated.append(copy(self.store[self.objtype][key]))
        for key in self.deleted:
            deleted.append(copy(self.store[self.objtype][key]))
        if clear:
            self.clear()
        return (added, updated, deleted)

class SimpleStore(IStore):
    '''
    classdocs
    '''
    app = None
    store = {}
    subsets = {}
    updates4sim = {}
    lock = threading.Lock()
    __Logger = logging.getLogger(__name__)

    def __init__(self):
        '''
        Constructor
        '''

        #self.store = {}
        #self.subsets = {}
        #self.updates = {}
        for t in schema.sets.union(schema.permutationsets):
            if t not in self.store:
                self.store[t] = {}
        for t in schema.subsets:
            if t not in self.subsets:
                self.subsets[t] = {}
        #for t in schema.permutedclss:
        #    self.store[t] = {}

    def register(self, sim):
        self.updates4sim[sim] = {}
        for t in schema.sets.union(schema.permutationsets):
            self.updates4sim[sim][t] = FrameUpdate(t, self.store)

    def insert(self, obj, sim):
        with self.lock:
            # Only accepts inserts of sets
            if obj.__class__ not in self.store:
                #self.store[obj.__class__] = {}
                self.__Logger.error("ERROR! Object type supposed to exist in store")
                sys.exit(0)
            new = False
            if obj._primarykey not in self.store[obj.__class__]:
                new = True

            self.store[obj.__class__][obj._primarykey] = copy(obj)
            for s in self.updates4sim.keys():
                if s != sim:
                    if new:
                        self.updates4sim[s][obj.__class__].add(obj)
                    else:
                        self.updates4sim[s][obj.__class__].update(obj)

    def get(self, typeObj):
        with self.lock:
            if typeObj in self.store:
                return copy(self.store[typeObj].values())
            elif typeObj in self.subsets:
                res = copy(typeObj.query())
                for o in res:
                    o.__class__ = typeObj
                return res
            else:
                self.__Logger.error("ERROR! Object type supposed to exist as a set or subset")
                sys.exit(0)

    def updated(self, typeObj, sim):
        with self.lock:
            return self.updates4sim[sim][typeObj].updatelist(True)

    def delete(self, typeObj, obj):
        with self.lock:
            if obj._primarykey in self.store[typeObj]:
                del self.store[typeObj][obj._primarykey]

    def close(self):
        return
