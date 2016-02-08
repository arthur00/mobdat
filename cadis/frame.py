'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

from threading import Timer
from copy import deepcopy, copy
from cadis.store.simplestore import SimpleStore
import logging, sys
from cadis.store.remotestore import RemoteStore
from cadis.language.schema import schema_data, CADISEncoder
import threading
import time
import platform
import cProfile
import signal

import uuid
import random
import StringIO
import pstats

logger = logging.getLogger(__name__)
LOG_HEADER = "[FRAME]"

USE_REMOTE_STORE = True # TODO Convert C# server to accept Strings instead of Integer
DEBUG = False

SimulatorStartup = False
SimulatorShutdown = False
CurrentIteration = 0
FinalIteration = 0

class TimerThread(threading.Thread) :
    # -----------------------------------------------------------------
    def __init__(self, frame) :
        """
        This thread will drive the simulation steps by sending periodic clock
        ticks that each of the connectors can process.

        Arguments:
        evrouter -- the initialized event handler object
        interval -- time between successive clock ticks
        """

        threading.Thread.__init__(self)

        self.__Logger = logging.getLogger(__name__)
        self.frame = frame

        #self.IntervalTime = float(settings["General"]["Interval"])
        if self.frame.interval:
            self.IntervalTime = self.frame.interval
        else:
            self.IntervalTime = 0.2

        global FinalIteration
        #FinalIteration = settings["General"].get("TimeSteps",0)
        FinalIteration = 0

        self.Clock = time.time

        ## this is an ugly hack because the cygwin and linux
        ## versions of time.clock seem seriously broken
        if platform.system() == 'Windows' :
            self.Clock = time.clock

    # -----------------------------------------------------------------
    def run(self) :
        global SimulatorStartup, SimulatorShutdown
        global FinalIteration, CurrentIteration
        global profile
        # Wait for the signal to start the simulation, this allows all of the
        # connectors to initialize
        while not SimulatorStartup :
            time.sleep(5.0)

        if DEBUG:
            self.profile = cProfile.Profile()
            self.profile.enable()
            self.__Logger.debug("starting profiler for %s", self.frame.app.__module__)

        # Start the main simulation loop
        self.__Logger.debug("start main simulation loop")
        starttime = self.Clock()

        CurrentIteration = 0
        schema_data.frame = self.frame
        try:
            while not SimulatorShutdown :

                    if FinalIteration > 0 and CurrentIteration >= FinalIteration :
                        break

                    stime = self.Clock()

                    self.frame.execute_Frame()

                    etime = self.Clock()

                    if (etime - stime) < self.IntervalTime :
                        time.sleep(self.IntervalTime - (etime - stime))
                    else:
                        self.__Logger.warn("[%s]: Exceeded interval time by %s" , self.frame.app.__module__, (etime - stime))

                    CurrentIteration += 1
        finally:
            if DEBUG:
                self.profile.disable()
                self.profile.create_stats()
                self.profile.dump_stats("stats_%s.ps" % self.frame.app.__module__)

        # compute a few stats
        elapsed = self.Clock() - starttime
        avginterval = 1000.0 * elapsed / CurrentIteration
        self.__Logger.warn("%d iterations completed with an elapsed time %f or %f ms per iteration", CurrentIteration, elapsed, avginterval)

        self.frame.stop()
        SimulatorShutdown = True

class Frame(object):
    '''
    classdocs
    '''

    Store = None
    def __init__(self, store=None):
        '''
        Constructor
        '''
        self.app = None
        self.timer = None
        self.interval = None
        self.track_changes = False
        self.step = 0
        self.curtime = time.time()
        self.__Logger = logger

        # Local storage for thread
        self.tlocal = None

        # Stores objects from store
        self.storebuffer = {}

        # Stores new objects since last pull
        self.new_storebuffer = {}
        # Stores modified objects since last pull
        self.mod_storebuffer = {}
        # Store removed objects since last pull
        self.del_storebuffer = {}

        # Types that are being observed (read) by this application
        self.observed = set()
        # Stores the types that can be updated (read/write) by app
        self.updated = set()
        # Stores the types that can be produced by app
        self.produced = set()
        # Stores the types that can be retrieved by app
        self.newlyproduced = {}

        # Properties that changed during this iteration
        self.changedproperties = {}

        # List of objects to be pushed to the store (no subsets)
        self.pushlist = {}

        # List of objects marked for deletion
        self.deletelist = {}

        # Holds a k,v storage of foreign keys (e.g. name -> id)
        self.fkdict = {}

        # Disables fetching of subsets
        self.subset_disable = set()

        self.timer = None
        self.step = 0
        self.thread = None
        self.encoder = CADISEncoder()
        if store:
            logger.debug("%s received store %s", LOG_HEADER, store)
            Frame.Store = store

    def attach(self, app):
        self.app = app
        self.process_declarations(app)
        self.app.initialize()

    def go(self):
        #self.timer = Timer(1.0, self.execute_Frame)
        #self.timer.start()
        self.thread = TimerThread(self)
        self.thread.start()
        #thread.join()
        #self.stop()
    def join(self):
        self.thread.join()

    def stop(self):
        if self.timer:
            self.timer.cancel()
        sys.exit(0)

    def disable_subset(self, t):
        self.subset_disable.add(t)

    def enable_subset(self, t):
        if t in self.subset_disable:
            self.subset_disable.remove(t)

    def process_declarations(self, app):
        self.produced = app._producer
        self.updated = app._gettersetter
        self.observed = set()
        if app._getter:
            self.observed = self.observed.union(app._getter)
        if app._gettersetter:
            self.observed = self.observed.union(app._gettersetter)

        for t in self.observed.union(self.produced).union(self.updated):
            #self.changedproperties[t] = {}
            self.storebuffer[t] = {}
            self.new_storebuffer[t] = {}
            self.mod_storebuffer[t] = {}
            self.del_storebuffer[t] = {}
            if hasattr(t, "_foreignkeys"):
                for propname, cls in t._foreignkeys.items():
                    fname = getattr(t, propname)._foreignprop._name
                    if not cls in self.fkdict:
                        self.fkdict[cls] = {}
                    if not fname in self.fkdict[cls]:
                        self.fkdict[cls][fname] = {}
            logger.debug("%s store buffer for type %s", LOG_HEADER, t)

        for t in self.produced:
            self.pushlist[t] = {}
            self.deletelist[t] = {}
            self.newlyproduced[t] = {}

        if Frame.Store == None:
            logger.debug("%s creating new store", LOG_HEADER)
            if USE_REMOTE_STORE:
                Frame.Store = RemoteStore()
            else:
                Frame.Store = SimpleStore()

        Frame.Store.register(self)

    def execute_Frame(self):
        try:
            self.pull()
            self.track_changes = True
            self.app.update()
            self.track_changes = False
            self.prepare_push()
            self.push()
            if self.timer:
                self.timer = Timer(1.0, self.execute_Frame)
                self.timer.start()
            self.step += 1
            self.curtime = time.time()
        except:
            logger.exception("uncaught exception: ")

    def pull(self):
        tmpbuffer = {}
        for t in self.observed.symmetric_difference(self.subset_disable):
            self.new_storebuffer[t] = {}
            self.mod_storebuffer[t] = {}
            self.del_storebuffer[t] = {}

            # if type is subset, it will always recalculate and return as new / updated
            # TODO: Allow caching of subsets to determine new / updated / deleted
            if hasattr(Frame.Store, "updated"):
                (new, mod, deleted) = Frame.Store.updated(t, self)
                for o in new:
                    self.storebuffer[t][o._primarykey] = o
                    self.new_storebuffer[t][o._primarykey] = o
                    if o.__class__ in self.fkdict:
                        for propname in self.fkdict[o.__class__].keys():
                            propvalue = getattr(o, propname)
                            self.fkdict[o.__class__][propname][propvalue] = o.ID
                for o in mod:
                    self.storebuffer[t][o._primarykey] = o
                    self.mod_storebuffer[t][o._primarykey] = o
                    if o.__class__ in self.fkdict:
                        for propname in self.fkdict[o.__class__].keys():
                            propvalue = getattr(o, propname)
                            self.fkdict[o.__class__][propname][propvalue] = o.ID
                for o in deleted:
                    if o._primarykey in self.storebuffer[t]:
                        del self.storebuffer[t][o._primarykey]
                        self.del_storebuffer[t][o._primarykey] = o
                    if o.__class__ in self.fkdict:
                        for propname in self.fkdict[o.__class__].keys():
                            propvalue = getattr(o, propname)
                            if propvalue in self.fkdict[o.__class__][propname]:
                                del self.fkdict[o.__class__][propname][propvalue]
            else:
                tmpbuffer[t] = {}
                for o in Frame.Store.get(t):
                    tmpbuffer[t][o._primarykey] = o

                #TODO: Remove when Store does this
                # Added objects since last pull
                for o in tmpbuffer[t].values():
                    if o._primarykey not in self.storebuffer[t]:
                        #logger.debug("%s Found new object: %s", LOG_HEADER, o)
                        self.new_storebuffer[t][o._primarykey] = o
                    else:
                        # check if updated
                        orig = self.encoder.encode(self.storebuffer[t][o._primarykey])
                        new = self.encoder.encode(o)
                        if orig != new:
                            self.mod_storebuffer[t][o._primarykey] = o

                # Deleted objects since last pull
                for o in self.storebuffer[t].values():
                    if o._primarykey not in tmpbuffer[t]:
                        self.del_storebuffer[t][o._primarykey] = o

                self.storebuffer[t] = tmpbuffer[t]

    def prepare_push(self):
        #for t in self.changedproperties:
        #    push_key = None
        #    push_obj = None
        #    if t in subsets:
        #        push_key = setsof[t]
        #
        #    elif t in sets:
        #        push_key = t
        #        for o in self.changedproperties[t]:
        #            self.pushlist[t][o._primarykey] = o
        pass

    def push(self):
        for t in self.pushlist:
            for primkey in self.pushlist[t].keys():
                if primkey in self.newlyproduced[t]:
                    continue
                elif primkey in self.storebuffer[t]:
                    Frame.Store.update(t, primkey, self, self.pushlist[t][primkey])
                else:
                    logger.error("[%s] Missing object in store buffer. Object ID %s,  Pushlist: %s", self.app, primkey, self.pushlist[t])
            self.pushlist[t] = {}

        for t in self.produced:
            if t in self.newlyproduced:
                for o in self.newlyproduced[t].values():
                    #logger.debug("[%s] Adding obj %s (ID %s) to store", self.app, o, o._primarykey)
                    Frame.Store.insert(o, self)
                self.newlyproduced[t] = {}

        for t in self.deletelist:
            for o in self.deletelist[t].values():
                Frame.Store.delete(t, o)

    def set_property(self, t, o, v, n):
        # Newly produced items will be pushed entirely. Skip...
        if not o._primarykey:
            return

        if t in self.newlyproduced and o._primarykey in self.newlyproduced[t]:
            #logger.debug("[%s] Object ID %s in newly produced")
            return

        # Object not tracked by store yet. Ignore...
        if o._primarykey not in self.storebuffer[t]:
            #logger.debug("[%s] Object ID %s not being tracked yet")
            return

        if o._primarykey not in self.pushlist[t]:
            self.pushlist[t][o._primarykey] = {}

        #logger.debug("[%s] Object ID %s property %s being set to %s")
        # Save the property update
        self.pushlist[t][o._primarykey][n] = v
        #logger.debug("property %s of object %s (ID %s) set to %s", n, o, o._primarykey, v)

    def add(self, obj):
        if obj.__class__ in self.storebuffer:
            #logger.debug("%s Creating new object %s.%s", LOG_HEADER, obj.__class__, obj._primarykey)
            if obj._primarykey == None:
                obj._primarykey =  uuid.uuid4()
            obj._frame = self
            self.newlyproduced[obj.__class__][obj._primarykey] = obj
            self.storebuffer[obj.__class__][obj._primarykey] = obj
        else:
            logger.error("%s Object not in dictionary: %s", LOG_HEADER, obj.__class__)

    def delete(self, t, oid):
        if oid in self.storebuffer[t]:
            o = self.storebuffer[t][oid]
            self.deletelist[t][o._primarykey] = o
            del self.storebuffer[t][oid]
        return True

    def findproperty(self, t, propname, value):
        for o in self.storebuffer[t].values():
            if getattr(o, propname) == value:
                return o
        return None

    def resolve_fk(self, obj, t):
        for propname, cls in t._foreignkeys.items():
            propvalue = getattr(obj, propname)
            fname = getattr(t, propname)._foreignprop._name
            if propvalue in self.fkdict[cls][fname]:
                primkey = self.fkdict[cls][fname][propvalue]
            else:
                #logger.error("Could not find property value in foreign key dictionary.")
                return None
            if primkey in self.storebuffer[cls]:
                newobj = self.storebuffer[cls][primkey]
                if hasattr(cls, '_foreignkeys'):
                    self.resolve_fk(newobj, cls)
                setattr(obj, propname, newobj)
            # Look up by name, just in case
            else:
                fname = getattr(obj.__class__, propname)._foreignprop._name
                for o in self.storebuffer[cls].values():
                    if propvalue == getattr(o, fname):
                        setattr(obj, propname, o)


    def get(self, t, primkey=None):
        self.track_changes = False
        try:
            if t not in self.storebuffer:
                self.__Logger.error("Could not find type %s in cache. Did you remember to add it as a gettter, setter, or producer in the simulator?")
                sys.exit(0)
            if primkey != None:
                obj = copy(self.storebuffer[t][primkey])
                if hasattr(t, '_foreignkeys'):
                    self.resolve_fk(obj, t)
                return obj
            else:
                res = []
                for obj in self.storebuffer[t].values():
                    res.append(self.get(t, obj._primarykey))
                return res
        except:
            self.__Logger.exception("Uncaught exception in Frame.Get")
            raise
        finally:
            self.track_changes = True


    def new(self, t):
        return copy(self.new_storebuffer[t].values())

    def deleted(self, t):
        return copy(self.del_storebuffer[t].values())

    def changed(self, t):
        return copy(self.mod_storebuffer[t].values())

    def __deepcopy__(self, memo):
        return self
