'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

from threading import Timer
from copy import deepcopy
from cadis.store.simplestore import SimpleStore
import logging, sys
from cadis.store.remotestore import RemoteStore
from cadis.language.schema import schema_data, CADISEncoder
import threading
import time
import platform
logger = logging.getLogger(__name__)
LOG_HEADER = "[FRAME]"

SimulatorStartup = True
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

        # Wait for the signal to start the simulation, this allows all of the
        # connectors to initialize
        while not SimulatorStartup :
            time.sleep(5.0)

        # Start the main simulation loop
        self.__Logger.debug("start main simulation loop")
        starttime = self.Clock()

        CurrentIteration = 0
        schema_data.frame = self.frame

        while not SimulatorShutdown :
            if FinalIteration > 0 and CurrentIteration >= FinalIteration :
                break

            stime = self.Clock()

            self.frame.execute_Frame()

            etime = self.Clock()

            if (etime - stime) < self.IntervalTime :
                time.sleep(self.IntervalTime - (etime - stime))

            CurrentIteration += 1

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
    def __init__(self, store=None):
        '''
        Constructor
        '''
        self.app = None
        self.Store = store
        self.timer = None
        self.interval = None
        self.track_changes = False
        self.step = 0
        self.curtime = time.time()

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

        self.timer = None
        self.step = 0
        self.thread = None
        self.encoder = CADISEncoder()
        if store:
            logger.debug("%s received store %s", LOG_HEADER, store)

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
            logger.debug("%s store buffer for type %s", LOG_HEADER, t)

        for t in self.produced:
            self.pushlist[t] = {}
            self.deletelist[t] = []
            self.newlyproduced[t] = []

        if Frame.Store == None:
            logger.debug("%s creating new store", LOG_HEADER)
            Frame.Store = SimpleStore()
        #Frame.Store = RemoteStore()

    def execute_Frame(self):
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

    def pull(self):
        tmpbuffer = {}
        for t in self.observed:
            self.new_storebuffer[t] = {}
            self.mod_storebuffer[t] = {}
            self.del_storebuffer[t] = {}

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

            self.storebuffer[t] = deepcopy(tmpbuffer[t])

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
                if primkey not in self.newlyproduced[t] and primkey in self.storebuffer[t]:
                    obj = self.storebuffer[t][primkey]
                    for propname in self.pushlist[t][primkey].keys():
                        #logger.debug("Setting attributes in pushlist for ID %s: %s = %s",primkey,propname,self.pushlist[t][primkey][propname])
                        setattr(obj, propname, self.pushlist[t][primkey][propname])
                    # TODO: This should be an update, not an insert
                    Frame.Store.insert(obj)
                else:
                    logger.error("[%s] Could not find superset object for object ID %s. Pushlist: %s", self.app, primkey, self.pushlist[t])
            self.pushlist[t] = {}

        for t in self.produced:
            if t in self.newlyproduced:
                for o in self.newlyproduced[t]:
                    #logger.debug("[%s] Adding obj %s (ID %s) to store", self.app, o, o._primarykey)
                    Frame.Store.insert(o)
                self.newlyproduced[t] = []

        for t in self.deletelist:
            for o in self.deletelist[t]:
                Frame.Store.delete(t, o)

    def set_property(self, t, o, v, n):
        # Newly produced items will be pushed entirely. Skip...
        if o._primarykey in self.newlyproduced[t]:
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
            obj._frame = self
            self.newlyproduced[obj.__class__].append(obj)
            self.storebuffer[obj.__class__][obj._primarykey] = obj
        else:
            logger.error("%s Object not in dictionary: %s", LOG_HEADER, obj.__class__)

    def delete(self, t, oid):
        if oid in self.storebuffer[t]:
            o = self.storebuffer[t][oid]
            self.deletelist[t].append(o)
            del self.storebuffer[t][oid]
        return True

    def get(self, t, primkey=None):
        if primkey != None:
            a = deepcopy(self.storebuffer[t][primkey])
        else:
            a = deepcopy(self.storebuffer[t].values())
        return a

    def new(self, t):
        return deepcopy(self.new_storebuffer[t].values())

    def deleted(self, t):
        return deepcopy(self.del_storebuffer[t].values())

    def changed(self, t):
        if len(self.mod_storebuffer[t].keys()):
            #logger.debug("objects with IDs %s have changed", self.mod_storebuffer[t].keys())
            return deepcopy(self.mod_storebuffer[t].values())
        else:
            return []

    def __deepcopy__(self, memo):
        return self


Frame.Store = None
