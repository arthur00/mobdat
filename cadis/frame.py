'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import StringIO
import cProfile
from copy import deepcopy, copy
import csv
from functools import wraps
import logging, sys
from multiprocessing import Process
import os
import platform
import pstats
import random
import signal
from threading import Timer
import time
import uuid

from cadis.common.IFramed import IFramed
from cadis.language.schema import schema_data, CADISEncoder, subsetsof, \
    setsof, sets as schema_sets, subsets as schema_subsets, permutationsets as schema_permutationsets, \
    CADIS
from cadis.store.remotestore import RemoteStore
from cadis.store.simplestore import SimpleStore


# import threading
logger = logging.getLogger(__name__)
LOG_HEADER = "[FRAME]"

USE_REMOTE_STORE = True # TODO Convert C# server to accept Strings instead of Integer
DEBUG = True
INSTRUMENT = True
INSTRUMENT_HEADERS = {}

# SimulatorStartup = False
# SimulatorShutdown = False
# SimulatorPaused = False


class TimerThread(Process) :
    # -----------------------------------------------------------------
    def __init__(self, frame, store, cmds) :
        """
        This thread will drive the simulation steps by sending periodic clock
        ticks that each of the connectors can process.

        Arguments:
        evrouter -- the initialized event handler object
        interval -- time between successive clock ticks
        """

        # threading.Thread.__init__(self)
        super(TimerThread, self).__init__()

        self.__Logger = logging.getLogger(__name__)
        self.frame = frame
        Frame.Store = store
        self.cmds = cmds
        self.CurrentIteration = 0
        # self.IntervalTime = float(settings["General"]["Interval"])
        if self.frame.interval:
            self.IntervalTime = self.frame.interval
        else:
            self.IntervalTime = 0.2

        self.Clock = time.time

        # # this is an ugly hack because the cygwin and linux
        # # versions of time.clock seem seriously broken
        if platform.system() == 'Windows' :
            self.Clock = time.clock

    # -----------------------------------------------------------------
    def run(self) :
        global profile
        # Wait for the signal to start the simulation, this allows all of the
        # connectors to initialize
        self.frame.app.initialize()
        while not self.cmds["SimulatorStartup"]:
            time.sleep(5.0)

        if DEBUG:
            if not os.path.exists('stats'):
                os.mkdir('stats')
            self.profile = cProfile.Profile()
            self.profile.enable()
            self.__Logger.debug("starting profiler for %s", self.frame.app._appname)

        if INSTRUMENT:
            if not os.path.exists('stats'):
                os.mkdir('stats')
            strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.ifname = os.path.join('stats', "%s_bench_%s.csv" % (strtime, self.frame.app._appname))

            if platform.system() != "Windows":
                linkname = os.path.join('stats', "latest_%s" % self.frame.app._appname)
                if os.path.exists(linkname):
                    os.remove(linkname)
                os.symlink(os.path.abspath(self.ifname), linkname) # @UndefinedVariable only in Linux!
            with open(self.ifname, 'w', 0) as csvfile:
                # Base headers
                headers = ['delta', 'nobjects', 'mem buffer']
                # Annotated headers
                headers.extend(INSTRUMENT_HEADERS[self.frame.__module__])
                headers.extend(INSTRUMENT_HEADERS[self.frame.app.__module__])

                self.fieldnames = headers
                writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
                writer.writeheader()


        # Start the main simulation loop
        self.__Logger.debug("start main simulation loop")
        starttime = self.Clock()

        self.CurrentIteration = 0
        schema_data.frame = self.frame

        try:
            while not self.cmds["SimulatorShutdown"]:
                if self.cmds["SimulatorPaused"]:
                    time.sleep(self.IntervalTime)
                    continue

                stime = self.Clock()
                self.frame.execute_Frame()

                etime = self.Clock()
                delta = etime - stime

                if INSTRUMENT:
                    with open(self.ifname, 'a', 0) as csvfile:
                    # csv.writer(["%.3f" % delta].append(self.inst_array))
                        writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
                        d = self.frame._instruments
                        d['delta'] = delta * 1000
                        if self.CurrentIteration % 10 == 0:
                            d['nobjects'], d['mem buffer'] = self.frame.buffersize()
                        writer.writerow(d)
                        self.frame._instruments = {}
                if delta < self.IntervalTime :
                    time.sleep(self.IntervalTime - delta)
                else:
                    self.__Logger.warn("[%s]: Exceeded interval time by %s" , self.frame.app.__module__, delta)

                self.CurrentIteration += 1
        finally:
            if DEBUG:
                self.profile.disable()
                self.profile.create_stats()
                self.profile.dump_stats(os.path.join('stats', "%s_stats.ps" % self.frame.app._appname))

        # compute a few stats
        elapsed = self.Clock() - starttime
        avginterval = 1000.0 * elapsed / self.CurrentIteration
        self.__Logger.warn("%d iterations completed with an elapsed time %f or %f ms per iteration", self.CurrentIteration, elapsed, avginterval)

        self.frame.stop()
        # self.cmds["SimulatorShutdown"] = True

def instrument(f):
    if not INSTRUMENT:
        return f
    else:
        # if f.func_name not in INSTRUMENT_HEADERS:
        #    INSTRUMENT_HEADERS.append(f.func_name)
        if not f.__module__ in INSTRUMENT_HEADERS:
            INSTRUMENT_HEADERS[f.__module__] = []
        INSTRUMENT_HEADERS[f.__module__].append(f.func_name)
        @wraps(f)
        def instrument(*args, **kwds):
            obj = args[0]
            if isinstance(obj, IFramed):
                obj = obj.frame
            start = time.time()
            ret = f(*args, **kwds)
            end = time.time()
            if not hasattr(obj, '_instruments'):
                obj._instruments = {}
            obj._instruments[f.__name__] = (end - start) * 1000
            return ret
        return instrument

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

        # Keeps track of object ids received for subset queries that are not in buffer yet
        self.orphan_objids = {}

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
        #self.app.initialize()

    def go(self, cmd_dict):
        # self.timer = Timer(1.0, self.execute_Frame)
        # self.timer.start()
        self.cmds = cmd_dict
        self.thread = TimerThread(self, Frame.Store, cmd_dict)
        self.thread.start()
        # thread.join()
        # self.stop()
    def join(self):
        self.thread.join()

    def buffersize(self):
        size = 0
        nobjects = 0
        for t in self.storebuffer:
            for o in self.storebuffer[t].values():
                nobjects += 1
                for prop in o._dimensions:
                    size += sys.getsizeof(prop)
        return (nobjects, size)

    def stop(self):
        if self.timer:
            self.timer.cancel()
        self.app.shutdown()

    def disable_subset(self, t):
        self.subset_disable.add(t)

    def enable_subset(self, t):
        if t in self.subset_disable:
            self.subset_disable.remove(t)

    def process_declarations(self, app):
        self.produced = app._producer
        self.updated = app._gettersetter

        self.observed = set()
        self.iterate_types = []
        if app._getter:
            self.observed = self.observed.union(app._getter)
        if app._gettersetter:
            self.observed = self.observed.union(app._gettersetter)

        for t in self.observed.union(self.produced).union(self.updated):
            # self.changedproperties[t] = {}
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

        # When iterating for pull, we should request, in order:
        # (1) set items
        # (2) permutations
        # (3) subsets

        setitems = self.observed.intersection(schema_sets)
        permutations = self.observed.intersection(schema_permutationsets)
        subsets = self.observed.intersection(schema_subsets)
        for t in subsets:
            self.orphan_objids[t] = set()

        self.iterate_types.extend(setitems)
        self.iterate_types.extend(permutations)
        self.iterate_types.extend(subsets)

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
        setattr(self.app, "_appname", self.app.__class__.__name__)
        Frame.Store.register(self.app._appname)

    def execute_Frame(self):
        try:
            self.pull()
            self.track_changes = True
            self.app.update()
            self.track_changes = False
            self.push()
            if self.timer:
                self.timer = Timer(1.0, self.execute_Frame)
                self.timer.start()
            self.step += 1
            self.curtime = time.time()
        except:
            self.cmds["SimulatorPaused"] = True
            logger.exception("[%s] uncaught exception: ", self.app.__module__)

    def _fkobj(self, o):
        for propname in self.fkdict[o.__class__].keys():
            propvalue = getattr(o, propname)
            self.fkdict[o.__class__][propname][propvalue] = o.ID

    def _delfkobj(self, o):
        for propname in self.fkdict[o.__class__].keys():
            propvalue = getattr(o, propname)
            if propvalue in self.fkdict[o.__class__][propname]:
                del self.fkdict[o.__class__][propname][propvalue]

    @instrument
    def pull(self):
        tmpbuffer = {}
        for t in self.iterate_types:
            if t in self.subset_disable:
                continue

            self.new_storebuffer[t] = {}
            self.mod_storebuffer[t] = {}
            self.del_storebuffer[t] = {}

            if hasattr(Frame.Store, "getupdated"):
                (new, mod, deleted) = Frame.Store.getupdated(t, self.app._appname)
                if t in schema_subsets:
                    # Parent type of subset 't'
                    pt = setsof[t]

                    # Iterate for orphan keys from last pull
                    notfound = set()
                    for key in self.orphan_objids[t]:
                        if key in self.storebuffer[pt]:
                            o = copy(self.storebuffer[pt][key])
                            o.__class__ = t
                            self.storebuffer[t][key] = o
                            self.new_storebuffer[t][key] = o
                        else:
                            notfound.add(key)
                            self.__Logger.error("missing key %s for new object in subset list", key)
                    self.orphan_objids[t] = set().union(notfound)

                    for key in new:
                        # If the key is not in the store's buffer, its likely the object was created between
                        # the get for parent type and the current subset query. This is expected, just keep a reference
                        # to check for again on next pull.
                        if key in self.storebuffer[pt]:
                            o = copy(self.storebuffer[pt][key])
                            o.__class__ = t
                            self.storebuffer[t][key] = o
                            self.new_storebuffer[t][key] = o
                        else:
                            self.orphan_objids[t].add(key)
                    for key in mod:
                        o = copy(self.storebuffer[pt][key])
                        o.__class__ = t
                        self.storebuffer[t][key] = o
                        self.mod_storebuffer[t][key] = o
                    for key in deleted:
                        if key in self.storebuffer[pt]:
                            o = copy(self.storebuffer[pt][key])
                        elif key in self.del_storebuffer[pt]:
                            o = copy(self.del_storebuffer[pt][key])
                        else:
                            self.__Logger.warn("object %s was deleted, could find find a reference to give to application.", key)
                            o = CADIS()
                            o.ID = key
                        o.__class__ = t
                        del self.storebuffer[t][key]
                        self.del_storebuffer[t][key] = o
                else:
                    for o in new:
                        self.storebuffer[t][o._primarykey] = o
                        self.new_storebuffer[t][o._primarykey] = o
                        if o.__class__ in self.fkdict:
                            self._fkobj(o)
                    for o in mod:
                        self.storebuffer[t][o._primarykey] = o
                        self.mod_storebuffer[t][o._primarykey] = o
                        if o.__class__ in self.fkdict:
                            self._fkobj(o)
                    for key in deleted:
                        if key in self.storebuffer[t]:
                            o = self.storebuffer[t][key]
                            self.del_storebuffer[t][key] = o
                            del self.storebuffer[t][key]
                        if o.__class__ in self.fkdict:
                            self._delfkobj(o)

            else:
                tmpbuffer[t] = {}
                for o in Frame.Store.get(t):
                    tmpbuffer[t][o._primarykey] = o

                # TODO: Remove when Store does this
                # Added objects since last pull
                for o in tmpbuffer[t].values():
                    if o._primarykey not in self.storebuffer[t]:
                        # logger.debug("%s Found new object: %s", LOG_HEADER, o)
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

    @instrument
    def push(self):
        for t in self.newlyproduced:
            if len(self.newlyproduced[t]) > 0:
                if hasattr(Frame.Store, "insert_all"):
                    Frame.Store.insert_all(t, self.newlyproduced[t].values(), self.app._appname)
                else:
                    for o in self.newlyproduced[t].values():
                        Frame.Store.insert(o, self.app._appname)
            self.newlyproduced[t] = {}

        for t in self.deletelist:
            for o in self.deletelist[t].values():
                Frame.Store.delete(t, o._primarykey, self.app._appname)
                if t in self.pushlist and o._primarykey in self.pushlist[t]:
                    del self.pushlist[t][o._primarykey]

        Frame.Store.update_all(self.pushlist, self.app._appname)
        # self.pushlist[t] = {}


    def set_property(self, t, o, v, n):
        # Newly produced items will be pushed entirely. Skip...
        if not o._primarykey:
            return

        if t in self.newlyproduced and o._primarykey in self.newlyproduced[t]:
            # logger.debug("[%s] Object ID %s in newly produced")
            return

        # Object not tracked by store yet. Ignore...
        if o._primarykey not in self.storebuffer[t]:
            # logger.debug("[%s] Object ID %s not being tracked yet")
            return

        if o._primarykey not in self.pushlist[t]:
            self.pushlist[t][o._primarykey] = {}

        # logger.debug("[%s] Object ID %s property %s being set to %s")
        # Save the property update
        self.pushlist[t][o._primarykey][n] = v
        # logger.debug("property %s of object %s (ID %s) set to %s", n, o, o._primarykey, v)

    def add(self, obj):
        t = obj.__class__
        if t in self.storebuffer:
            # logger.debug("%s Creating new object %s.%s", LOG_HEADER, obj.__class__, obj._primarykey)
            if obj._primarykey == None:
                obj._primarykey = uuid.uuid4()
            obj._frame = self
            self.newlyproduced[t][obj._primarykey] = obj
            self.storebuffer[t][obj._primarykey] = obj
            # If we removed then readded in the same tick, make sure we don't send the remove anymore
            if t in self.deletelist and obj._primarykey in self.deletelist[t]:
                del self.deletelist[t][obj._primarykey]
        else:
            logger.error("%s Object not in dictionary: %s", LOG_HEADER, obj.__class__)

    def delete(self, t, oid):
        if oid in self.storebuffer[t]:
            o = self.storebuffer[t][oid]
            self.deletelist[t][o._primarykey] = o
            del self.storebuffer[t][oid]
            # If we added and removed in the same tick, make sure we don't send the add
            if t in self.newlyproduced and o._primarykey in self.newlyproduced[t]:
                del self.newlyproduced[t][o._primarykey]

        else:
            logger.warn("could not find object %s in storebuffer")
        return True

    def findproperty(self, t, propname, value):
        for o in self.storebuffer[t].values():
            if getattr(o, propname) == value:
                return o
        return None

    def _resolve_fk(self, obj, t):
        for propname, cls in t._foreignkeys.items():
            propvalue = getattr(obj, propname)
            fname = getattr(t, propname)._foreignprop._name
            if propvalue in self.fkdict[cls][fname]:
                primkey = self.fkdict[cls][fname][propvalue]
            else:
                # logger.error("Could not find property value in foreign key dictionary.")
                return None
            if primkey in self.storebuffer[cls]:
                newobj = self.storebuffer[cls][primkey]
                if hasattr(cls, '_foreignkeys'):
                    self._resolve_fk(newobj, cls)
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
                obj = self.storebuffer[t][primkey]
                if hasattr(t, '_foreignkeys'):
                    self._resolve_fk(obj, t)
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
        return self.new_storebuffer[t].values()

    def deleted(self, t):
        return self.del_storebuffer[t].values()

    def changed(self, t):
        return self.mod_storebuffer[t].values()

    def __deepcopy__(self, memo):
        return self
