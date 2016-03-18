#!/usr/bin/env python
"""
Copyright (c) 2014, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer. 

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution. 

* Neither the name of Intel Corporation nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission. 

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. 

@file    BaseConnector.py
@author  Mic Bowman
@date    2013-12-03

BaseConnector is the base class for the connectors. It implements
world time and other functions common to all connectors.

"""

import os, sys
import logging
import csv
from functools import wraps
from mobdat.simulator import EventTypes
from mobdat.simulator.Controller import INSTRUMENT, INSTRUMENT_HEADERS
import datetime

sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import platform, time

def instrument(f):
    if not INSTRUMENT:
        return f
    else:
        #if f.func_name not in INSTRUMENT_HEADERS:
        #    INSTRUMENT_HEADERS.append(f.func_name)
        if not f.__module__ in INSTRUMENT_HEADERS:
            INSTRUMENT_HEADERS[f.__module__] = []
        INSTRUMENT_HEADERS[f.__module__].append(f.func_name)
        @wraps(f)
        def instrument(*args, **kwds):
            obj = args[0]
            start = time.time()
            ret = f(*args, **kwds)
            end = time.time()
            if not hasattr(obj, '_instruments'):
                obj._instruments = {}
            if f.__name__ in obj._instruments:
                obj._instruments[f.__name__] += (end-start) * 1000
            else:
                obj._instruments[f.__name__] = (end-start) * 1000
            return ret
        return instrument

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class BaseConnector :
           
    # -----------------------------------------------------------------
    def __init__(self, settings, world, netsettings) :

        self.__Logger = logging.getLogger(__name__)

        self.CurrentStep = 0

        # Get world time
        self.Interval =  float(settings["General"].get("Interval", 0.150))
        self.SecondsPerStep = float(settings["General"].get("SecondsPerStep", 2.0))
        self.StartTimeOfDay = float(settings["General"].get("StartTimeOfDay", 8.0))
        self.RealDayLength = 24.0 * self.Interval / self.SecondsPerStep
        self.vehicle_count = 0
        self.Clock = time.time

        process = settings["General"].get("MultiProcessing", False)
        maxt = settings["General"].get("MaximumTravelers", None)
        timer = settings["General"].get("Timer", False)
        if timer:
            timer = "%s:%s:%s" % (timer["Hours"], timer["Minutes"], timer["Seconds"])

        ## this is an ugly hack because the cygwin and linux
        ## versions of time.clock seem seriously broken
        if platform.system() == 'Windows' :
            self.Clock = time.clock

        if INSTRUMENT:
            if not os.path.exists('stats'):
                os.mkdir('stats')
            strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.ifname = os.path.join('stats', "%s_original_%s.csv" % (strtime, self.__class__.__name__))
            with open(self.ifname, 'w', 0) as csvfile:
                csvfile.write("########\n")
                csvfile.write("Options, MultiProcessing : %s, MaximumTravelers : %s, Interval : %s, Timer: %s\n" % (process, maxt, self.Interval, timer))
                csvfile.write("########\n\n")
                # Base headers
                headers = ['time', 'step', 'vehicles', 'HandleEvent']
                # Annotated headers
                if self.__module__ in INSTRUMENT_HEADERS:
                    headers.extend(INSTRUMENT_HEADERS[self.__module__])

                self.fieldnames = headers
                writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
                writer.writeheader()
                self.exec_start = None
            self.SubscribeEvent(EventTypes.InstrumentEvent, self.HandleInstrumentation)

        # Save network information
        self.NetSettings = netsettings
        self.World = world


    # -----------------------------------------------------------------
    def GetWorldTime(self, currentstep) :
        """
        GetWorldTime -- return the time associated with the step count in hours
        """
        return self.StartTimeOfDay + (currentstep * self.SecondsPerStep) / (60.0 * 60.0)

    # -----------------------------------------------------------------
    def GetWorldTimeOfDay(self, currentstep) :
        return self.GetWorldTime(currentstep) % 24.0

    # -----------------------------------------------------------------
    def GetWorldDay(self, currentstep) :
        return int(self.GetWorldTime(currentstep) / 24.0)

    # -----------------------------------------------------------------
    @property
    def WorldTime(self) : return self.GetWorldTime(self.CurrentStep)

    # -----------------------------------------------------------------
    @property
    def WorldTimeOfDay(self) : return self.GetWorldTimeOfDay(self.CurrentStep)

    # -----------------------------------------------------------------
    @property
    def WorldDay(self) : return self.GetWorldDay(self.CurrentStep)

    def HandleInstrumentation(self, event):
        if hasattr(self, "_instruments"):
            with open(self.ifname, 'a', 0) as csvfile:
            #csv.writer(["%.3f" % delta].append(self.inst_array))
                writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
                d = self._instruments
                if not self.exec_start:
                    self.exec_start = datetime.datetime.now()
                d['time'] = str(datetime.datetime.now() - self.exec_start)
                d['step'] = event.Step
                d['vehicles'] = self.vehicle_count
                #if self.CurrentIteration % 10 == 0:
                #    d['nobjects'], d['mem buffer'] = self.frame.buffersize()
                writer.writerow(d)
                self._instruments = {}
