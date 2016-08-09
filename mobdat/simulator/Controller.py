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

@file    Controller.py
@author  Mic Bowman
@date    2013-12-03

This module defines routines for controling the mobdat simulator. The controller
sets up the connectors and then drives the simulation through the periodic
clock ticks.

"""

import os, sys
import logging
import csv
from threading import Thread
import datetime
from hla.hla_connector import HLAConnector

sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

# -----------------------------------------------------------------
# -----------------------------------------------------------------
SimulatorStartup = False
SimulatorShutdown = False
CurrentIteration = 0
FinalIteration = 0

INSTRUMENT = False
INSTRUMENT_HEADERS = {}

import platform, time, threading, cmd
import EventTypes
from mobdat.common.Utilities import AuthByUserName
from mobdat.common import LayoutSettings, WorldInfo
from threading import Thread

# -----------------------------------------------------------------
# -----------------------------------------------------------------
import SumoConnector, OpenSimConnector, SocialConnector

_SimulationControllers = {
    'sumo' : SumoConnector.SumoConnector,
    'social' : SocialConnector.SocialConnector,
    'opensim' : OpenSimConnector.OpenSimConnector
    }

logger = logging.getLogger(__name__)



class TimerThread(threading.Thread) :
    # -----------------------------------------------------------------
    def __init__(self, settings) :
        """
        This thread will drive the simulation steps by sending periodic clock
        ticks that each of the connectors can process.

        Arguments:
        evrouter -- the initialized event handler object
        interval -- time between successive clock ticks
        """

        threading.Thread.__init__(self)
        self.hla_conn = HLAConnector(settings, "TimerFederate")
        self.hla_conn.start()
        self.__Logger = logging.getLogger(__name__)
        self.IntervalTime = float(settings["General"]["Interval"])
        timer = settings["General"].get("Timer", None)
        if timer:
            seconds = 0
            minutes = 0
            hours = 0
            if "Seconds" in timer:
                seconds = timer["Seconds"]
            if "Minutes" in timer:
                minutes = timer["Minutes"]
            if "Hours" in timer:
                hours = timer["Hours"]
            self.timer = datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)
        else:
            self.timer = None

        global FinalIteration
        FinalIteration = settings["General"].get("TimeSteps",0)

        self.Clock = time.time

        ## this is an ugly hack because the cygwin and linux
        ## versions of time.clock seem seriously broken
        #if platform.system() == 'Windows' :
        #    self.Clock = time.clock

    # -----------------------------------------------------------------
    def run(self) :
        global SimulatorStartup, SimulatorShutdown
        global FinalIteration, CurrentIteration

        while not self.hla_conn.ready():
            time.sleep(1.0)

        self.hla_conn.producesInteraction("HLAinteractionRoot.TimerEvent")
        SimulatorStartup = True

        # Wait for the signal to start the simulation, this allows all of the
        # connectors to initialize
        #while not (SimulatorStartup or SimulatorShutdown):
        #    time.sleep(5.0)

        # Start the main simulation loop
        self.__Logger.debug("start main simulation loop")
        starttime = self.Clock()

        # timer can be set to stop the simulation automatically at a configured time
        if self.timer:
            # save start time, for checking when the application should be stopped
            self.exec_start = datetime.datetime.now()

        CurrentIteration = 0
        pmap = {}
        while not SimulatorShutdown :
            if FinalIteration > 0 and CurrentIteration >= FinalIteration :
                break

            #event = EventTypes.TimerEvent(CurrentIteration, stime)
            #self.EventRouter.RouterQueue.put(event)
            stime = self.Clock()

            pmap["CurrentStep"] = CurrentIteration
            self.hla_conn.sendInteraction("HLAinteractionRoot.TimerEvent", pmap)

            etime = self.Clock()

            if (etime - stime) < self.IntervalTime :
                time.sleep(self.IntervalTime - (etime - stime))

            CurrentIteration += 1

        # compute a few stats
        elapsed = self.Clock() - starttime
        if CurrentIteration > 0:
            avginterval = 1000.0 * elapsed / CurrentIteration
            self.__Logger.warn("%d iterations completed with an elapsed time %f or %f ms per iteration", CurrentIteration, elapsed, avginterval)

        # send the shutdown events
        #event = EventTypes.ShutdownEvent(False)
        #self.EventRouter.RouterQueue.put(event)

        SimulatorShutdown = True

# -----------------------------------------------------------------
# -----------------------------------------------------------------
class MobdatController(cmd.Cmd) :
    pformat = 'mobdat [{0}]> '

    # -----------------------------------------------------------------
    def __init__(self, evrouter, logger, autostart) :
        cmd.Cmd.__init__(self)

        self.prompt = self.pformat.format(CurrentIteration)
        self.EventRouter = evrouter
        self.__Logger = logger
        if autostart:
            self.do_start(None)

    # -----------------------------------------------------------------
    def postcmd(self, flag, line) :
        self.prompt = self.pformat.format(CurrentIteration)
        return flag

    # -----------------------------------------------------------------
    def do_stopat(self, args) :
        """stopat iteration
        Stop sending timer events and shutdown the simulator after the specified iteration
        """
        pargs = args.split()
        try :
            global FinalIteration
            FinalIteration = int(pargs[0])
        except :
            print 'Unable to parse input parameter %s' % args

    # -----------------------------------------------------------------
    def do_start(self, args) :
        """start
        Start the simulation after all connectors are initialized
        """
        self.__Logger.warn("starting the timer loop")

        global SimulatorStartup
        SimulatorStartup = True

    # -----------------------------------------------------------------
    def do_exit(self, args) :
        """exit
        Shutdown the simulator and exit the command loop
        """

        self.__Logger.warn("stopping the timer loop")

        # kill the timer if it hasn't already shutdown
        global SimulatorShutdown
        SimulatorShutdown = True

        return True

    # -----------------------------------------------------------------
    def do_shutdown(self, args) :
        self.do_exit(args)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
def Controller(settings) :
    """
    Controller is the main entry point for driving the simulation.

    Arguments:
    settings -- nested dictionary with variables for configuring the connectors
    """

    laysettings = LayoutSettings.LayoutSettings(settings)
    # load the world
    infofile = settings["General"].get("WorldInfoFile","info.js")
    logger.info('loading world data from %s',infofile)
    world = WorldInfo.WorldInfo.LoadFromFile(infofile)
    autostart = settings["General"].get("AutoStart", False)

    cnames = settings["General"].get("Connectors",['sumo', 'opensim', 'social', 'stats'])
    connectors = []
    hla_connectors = {}

    # start the timer thread
    thread = TimerThread(settings)

    for cname in cnames :
        if cname not in _SimulationControllers :
            logger.warn('skipping unknown simulation connector; %s' % (cname))
            continue

        hla_conn = HLAConnector(settings, cname)
        hla_conn.start()
        #while not hla_conn.ready():
        #    time.sleep(1)
        connector = _SimulationControllers[cname](hla_conn, settings, world, laysettings, cname)
        connproc = Thread(target=connector.SimulationStart, args=())
        connproc.start()
        connectors.append(connproc)

    thread.start()
    #controller = MobdatController(evrouter, logger, autostart)
    #controller.cmdloop()

    del world

    thread.join()

    # send the shutdown event to the connectors
    for connproc in connectors :
        connproc.join()

    # and send the shutdown event to the router
    #event = EventTypes.ShutdownEvent(True)
    #evrouter.RouterQueue.put(event)

    #evrouterproc.join()
