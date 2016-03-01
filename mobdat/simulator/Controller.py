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

import logging
from multiprocessing import Process, Manager
import os, sys
import platform, time, threading, cmd
import pydevd

import EventRouter, EventTypes
import SumoConnector, OpenSimConnector, SocialConnector, StatsConnector
from cadis.frame import Frame
import cadis.frame as frame_module
from cadis.store.remotestore import RemoteStore, PythonRemoteStore
from cadis.store.simplestore import SimpleStore
from mobdat.common import LayoutSettings, WorldInfo
from mobdat.common.Utilities import AuthByUserName
from prime import PrimeSimulator
import datetime


sys.path.append(os.path.join(os.environ.get("OPENSIM", "/share/opensim"), "lib", "python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))


# -----------------------------------------------------------------
# -----------------------------------------------------------------

_SimulationControllers = {
    'sumo' : SumoConnector.SumoConnector,
    'social' : SocialConnector.SocialConnector,
    'stats' : StatsConnector.StatsConnector,
    'opensim' : OpenSimConnector.OpenSimConnector,
    'prime' : PrimeSimulator.PrimeSimulator
    }

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------
# -----------------------------------------------------------------
CurrentIteration = 0
FinalIteration = 0

# -----------------------------------------------------------------
# -----------------------------------------------------------------
class MobdatController(cmd.Cmd) :
    pformat = 'mobdat [{0}]> '

    # -----------------------------------------------------------------
    def __init__(self, logger, connectors, cmd_dict) :
        cmd.Cmd.__init__(self)
        self.connectors = connectors
        self.prompt = self.pformat.format(CurrentIteration)
        self.__Logger = logger
        self.cmds = cmd_dict

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
        ready = False
        while(not ready):
            ready = True
            for k,v in self.cmds["Apps"].items():
                if v != "Ready":
                    print "Application %s not ready yet." % k
                    ready = False
            if not ready:
                time.sleep(1)
        self.cmds["SimulatorStartup"] = True

    def do_pause(self, args) :
        """start
        Start the simulation after all connectors are initialized
        """
        self.__Logger.warn("pausing all simulations")

        self.cmds["SimulatorPaused"] = True

    def do_unpause(self, args) :
        """start
        Start the simulation after all connectors are initialized
        """
        self.__Logger.warn("unpausing simulations")

        self.cmds["SimulatorPaused"] = False

    # -----------------------------------------------------------------
    def do_exit(self, args) :
        """exit
        Shutdown the simulator and exit the command loop
        """

        self.__Logger.warn("stopping the timer loop")

        self.cmds["SimulatorStartup"] = True
        self.cmds["SimulatorShutdown"] = True


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
    # laysettings = None
    # load the world
    infofile = settings["General"].get("WorldInfoFile", "info.js")
    logger.info('loading world data from %s', infofile)
    world = WorldInfo.WorldInfo.LoadFromFile(infofile)
    # world = None

    cnames = settings["General"].get("Connectors", ['sumo', 'opensim', 'social', 'stats'])
    store_type = settings["General"].get("Store", "SimpleStore")
    process = settings["General"].get("MultiProcessing", False)
    timer = settings["General"].get("Timer", None)
    if timer:
        secs = 0
        minutes = 0
        hours = 0
        if "Seconds" in timer:
            seconds = timer["Seconds"]
        if "Minutes" in timer:
            minutes = timer["Minutes"]
        if "Hours" in timer:
            hours = timer["Hours"]
        timer = datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours)

    connectors = []

    if store_type == "RemoteStore":
        manager = Manager()
        cmd_dict = manager.dict()
        cmd_dict["Apps"] = manager.dict()
    else:
        cmd_dict = {}
        cmd_dict["Apps"] = {}
    cmd_dict["SimulatorStartup"] = False
    cmd_dict["SimulatorShutdown"] = False
    cmd_dict["SimulatorPaused"] = False

    if store_type == "RemoteStore":
        Store = PythonRemoteStore
    elif store_type == "SimpleStore":
        Store = SimpleStore
    else: #default to SimpleStore
        Store = SimpleStore

    if process and Store == SimpleStore:
        logger.warn("Cannot use multiprocessing with SimpleStore. Continuing with Threading.")
        process = False

    for cname in cnames :
        if cname not in _SimulationControllers :
            logger.warn('skipping unknown simulation connector; %s' % (cname))
            continue

        cframe = Frame(Store(), process)
        connector = _SimulationControllers[cname](settings, world, laysettings, cname, cframe)
        cframe.attach(connector)
        connectors.append(cframe)
        cframe.go(cmd_dict, timer)

    controller = MobdatController(logger, connectors, cmd_dict)
    controller.cmdloop()

    for connproc in connectors :
        connproc.join()
    print "closing down controller"
    sys.exit(0)
