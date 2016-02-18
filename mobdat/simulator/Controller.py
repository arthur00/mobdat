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
import cadis.frame as frame_module
from cadis.frame import Frame
from cadis.store.simplestore import SimpleStore
from cadis.store.remotestore import RemoteStore
from prime import PrimeSimulator

sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import platform, time, threading, cmd
import EventRouter, EventTypes
from mobdat.common.Utilities import AuthByUserName
from mobdat.common import LayoutSettings, WorldInfo
from multiprocessing import Process

# -----------------------------------------------------------------
# -----------------------------------------------------------------
import SumoConnector, OpenSimConnector, SocialConnector, StatsConnector

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
SimulatorStartup = False
SimulatorShutdown = False
CurrentIteration = 0
FinalIteration = 0

# -----------------------------------------------------------------
# -----------------------------------------------------------------
class MobdatController(cmd.Cmd) :
    pformat = 'mobdat [{0}]> '

    # -----------------------------------------------------------------
    def __init__(self, logger, connectors) :
        cmd.Cmd.__init__(self)
        self.connectors = connectors
        self.prompt = self.pformat.format(CurrentIteration)
        self.__Logger = logger

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

        frame_module.SimulatorStartup = True

    def do_pause(self, args) :
        """start
        Start the simulation after all connectors are initialized
        """
        self.__Logger.warn("pausing all simulations")

        frame_module.SimulatorPaused = True

    def do_unpause(self, args) :
        """start
        Start the simulation after all connectors are initialized
        """
        self.__Logger.warn("unpausing simulations")

        frame_module.SimulatorPaused = False

    # -----------------------------------------------------------------
    def do_exit(self, args) :
        """exit
        Shutdown the simulator and exit the command loop
        """

        self.__Logger.warn("stopping the timer loop")

        frame_module.SimulatorStartup = True
        # kill the timer if it hasn't already shutdown
        frame_module.SimulatorShutdown = True

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
    #laysettings = None
    # load the world
    infofile = settings["General"].get("WorldInfoFile","info.js")
    logger.info('loading world data from %s',infofile)
    world = WorldInfo.WorldInfo.LoadFromFile(infofile)
    #world = None

    cnames = settings["General"].get("Connectors",['sumo', 'opensim', 'social', 'stats'])
    #if 'opensim' in cnames:
    #    cnames.remove('opensim')
    #    for sname in settings["OpenSimConnector"]["Scenes"].keys():
    #        _SimulationControllers['osc:'+sname] = OpenSimConnector.OpenSimConnector
    #        cnames.append('osc:'+sname)
    # evrouter = EventRouter.EventRouter()
    # initialize the connectors first
    connectors = []
    store = SimpleStore()
    #store= RemoteStore()
    for cname in cnames :
        if cname not in _SimulationControllers :
            logger.warn('skipping unknown simulation connector; %s' % (cname))
            continue

        cframe = Frame(store)
        connector = _SimulationControllers[cname](settings, world, laysettings, cname, cframe)
        cframe.attach(connector)
        #connproc = Process(target=connector.SimulationStart, args=())
        #connproc.start()
        connectors.append(cframe)
        cframe.go()
            
    #evrouterproc = Process(target=evrouter.RouteEvents, args=())
    #evrouterproc.start()

    # start the timer thread
    #thread = TimerThread(evrouter, settings)
    #thread.start()

    controller = MobdatController(logger, connectors)
    controller.cmdloop()

    #thread.join()

    # send the shutdown event to the connectors
    for connproc in connectors :
        connproc.join()
    print "closing down controller"
    sys.exit(0)
    # and send the shutdown event to the router
    #event = EventTypes.ShutdownEvent(True)
    #evrouter.RouterQueue.put(event)

    #evrouterproc.join()
