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

@file    SumoConnector.py
@author  Mic Bowman
@date    2013-12-03

This file defines the SumoConnector class that translates mobdat events
and operations into and out of the sumo traffic simulator.

"""
import os, sys
import logging
import subprocess
import platform
from mobdat.simulator.BaseConnector import instrument
import time
from hla import Position, Quaternion
from mobdat.common.Utilities import get_os
from java.lang import Double

sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

from sumolib import checkBinary

import traci
import traci.constants as tc
import BaseConnector, EventTypes
from mobdat.common import ValueTypes

import math

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class SumoConnector(BaseConnector.BaseConnector) :

    # -----------------------------------------------------------------
    def __init__(self, hlaconn, settings, world, netsettings, cname) :
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)
        self.hlaconn = hlaconn
        self.__Logger = logging.getLogger(__name__)

        # the sumo time scale is 1sec per iteration so we need to scale
        # to the 100ms target for our iteration time, this probably
        # should be computed based on the target step size
        self.TimeScale = 1.0 / self.Interval

        self.ConfigFile = settings["SumoConnector"]["ConfigFile"]
        self.Port = settings["SumoConnector"]["SumoPort"]
        self.TrafficLights = {}

        self.DumpCount = 50
        self.EdgesPerIteration = 25

        self.VelocityFudgeFactor = settings["SumoConnector"].get("VelocityFudgeFactor",0.90)

        self.AverageClockSkew = 0.0

    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    def __NormalizeCoordinate(self,pos) :
        return ValueTypes.Vector3((pos[0] - self.XBase) / self.XSize, (pos[1] - self.YBase) / self.YSize, 0.0)

    # -----------------------------------------------------------------
    # see http://www.euclideanspace.com/maths/geometry/rotations/conversions/eulerToQuaternion/
    # where heading is interesting and bank and attitude are 0
    # -----------------------------------------------------------------
    def __NormalizeAngle(self,heading) :
        # convert to radians
        heading = (2.0 * heading * math.pi) / 360.0
        return ValueTypes.Quaternion.FromHeading(heading)

    # -----------------------------------------------------------------
    def __NormalizeVelocity(self, speed, heading) :
        # i'm not at all sure why the coordinates for speed are off
        # by 270 degrees... but this works
        heading = (2.0 * (heading + 270.0) * math.pi) / 360.0

        # the 0.9 multiplier just makes sure we dont overestimate
        # the velocity because of the time shifting, experience
        # is better if the car falls behind a bit rather than
        # having to be moved back because it got ahead
        x = self.VelocityFudgeFactor * self.TimeScale * speed * math.cos(heading)
        y = self.VelocityFudgeFactor * self.TimeScale * speed * math.sin(heading)

        return ValueTypes.Vector3(x / self.XSize, y / self.YSize, 0.0)

    # -----------------------------------------------------------------
    def _RecomputeRoutes(self) :
        if len(self.CurrentEdgeList) == 0 :
            self.CurrentEdgeList = list(self.EdgeList)

        count = 0
        while self.CurrentEdgeList and count < self.EdgesPerIteration :
            edge = self.CurrentEdgeList.pop()
            traci.edge.adaptTraveltime(edge, traci.edge.getTraveltime(edge))
            count += 1

    # -----------------------------------------------------------------
    @instrument
    def HandleDepartedVehicles(self, currentStep) :
        dlist = traci.simulation.getDepartedIDList()
        for v in dlist :
            traci.vehicle.subscribe(v,[tc.VAR_POSITION, tc.VAR_SPEED, tc.VAR_ANGLE])

            vtype = traci.vehicle.getTypeID(v)
            pos = self.__NormalizeCoordinate(traci.vehicle.getPosition(v))
            pmap = {}
            pmap["ID"] = v
            pmap["VehicleType"] = vtype
            pmap["Position"] = Position(pos.x, pos.y, pos.z)
            self.hlaconn.sendInteraction("HLAinteractionRoot.CreateObject", pmap)

    # -----------------------------------------------------------------
    @instrument
    def HandleArrivedVehicles(self, currentStep) :
        alist = traci.simulation.getArrivedIDList()
        for v in alist :
            pmap = {}
            pmap["ID"] = v
            self.hlaconn.sendInteraction("HLAinteractionRoot.DeleteObject", pmap)
            self.vehicle_count -= 1
            self.__Logger.warn('vehicle %s arrived', v)


    # -----------------------------------------------------------------
    def HandleVehicleUpdates(self, currentStep) :
        changelist = traci.vehicle.getSubscriptionResults()
        for v, info in changelist.iteritems() :
            pos = self.__NormalizeCoordinate(info[tc.VAR_POSITION])
            ang = self.__NormalizeAngle(info[tc.VAR_ANGLE])
            vel = self.__NormalizeVelocity(info[tc.VAR_SPEED], info[tc.VAR_ANGLE])
            amap = {}
            #amap["VehicleName"] = v
            amap["Position"] = Position(pos.x, pos.y, pos.z)
            amap["Velocity"] = Position(vel.x, vel.y, vel.z)
            amap["Angle"] = Quaternion(ang.x, ang.y, ang.z, ang.w)
            self.hlaconn.updateObjectWithName("HLAobjectRoot.Vehicle", v, amap)

    # -----------------------------------------------------------------
    # def HandleRerouteVehicle(self, event) :
    #     traci.vehicle.rerouteTraveltime(str(event.ObjectIdentity))

    # -----------------------------------------------------------------
    @instrument
    def HandleAddVehicleEvent(self, pmap) :
        self.__Logger.warn('add vehicle %s going from %s to %s', pmap["VehicleName"], pmap["DestinationName"], pmap["Source"])
        traci.vehicle.add(pmap["VehicleName"], pmap["DestinationName"], typeID=pmap["VehicleType"])
        traci.vehicle.changeTarget(pmap["VehicleName"], pmap["Source"])
        self.hlaconn.createObject("HLAobjectRoot.Vehicle", pmap["VehicleName"] )
        self.vehicle_count += 1

    # -----------------------------------------------------------------
    # Returns True if the simulation can continue
    @instrument
    def HandleTimerEvent(self, pmap) :
        self.CurrentStep = pmap["CurrentStep"]
        self.CurrentTime = pmap["CurrentTime"]

        # Compute the clock skew
        self.AverageClockSkew = (9.0 * self.AverageClockSkew + (self.Clock() - self.CurrentTime)) / 10.0

        # handle the time scale computation based on the inter-interval
        # times
        # if self.LastStepTime > 0 :
        #     delta = ctime - self.LastStepTime
        #     if delta > 0 :
        #         self.TimeScale = (9.0 * self.TimeScale + 1.0 / delta) / 10.0
        # self.LastStepTime = ctime

        try :
            traci.simulationStep()

            #self.HandleInductionLoops(self.CurrentStep)
            #self.HandleTrafficLights(self.CurrentStep)
            self.HandleDepartedVehicles(self.CurrentStep)
            self.HandleVehicleUpdates(self.CurrentStep)
            self.HandleArrivedVehicles(self.CurrentStep)
        except TypeError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with type error %s" % (str(detail)))
            return
        except ValueError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with value error %s" % (str(detail)))
            return
        except NameError as detail:
            self.__Logger.exception("[sumoconector] simulation step failed with name error %s" % (str(detail)))
            return
        except AttributeError as detail:
            self.__Logger.exception("[sumoconnector] simulation step failed with attribute error %s" % (str(detail)))
            return
        except :
            self.__Logger.exception("[sumoconnector] error occured in simulation step; %s" % (sys.exc_info()[0]))
            return

        self._RecomputeRoutes()

        return True

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self) :
        try :
            idlist = traci.vehicle.getIDList()
            for v in idlist :
                traci.vehicle.remove(v)

            traci.close()
            sys.stdout.flush()

            self.SumoProcess.wait()
            self.__Logger.info('shut down')
        except :
            exctype, value =  sys.exc_info()[:2]
            self.__Logger.warn('shutdown failed with exception type %s; %s' %  (exctype, str(value)))
        self.hlaconn.shutdown()

    # -----------------------------------------------------------------
    def SimulationStart(self) :
        #if platform.system() == 'Windows' or platform.system().startswith("CYGWIN"):
        if get_os().startswith('Windows'):
            sumoBinary = checkBinary('sumo.exe')
        else:
            sumoBinary = checkBinary('sumo')
        self.__Logger.warn("################## Sumo binary: %s", sumoBinary)
        sumoCommandLine = [sumoBinary, "-c", self.ConfigFile, "-l", "sumo.log"]

        self.SumoProcess = subprocess.Popen(sumoCommandLine, stdout=sys.stdout, stderr=sys.stderr)
        traci.init(self.Port)

        self.SimulationBoundary = traci.simulation.getNetBoundary()
        self.XBase = self.SimulationBoundary[0][0]
        self.XSize = self.SimulationBoundary[1][0] - self.XBase
        self.YBase = self.SimulationBoundary[0][1]
        self.YSize = self.SimulationBoundary[1][1] - self.YBase
        self.__Logger.warn("starting sumo connector")

        # initialize the edge list, drop all the internal edges
        self.EdgeList = []
        for edge in traci.edge.getIDList() :
            # this is just to ensure that everything is initialized first time
            traci.edge.adaptTraveltime(edge, traci.edge.getTraveltime(edge))

            # only keep the "real" edges for computation for now
            if not edge.startswith(':') :
                self.EdgeList.append(edge)
        self.CurrentEdgeList = list(self.EdgeList)

        # initialize the traffic light state
        tllist = traci.trafficlights.getIDList()
        for tl in tllist :
            self.TrafficLights[tl] = traci.trafficlights.getRedYellowGreenState(tl)
            traci.trafficlights.subscribe(tl,[tc.TL_RED_YELLOW_GREEN_STATE])

        # initialize the induction loops
        illist = traci.inductionloop.getIDList()
        for il in illist :
            traci.inductionloop.subscribe(il, [tc.LAST_STEP_VEHICLE_NUMBER])


        while not self.hlaconn.ready():
            time.sleep(1.0)

        # subscribe to the events
        self.hlaconn.subscribesInteraction("HLAinteractionRoot.AddVehicle", self.HandleAddVehicleEvent)
        self.hlaconn.subscribesInteraction("HLAinteractionRoot.TimerEvent", self.HandleTimerEvent)
        self.hlaconn.producesInteraction("HLAinteractionRoot.CreateObject")
        self.hlaconn.producesInteraction("HLAinteractionRoot.DeleteObject")

        self.hlaconn.producesObjectAttributes("HLAobjectRoot.Vehicle", ["Position", "Angle", "Velocity", "VehicleName", "VehicleType"])


        # all set... time to get to work!
        # self.HandleEvents()