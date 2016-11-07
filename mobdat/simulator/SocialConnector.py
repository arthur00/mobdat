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

@file    SocialConnector.py
@author  Mic Bowman
@date    2013-12-03

This module defines the SocialConnector class. This class implements
the social (people) aspects of the mobdat simulation.

"""

import os, sys
import logging
import json

from hla.hla_connector import HLAConnector
import time

sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import heapq
from mobdat.simulator.BaseConnector import instrument
import BaseConnector, EventTypes, Traveler

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class SocialConnector(BaseConnector.BaseConnector) :
           
    # -----------------------------------------------------------------
    def __init__(self, hlaconn, settings, world, netsettings, cname) :
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)

        self.__Logger = logging.getLogger(__name__)
        self.hlaconn = hlaconn
        self.MaximumTravelers = int(settings["General"].get("MaximumTravelers", 0))
        self.TripCallbackMap = {}
        self.TripTimerEventQ = []
        self.DataFolder = settings["General"]["Data"]
        if "ExperimentMode" in settings["SocialConnector"]:
            self.ExperimentMode = settings["SocialConnector"]["ExperimentMode"]
            if self.ExperimentMode:
                try:
                    fname = settings["Experiment"]["TravelerFilePath"]
                    data_path = settings["General"]["Data"]

                    with open(os.path.join(data_path,fname)) as data_file:
                        self.TravelerList = json.load(data_file)
                except:
                    self.__Logger.error("could not read traveler information for experiment. Aborting experiment mode.")
                    self.ExperimentMode = False
        else:
            self.ExperimentMode = False

        self.Travelers = {}
        self.CreateTravelers()

        self.__Logger.warn('SocialConnector initialization complete')

    # -----------------------------------------------------------------
    def AddTripToEventQueue(self, trip) :
        heapq.heappush(self.TripTimerEventQ, trip)

    # -----------------------------------------------------------------
    def CreateTravelers(self) :
        #for person in self.PerInfo.PersonList.itervalues() :
        count = 0
        if self.ExperimentMode:
            self.__Logger.warn('Running SocialConnector in experiment mode.')
            for name in self.TravelerList:
                person = self.World.FindNodeByName(name)
                if count % 100 == 0 :
                    self.__Logger.warn('%d travelers created', count)
                traveler = Traveler.Traveler(person, self)
                self.Travelers[name] = traveler
                count += 1
        else:
            for name, person in self.World.IterNodes(nodetype = 'Person') :
                if count % 100 == 0 :
                    self.__Logger.warn('%d travelers created', count)

                traveler = Traveler.Traveler(person, self)
                self.Travelers[name] = traveler

                count += 1
                if self.MaximumTravelers > 0 and self.MaximumTravelers < count :
                    break
        del self.World

            
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # EVENT GENERATORS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # -----------------------------------------------------------------
    @instrument
    def GenerateAddVehicleEvent(self, trip) :
        """
        GenerateAddVehicleEvent -- generate an AddVehicle event to start
        a new trip

        trip -- Trip object initialized with traveler, vehicle and destination information
        """


        vname = str(trip.VehicleName)
        vtype = str(trip.VehicleType)
        rname = str(trip.Source.Capsule.DestinationName)
        tname = str(trip.Destination.Capsule.SourceName)
        # self.__Logger.debug('add vehicle %s from %s to %s',vname, rname, tname)

        # save the trip so that when the vehicle arrives we can get the trip
        # that caused the car to be created
        self.TripCallbackMap[vname] = trip


        addv = {}
        addv["VehicleName"] = vname
        addv["VehicleType"] = vtype
        addv["DestinationName"] = rname
        addv["Source"] = tname

        self.hlaconn.sendInteraction("HLAinteractionRoot.AddVehicle", addv)
        self.vehicle_count += 1

    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # EVENT HANDLERS
    # XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

    # -----------------------------------------------------------------
    @instrument
    def HandleDeleteObjectEvent(self, pmap) :
        """
        HandleDeleteObjectEvent -- delete object means that a car has completed its
        trip so record the stats and add the next trip for the person

        event -- a DeleteObject event object
        """

        #vname = event.ObjectIdentity
        vname = pmap["ID"]
        
        trip = self.TripCallbackMap.pop(vname)
        trip.TripCompleted(self)
        self.vehicle_count -= 1

    # -----------------------------------------------------------------
    @instrument
    def HandleTimerEvent(self, pmap) :
        """
        HandleTimerEvent -- timer event happened, process pending events from
        the eventq

        event -- Timer event object
        """
        #self.CurrentStep = event.CurrentStep
        self.CurrentStep = pmap["CurrentStep"]

        if self.CurrentStep % 100 == 0 :
            wtime = self.WorldTime
            qlen = len(self.TripTimerEventQ)
            stime = self.TripTimerEventQ[0].ScheduledStartTime if self.TripTimerEventQ else 0.0
            self.__Logger.warn('at time %0.3f, timer queue contains %s elements, next event scheduled for %0.3f', wtime, qlen, stime)

        while self.TripTimerEventQ :
            if self.TripTimerEventQ[0].ScheduledStartTime > self.WorldTime :
                break

            trip = heapq.heappop(self.TripTimerEventQ)
            trip.TripStarted(self)

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self) :
        self.hlaconn.shutdown()

    # -----------------------------------------------------------------
    def SimulationStart(self) :
        #self.SubscribeEvent(EventTypes.EventDeleteObject, self.HandleDeleteObjectEvent)
        #self.SubscribeEvent(EventTypes.TimerEvent, self.HandleTimerEvent)
        #self.SubscribeEvent(EventTypes.ShutdownEvent, self.HandleShutdownEvent)

        # all set... time to get to work!
        while not self.hlaconn.ready():
            time.sleep(1.0)
        self.hlaconn.producesInteraction("HLAinteractionRoot.AddVehicle")
        self.hlaconn.subscribesInteraction("HLAinteractionRoot.DeleteObject", self.HandleDeleteObjectEvent)
        self.hlaconn.subscribesInteraction("HLAinteractionRoot.TimerEvent", self.HandleTimerEvent)
        #self.HandleEvents()
