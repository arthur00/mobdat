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

@file    OpenSimConnector.py
@author  Mic Bowman
@date    2013-12-03

Simple test combining sumo traffic simulation and opensim 3d virtual world.

"""

import os, sys
import logging
from mobdat.common.Utilities import AuthByUserName, GenCoordinateMap,\
    CalculateOSCoordinates, CalculateOSCoordinatesFromScene,\
    GetSceneFromCoordinates
import OpenSimRemoteControl
from mobdat.simulator.BaseConnector import instrument
from mobdat.simulator.Controller import INSTRUMENT_HEADERS

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("SUMO_HOME"), "tools"))
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

import uuid
import BaseConnector, EventHandler, EventTypes
from mobdat.common import ValueTypes

from collections import deque
import Queue, threading, time, platform

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OpenSimUpdateThread(threading.Thread) :

    # -----------------------------------------------------------------
    def __init__(self, workq, scenes, osconnector, vmap, binary = False) :
        threading.Thread.__init__(self)

        self.__Logger = logging.getLogger(__name__)

        self.TotalUpdates = 0
        self.WorkQ = workq
        self.Scenes = scenes
        self.Vehicles2Sim = vmap
        self.Binary = binary
        self.OpenSimConnector = osconnector;

    # -----------------------------------------------------------------
    def run(self) :
        self.ProcessUpdatesLoop()
        
        updates = self.TotalUpdates
        messages = 0
        mbytes = 0
        for _,scene in self.Scenes.items():
            messages += scene["RemoteControl"].MessagesSent
            mbytes += scene["RemoteControl"].BytesSent / 1000000.0
        self.__Logger.info('%d updates sent to OpenSim in %d messages using %f MB',updates, messages, mbytes)

    # -----------------------------------------------------------------
    def ProcessUpdatesLoop(self) :
        while True :
            try :
                # wait synchronously for the first incoming request
                vname = self.WorkQ.get(True)
                if not vname :
                    return

                updates = [vname]
                self.WorkQ.task_done()

                count = 1

                # then grab everything thats in the queue
                while not self.WorkQ.empty() :
                    vname = self.WorkQ.get()
                    if not vname :
                        self.WorkQ.task_done()
                        return

                    updates.append(vname)
                    self.WorkQ.task_done()

                    count += 1
                    if count >= 50 :
                        break

                self.ProcessUpdates(updates)

            except Queue.Empty as _ :
                pass

    # -----------------------------------------------------------------
    def ProcessUpdates(self, vnames) :
        # print 'sending %d update_info' % (len(vnames))

        update_info = {}
        for vname in vnames :
            if vname not in self.Vehicles2Sim :
                self.__Logger.warn("missing vehicle %s in update thread" % (vname))
                continue

            sim = self.Vehicles2Sim[vname]
            if not sim["Name"] in update_info:
                update_info[sim["Name"]] = {}
                update_info[sim["Name"]]["Sim"] = sim
                update_info[sim["Name"]]["Updates"] = []

            if vname not in sim["Vehicles"]:
                self.__Logger.warn("vehicle %s in dictionary, but not in sim. Removing from dictionary." % (vname))
                del self.Vehicles2Sim[vname]
                continue
            vehicle = sim["Vehicles"][vname]
            vid = vehicle.VehicleID
            pos = vehicle.TweenUpdate.Position.ToList()
            #(osx,osy),_ = CalculateOSCoordinates(pos[0], pos[1], self.OpenSimConnector)
            (osx,osy) = CalculateOSCoordinatesFromScene(pos[0], pos[1], sim, self.OpenSimConnector)
            vpos = [osx,osy,pos[2]]
            #if self.Debug == True:
                #if self.TotalUpdates % 100 == 0:
                #    print str(vpos) + " and Scene: " + sim["Name"]
            vvel = vehicle.TweenUpdate.Velocity.ToList()
            vrot = vehicle.TweenUpdate.Rotation.ToList()
            vacc = vehicle.TweenUpdate.Acceleration.ToList()
            update = OpenSimRemoteControl.BulkUpdateItem(vid, vpos, vvel, vrot, vacc)
            update_info[sim["Name"]]["Updates"].append(update)
            vehicle.InUpdateQueue = False

        for _,sinfo in update_info.items():
            count = len(sinfo["Updates"])
            if count > 0 :
                self.TotalUpdates += count
                bulkupdate = sinfo["Updates"]
                sim = sinfo["Sim"]
                result = sim["RemoteControl"].BulkDynamics(bulkupdate)


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OpenSimVehicleDynamics :

    # -----------------------------------------------------------------
    @staticmethod
    def ComputeVelocity(velocity, acceleration, deltat) :
        return velocity + acceleration * deltat
    
    # -----------------------------------------------------------------
    @staticmethod
    def ComputePosition(position, velocity, acceleration, deltat) :
        return position + velocity * deltat + acceleration * (0.5 * deltat * deltat)

    # -----------------------------------------------------------------
    @staticmethod
    def CreateTweenUpdate(oldpos, newpos, deltat) :
        """
        Compute the dynamics (position, velocity and acceleration) that occurs
        at a time between two update events.
        """

        # this is the average acceleration over the time interval
        acceleration = newpos.Velocity.SubVector(oldpos.Velocity) / deltat

        tween = OpenSimVehicleDynamics()
        tween.Position = OpenSimVehicleDynamics.ComputePosition(oldpos.Position, oldpos.Velocity, acceleration, 0.5 * deltat)
        tween.Velocity = OpenSimVehicleDynamics.ComputeVelocity(oldpos.Velocity, acceleration, 0.5 * deltat)
        tween.Rotation = newpos.Rotation # this is just wrong but i dont like quaternion math
        tween.Acceleration = acceleration
        tween.UpdateTime = oldpos.UpdateTime + 0.5 * deltat
        return tween

    # -----------------------------------------------------------------
    def __init__(self) :
        self.Position = ValueTypes.Vector3()
        self.Velocity = ValueTypes.Vector3()
        self.Acceleration = ValueTypes.Vector3()
        self.Rotation = ValueTypes.Quaternion()
        self.UpdateTime = 0

    # -----------------------------------------------------------------
    def InterpolatePosition(self, deltat) :
        return OpenSimVehicleDynamics.ComputePosition(self.Position, self.Velocity, self.Acceleration, deltat)
    

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OpenSimVehicle :

    # -----------------------------------------------------------------
    def __init__(self, vname, vtype, vehicle) :
        self.VehicleName = vname # Name of the sumo vehicle
        self.VehicleType = vtype
        self.VehicleID = vehicle  # UUID of the vehicle object in OpenSim

        self.LastUpdate = OpenSimVehicleDynamics()
        self.TweenUpdate = OpenSimVehicleDynamics()

        self.InUpdateQueue = False

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class OpenSimConnector(EventHandler.EventHandler, BaseConnector.BaseConnector) :

    # -----------------------------------------------------------------
    def __init__(self, evrouter, settings, world, netsettings, cname) :
        """Initialize the OpenSimConnector by creating the opensim remote control handlers.

        Keyword arguments:
        evhandler -- the initialized event handler, EventRegistry type
        settings -- dictionary of settings from the configuration file
        """
        INSTRUMENT_HEADERS[self.__module__].append("CreateObject")
        self.Debug = True
        if self.Debug == True:
            self.debug_ct = 0

        EventHandler.EventHandler.__init__(self, evrouter)
        BaseConnector.BaseConnector.__init__(self, settings, world, netsettings)

        self.__Logger = logging.getLogger(__name__)

        # Get the world size
        wsize =  settings["OpenSimConnector"]["WorldSize"]
        self.WorldSize = ValueTypes.Vector3(wsize[0], wsize[1], wsize[2])

        woffs = settings["OpenSimConnector"]["WorldOffset"]
        self.WorldOffset = ValueTypes.Vector3(woffs[0], woffs[1], woffs[2])

        rsize = settings["OpenSimConnector"]["RegionSize"]
        self.RegionSizeX = rsize[0]
        self.RegionSizeY = rsize[1]

        AuthByUserName(settings)
        self.RegionMap = GenCoordinateMap(settings)
        self.Scenes = settings["OpenSimConnector"]["Scenes"]
        self.Vehicles2Sim = {}

        for name,sim in self.Scenes.items():
            # Initialize the vehicle and vehicle types
            sim["Vehicles"] = {}
            sim["VehicleReuseList"] = {}
            sim["VehicleTypes"] = self.NetSettings.VehicleTypes
            for vname, vinfo in sim["VehicleTypes"].iteritems() :
                sim["VehicleReuseList"][vname] = deque([])
                sim["VehicleTypes"][vname] = vinfo

        # Initialize some of the update control variables
        self.PositionDelta = settings["OpenSimConnector"].get("PositionDelta",0.1)
        self.VelocityDelta = settings["OpenSimConnector"].get("VelocityDelta",0.1)
        self.AccelerationDelta = settings["OpenSimConnector"].get("AccelerationDelta",0.1)
        self.Interpolated = 0
        self.Binary = settings["OpenSimConnector"].get("Binary",False)

        self.UpdateThreadCount = settings["OpenSimConnector"].get("UpdateThreadCount",2)

        self.DumpCount = 50
        self.CurrentStep = 0
        self.CurrentTime = 0
        self.AverageClockSkew = 0.0

        self.Clock = time.time

        ## this is an ugly hack because the cygwin and linux
        ## versions of time.clock seem seriously broken
        if platform.system() == 'Windows' :
            self.Clock = time.clock

    # -----------------------------------------------------------------
    @instrument
    def _FindAssetInObject(self, assetinfo) :
        oname = assetinfo["ObjectName"]
        iname = assetinfo["ItemName"]
        for name,sim in self.Scenes.items():
            conn = sim["RemoteControl"]
            result = conn.FindObjects(pattern = oname, async = False)
            if not result["_Success"] or len(result["Objects"]) == 0 :
                continue

            objectid = result["Objects"][0]
            result = conn.GetObjectInventory(objectid, async = False)
            if not result["_Success"] :
                self.__Logger.warn("Failed to get inventory from container object %s; %s",oname, result["_Message"])
                #sys.exit(-1)
                continue

            for item in result["Inventory"] :
                if item["Name"] == iname :
                    return item["AssetID"]

        self.__Logger.warn("Failed to locate item %s in object %s",iname, oname);
        return None

    # -----------------------------------------------------------------
    @instrument
    def HandleCreateObjectEvent(self,event) :
        OpenSimPosition = event.ObjectPosition.ScaleVector(self.WorldSize).AddVector(self.WorldOffset)
        sim = GetSceneFromCoordinates(OpenSimPosition.x,OpenSimPosition.y,self)

        vtype = sim["VehicleTypes"][event.ObjectType]
        vtypename = vtype.Name
        vname = event.ObjectIdentity
        self.Vehicles2Sim[vname] = sim

        self.__Logger.debug("create vehicle %s with type %s", vname, vtypename)
        
        if len(sim["VehicleReuseList"][vtypename]) > 0 :
            vehicle = sim["VehicleReuseList"][vtypename].popleft()
            # self.__Logger.debug("reuse vehicle %s for %s", vehicle.VehicleName, vname)

            # remove the old one from the vehicle map
            del sim["Vehicles"][vehicle.VehicleName]
            del self.Vehicles2Sim[vehicle.VehicleName]
            
            # update it and add it back to the map with the new name
            vehicle.VehicleName = vname
            sim["Vehicles"][vname] = vehicle
            self.Vehicles2Sim[vname] = sim
            return

        vuuid = str(uuid.uuid4())
        sim["Vehicles"][vname] = OpenSimVehicle(vname, vtypename, vuuid)

        assetid = vtype.AssetID
        if type(assetid) == dict :
            assetid = self._FindAssetInObject(assetid)
            vtype.AssetID = assetid

        conn = sim["RemoteControl"]
        stime = time.time()
        result = conn.CreateObject(vtype.AssetID, objectid=vuuid, name=vname, parm=vtype.StartParameter, async=True)
        etime = time.time()
        if not hasattr(self, "_instruments"):
            self._instruments = {}
        self._instruments["CreateObject"] = (etime-stime)*1000
 
        # self.__Logger.debug("create new vehicle %s with id %s", vname, vuuid)
        return True

    # -----------------------------------------------------------------
    @instrument
    def HandleDeleteObjectEvent(self,event) :
        """Handle the delete object event. In this case, rather than delete the
        object from the scene completely, mothball it in a location well away from
        the simulation.
        """
        
        vname = event.ObjectIdentity
        if vname not in self.Vehicles2Sim :
            self.__Logger.warn("attempt to delete unknown vehicle %s" % (vname))
            return True
        sim = self.Vehicles2Sim[vname]

        vehicle = sim["Vehicles"][event.ObjectIdentity]
        sim["VehicleReuseList"][vehicle.VehicleType].append(vehicle)

        mothball = OpenSimVehicleDynamics()
        mothball.Position = ValueTypes.Vector3(10.0, 10.0, 500.0);
        mothball.UpdateTime = self.CurrentTime

        vehicle.TweenUpdate = mothball
        vehicle.LastUpdate = mothball
        vehicle.InUpdateQueue = True

        self.WorkQ.put(vehicle.VehicleName)

        # result = self.OpenSimConnector.DeleteObject(vehicleID)
        # result = sim["RemoteControl"].DeleteObject(vehicleID)

        # print "Deleted vehicle " + vname + " with id " + str(vehicle)
        return True

    # -----------------------------------------------------------------
    @instrument
    def HandleObjectDynamicsEvent(self,event) :
        vname = event.ObjectIdentity
        if vname not in self.Vehicles2Sim :
            self.__Logger.warn("attempt to update unknown vehicle %s" % (vname))
            return True

        sim = self.Vehicles2Sim[vname]
        vehicle = sim["Vehicles"][vname]

        deltat = self.CurrentTime - vehicle.LastUpdate.UpdateTime
        if deltat == 0 : return True

        # Save the dynamics information, acceleration is only needed in the tween update
        update = OpenSimVehicleDynamics()
        update.Position = event.ObjectPosition.ScaleVector(self.WorldSize).AddVector(self.WorldOffset)
        update.Velocity = event.ObjectVelocity.ScaleVector(self.WorldSize)
        update.Rotation = event.ObjectRotation
        update.UpdateTime = self.CurrentTime

        #if self.Debug == True:
        #    self.__Logger.warn("Normalized position: %s, Sumo position: %s, OpenSim Position: %s" %
        #                       (event.ObjectPosition,event.ObjectPosition.ScaleVector(ValueTypes.Vector3(38560.0,38560.0,100.0)),update.Position))

        # Compute the tween update (the update halfway between the last reported position and
        # the current reported position, with the tween we know the acceleration as opposed to
        # the current update where we dont know acceleration
        tween = OpenSimVehicleDynamics.CreateTweenUpdate(vehicle.LastUpdate, update, deltat)

        # if the vehicle is already in the queue then just save the new values
        # and call it quits
        if vehicle.InUpdateQueue :
            vehicle.TweenUpdate = tween
            vehicle.LastUpdate = update
            return True

        # check to see if the change in position or velocity is signficant enough to
        # warrant sending an update, emphasize velocity changes because dead reckoning
        # will handle position updates reasonably if the velocity is consistent

        # Condition 1: this is not the first update
        if vehicle.LastUpdate.UpdateTime > 0 :
            # Condition 2: the acceleration is about the same
            if vehicle.TweenUpdate.Acceleration.ApproxEquals(tween.Acceleration, self.AccelerationDelta) :
                # Condition 3: the position check, need to handle lane changes so this check
                # is not redundant with acceleration check
                ideltat = tween.UpdateTime - vehicle.TweenUpdate.UpdateTime
                ipos = vehicle.TweenUpdate.InterpolatePosition(ideltat)
                if ipos.ApproxEquals(tween.Position,self.PositionDelta) :
                    self.Interpolated += 1
                    return True
            
        vehicle.TweenUpdate = tween
        vehicle.LastUpdate = update
        vehicle.InUpdateQueue = True

        # if self.WorkQ.full() :
        #     print "full queue at time step %d" % (self.CurrentStep)

        self.WorkQ.put(vname)

        # print "Moved vehicle " + vname + " with id " + str(vehicle) + " to location " + str(position)
        return True

    # -----------------------------------------------------------------
    # Returns True if the simulation can continue
    def HandleTimerEvent(self, event) :
        self.CurrentStep = event.CurrentStep
        self.CurrentTime = event.CurrentTime

        # Compute the clock skew
        self.AverageClockSkew = (9.0 * self.AverageClockSkew + (self.Clock() - self.CurrentTime)) / 10.0

        if (self.CurrentStep % int(5.0 / 0.2)) == 0:
                self.__Logger.warn('[%s] number of vehicles in simulation: %s', self.CurrentStep, len(self.Vehicles2Sim))
        # Send the event if we need to
        if (self.CurrentStep % self.DumpCount) == 0 :
            event = EventTypes.OpenSimConnectorStatsEvent(self.CurrentStep, self.AverageClockSkew)
            self.PublishEvent(event)

    # -----------------------------------------------------------------
    def HandleShutdownEvent(self, event) :
        for name,sim in self.Scenes.items():
            conn = sim["RemoteControl"]
            # clean up all the outstanding vehicles
            for vehicle in sim["Vehicles"].itervalues() :
                conn.DeleteObject(vehicle.VehicleID)

            # print 'waiting for update thread to terminate'
            for count in range(self.UpdateThreadCount) :
                self.WorkQ.put(None)

            for count in range(self.UpdateThreadCount) :
                self.UpdateThreads[count].join()

            #self.__Logger.info('create/delete messages sent to opensim: %d', self.OpenSimConnector.MessagesSent)
            self.__Logger.info('%d vehicles interpolated correctly', self.Interpolated)
            self.__Logger.info('shut down')


    # -----------------------------------------------------------------
    def SimulationStart(self) :
        # set up the simulator time to match, the daylength is the number of wallclock
        # hours necessary to complete one virtual day
        for _,scene in self.Scenes.items():
            scene["RemoteControl"].SetSunParameters(self.StartTimeOfDay, daylength=self.RealDayLength)

        self.__Logger.debug("Debug logger is on")
        # Connect to the event registry
        self.SubscribeEvent(EventTypes.EventCreateObject, self.HandleCreateObjectEvent)
        self.SubscribeEvent(EventTypes.EventDeleteObject, self.HandleDeleteObjectEvent)
        self.SubscribeEvent(EventTypes.EventObjectDynamics, self.HandleObjectDynamicsEvent)
        self.SubscribeEvent(EventTypes.TimerEvent, self.HandleTimerEvent)
        self.SubscribeEvent(EventTypes.ShutdownEvent, self.HandleShutdownEvent)

        # Start the worker threads
        self.WorkQ = Queue.Queue(0)
        self.UpdateThreads = []
        for _ in range(self.UpdateThreadCount) :
            thread = OpenSimUpdateThread(self.WorkQ, self.Scenes, self, self.Vehicles2Sim, self.Binary)
            thread.start()
            self.UpdateThreads.append(thread)

        # all set... time to get to work!
        self.HandleEvents()

