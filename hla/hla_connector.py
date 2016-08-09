'''
Created on Jul 11, 2016

@author: arthurvaladares
'''

from __future__ import absolute_import
import sys
import time
import datetime
from collections import defaultdict
import jarray
from java.lang import String

from hla.rti1516e import RtiFactoryFactory, RTIambassador,\
    AttributeHandleValueMap, AttributeHandleValueMapFactory,\
    ParameterHandleValueMapFactory
from hla.rti1516e.time import HLAfloat64TimeFactory

from hla import PositionCoder, JavaFederate, JavaFederateSettings, Position
from hla.rti1516e import CallbackModel
import logging
from java.io import File
from threading import Thread
import xml.etree.ElementTree as et

from hla.rti1516e.encoding import HLAASCIIchar, HLAASCIIstring, HLAboolean,\
    HLAbyte, HLAfloat32LE, HLAfloat32BE, HLAfloat64BE, HLAfloat64LE,\
    HLAinteger16LE, HLAinteger16BE, HLAinteger32BE, HLAinteger32LE,\
    HLAinteger64BE, HLAinteger64LE, HLAoctet, HLAoctetPairBE, HLAoctetPairLE,\
    HLAopaqueData, HLAunicodeChar, HLAunicodeString

class FOMAttribute:
    def __init__(self, name, fom_type):
        self.name = name
        self.type = fom_type

class HLAConnector(Thread):
    def __init__(self, settings, name, obj_callback=None, int_callback=None):
        Thread.__init__(self)
        self.logger = logging.getLogger(__name__)
        self.settings = JavaFederateSettings()

        self.settings.federateName = name
        self.settings.federateType = settings["HLA"]["FederateType"]
        self.settings.federationName = settings["HLA"]["FederationName"]

        # Read FOM data types
        self.prefix = '{http://standards.ieee.org/IEEE1516-2010}'
        self.fomfile = settings["HLA"]["FOM"]
        self.fomroot = et.parse(self.fomfile).getroot()
        self.objatt2types = self.__readFOMObjects(settings)
        self.intpar2types = self.__readFOMInteractions(settings)

        timestep = settings["General"].get("TimeSteps", 0.2)
        self.settings.timeStep = long(timestep*1000)
        self.__init_encoders()

        # For each object (string), stores { attribute (string) : type (string) }
        self.obj2attributes = {}
        self.int2handle = {}
        self.int2param = {}
        self.int2callback = {}

        self.objsimcallback = obj_callback
        self.intsimcallback = int_callback

        self.federate = JavaFederate(self.settings, self.obj_callback, self.int_callback)
        self.shutdown = False

    def __readFOMObjects(self, settings):
        prefix = self.prefix
        objects = self.fomroot.find('%sobjects' % prefix)[0][1:]
        res = {}
        for obj in objects:
            objname = 'HLAobjectRoot.' + obj.find('%sname' % prefix).text
            #TODO: Asssuming all objects are children of HLAObjectRoot and there
            # are no nested objects.
            res[objname] = {}
            for att in obj.findall('%sattribute' % prefix):
                att_name = att.find('%sname' % prefix).text
                att_type = att.find('%sdataType' % prefix).text
                res[objname][att_name] = att_type
        return res

    def __readFOMInteractions(self, settings):
        prefix = self.prefix
        interactions = self.fomroot.find('%sinteractions' % prefix)[0][1:]
        res = {}
        for i in interactions:
            objname = 'HLAinteractionRoot.' + i.find('%sname' % prefix).text
            #TODO: Asssuming all objects are children of HLAObjectRoot and there
            # are no nested objects.
            res[objname] = {}
            for att in i.findall('%sparameter' % prefix):
                att_name = att.find('%sname' % prefix).text
                att_type = att.find('%sdataType' % prefix).text
                res[objname][att_name] = att_type
        return res

    def ready(self):
        return self.federate.ready()

    def run(self):
        self.federate.runFederate()

    def int_callback(self, int_handle, parameter_map):
        print "## int callback"
        print int_handle
        print parameter_map
        int_params = {}
        intname = self.federate.getInteractionClassName(int_handle)
        for key in parameter_map:
            int_params[key] = {}
            parname = self.federate.getParameterName(int_handle, key)
            inttype = self.intpar2types[intname][parname]
            typeinst = self.encoders[inttype]()
            typeinst.decode(parameter_map[key])
            v = typeinst.getValue()
            print parname + " = " + str(v)
            int_params[parname] = v
        if self.int2callback[intname]:
            self.int2callback[intname](int_params)

    def obj_callback(self, obj_instance, attribute_map):
        print "### obj callback"
        print obj_instance
        print attribute_map
        obj_updates = {}
        for key in attribute_map:
            print "### key %s" % key
            obj_updates[obj_instance] = {}
            attname = self.federate.getAttributeNameFromInstance(obj_instance, key)
            objname = self.federate.getObjectClassName(obj_instance)
            atttype = self.objatt2types[objname][attname]
            typeinst = self.encoders[atttype]()
            typeinst.decode(attribute_map[key])
            v = typeinst.getValue()
            print attname + " = " + str(v)
            obj_updates[obj_instance][attname] = v
        if self.objsimcallback:
            self.objsimcallback(obj_updates)


    # attribute_map : <attribute_name> : <attribute_type_string>
    def registerAttributes(self, obj_name, attribute_map):
        self.obj2attributes[obj_name] = {}
        for att_name, att_type in attribute_map.items():
            self.obj2attributes[obj_name][att_name] = att_type

    # parameter_map : <parameter_name> : <parameter_type_string>
    def registerParameters(self, intClass, parameter_map):
        self.int2param[intClass] = {}
        for att_name, att_type in parameter_map.items():
            self.int2param[intClass][att_name] = att_type

    def __init_encoders(self):
        self.encoders = {}
        self.encoder_factory = RtiFactoryFactory.getRtiFactory().getEncoderFactory()
        self.encoders["HLAASCIIchar"] = self.encoder_factory.createHLAASCIIchar
        self.encoders["HLAASCIIstring"] = self.encoder_factory.createHLAASCIIstring
        self.encoders["HLAboolean"] = self.encoder_factory.createHLAboolean
        self.encoders["HLAbyte"] = self.encoder_factory.createHLAbyte
        #TODO: self.encoders["HLAfixedRecord"] = self.encoder_factory.createHLAfixedRecord
        #TODO: self.encoders["HLAvariableArray"] = self.encoder_factory.HLAvariableArray
        #TODO: self.encoders["HLAfixedArray"] = self.encoder_factory.createHLAfixedArray
        #TODO: self.encoders["HLAvariantRecord"] = self.encoder_factory.createHLAvariantRecord
        self.encoders["HLAfloat32BE"] = self.encoder_factory.createHLAfloat32BE
        self.encoders["HLAfloat32LE"] = self.encoder_factory.createHLAfloat32LE
        self.encoders["HLAfloat64BE"] = self.encoder_factory.createHLAfloat64BE
        self.encoders["HLAfloat64LE"] = self.encoder_factory.createHLAfloat64LE
        self.encoders["HLAinteger16BE"] = self.encoder_factory.createHLAinteger16BE
        self.encoders["HLAinteger16LE"] = self.encoder_factory.createHLAinteger16LE
        self.encoders["HLAinteger32BE"] = self.encoder_factory.createHLAinteger32BE
        self.encoders["HLAinteger32LE"] = self.encoder_factory.createHLAinteger32LE
        self.encoders["HLAinteger64BE"] = self.encoder_factory.createHLAinteger64BE
        self.encoders["HLAinteger64LE"] = self.encoder_factory.createHLAinteger64LE
        self.encoders["HLAoctet"] = self.encoder_factory.createHLAoctet
        self.encoders["HLAoctetPairBE"] = self.encoder_factory.createHLAoctetPairBE
        self.encoders["HLAoctetPairLE"] = self.encoder_factory.createHLAoctetPairLE
        self.encoders["HLAopaqueData"] = self.encoder_factory.createHLAopaqueData
        self.encoders["HLAunicodeChar"] = self.encoder_factory.createHLAunicodeChar
        self.encoders["HLAunicodeString"] = self.encoder_factory.createHLAunicodeString

        # TODO: this is a custom type, and should be passed as a parameter
        self.encoders["Position"] = PositionCoder

    def createObject(self, obj_string):
        return self.federate.createObject(obj_string)

    def producesObjectAttributes(self, obj_string, attributes=None):
        if not attributes:
            self.federate.producesObjectAttributes(obj_string, None)
        else:
            try:
                java_list = jarray.array(attributes, String)
                self.federate.producesObjectAttributes(obj_string, java_list)
            except:
                self.logger.error("[producesObject] could not convert list to String array.")

    def producesInteraction(self, int_string):
        self.federate.producesInteraction(int_string)

    def subscribesObject(self, obj_string, attributes=None):
        if not attributes:
            self.federate.subscribesObject(obj_string)
        else:
            try:
                java_list = jarray.array(attributes, String)
                self.federate.subscribesObjectAttributes(obj_string, java_list)
            except:
                self.logger.error("[subscribeObject] could not convert list to String array.")

    def subscribesInteraction(self, int_string, interaction_callback = None):
        self.federate.subscribesInteraction(int_string)
        self.int2callback[int_string] = interaction_callback

    # attributeValueMap: { <attribute_name> : <attribute_value> }
    def updateObject(self, obj_string, objinst_handle, attributeValueMap):
        try:
            if obj_string not in self.obj2attributes:
                self.logger.error("object %s not properly registered.", objinst_handle)
                return
            #attmap = self.federate.createAttributeHandleValueMap(len(attributeValueMap))
            for att_name in attributeValueMap.keys():
                if att_name not in self.obj2attributes[obj_string]:
                    self.logger.error("Unkown attribute name for object (%s.%s).", obj_string, att_name)
                    return
                att_type = self.obj2attributes[obj_string][att_name]
                attributeValueMap[att_name] = self.encoders[att_type](attributeValueMap[att_name]).toByteArray()
            self.federate.updateObjectAttributes(obj_string, objinst_handle, attributeValueMap)
        except:
            self.logger.error("Could not update attributes for unknown reason.")
            raise

    # interactionMap { <parameter_name> : <parameter_value> }
    # pvmap { <parameter_name> : <parameter_value> (byte encoded) }
    def sendInteraction(self, int_name, parameterMap):
        try:
            pvmap = {}
            for par_name in parameterMap:
                atttype = self.intpar2types[int_name][par_name]
                pvmap[par_name] = self.encoders[atttype](parameterMap[par_name]).toByteArray()
            self.federate.sendInteraction(int_name, pvmap)
        except:
            self.logger.error("could not send interaction %s", int_name)
            raise


    def start_timer(self):
        while (True):
            while not self.shutdown :
                stime = time.time()

                # Replace by Timer interaction in HLA
                # event = EventTypes.TimerEvent(CurrentIteration, stime)
                # self.EventRouter.RouterQueue.put(event)
                #self.federate.sendInteraction(?)

                etime = time.time()
                # TODO: determine when to stop
                if (etime - stime) < self.IntervalTime :
                    time.sleep(self.IntervalTime - (etime - stime))

class SimulationA(object):
    def __init__(self, settings):
        self.hlaconn = HLAConnector(settings, "FederateTest A")
        self.hlaconn.start()

class SimulationB(object):
    def __init__(self, settings):
        self.hlaconn = HLAConnector(settings, "FederateTest B")
        self.hlaconn.start()

#     def B_obj_callback(self, obj_instance, attribute_map):
#         print "### B obj callback"
#         print obj_instance
#         print attribute_map
#         print self.hlaconn.objatt2types
#         for key in attribute_map:
#             attname = self.hlaconn.federate.getAttributeName(obj_instance, key)
#             objname = self.hlaconn.federate.getObjectClassName(obj_instance)
#             atttype = self.hlaconn.objatt2types[objname][attname]
#             typeinst = self.hlaconn.encoders[atttype]()
#             typeinst.decode(attribute_map[key])
#             v = typeinst.getValue()
#             print self.hlaconn.federate.getAttributeName(obj_instance, key) + " = " + str(v)
#
#     def B_int_callback(self, int_handle, parameter_map):
#         print "### B int callback"
#         print int_handle
#         print parameter_map

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Testing logger...")
    settings = defaultdict(dict)
    settings["General"] = {}
    settings["General"]["TimeSteps"] = 0.2
    settings["HLA"]["FOM"] = "fom/UrbanSim.xml"
    settings["HLA"]["FederateType"] = "TestType"
    settings["HLA"]["FederationName"] = "TestFederation"
    from threading import Thread
    sima = SimulationA(settings)
    simb = SimulationB(settings)

    a = sima.hlaconn
    b = simb.hlaconn

    while(not a.ready() and not b.ready()):
        time.sleep(0.5)


    att_map = {}
    att_map["Position"] = "Position"
    att_map["Angle"] = "HLAfloat64BE"
    att_map["Velocity"] = "HLAfloat64BE"
    att_map["VehicleName"] = "HLAASCIIstring"
    att_map["VehicleType"] = "HLAASCIIstring"
    a.producesObjectAttributes("HLAobjectRoot.Vehicle", ["Position", "Angle", "Velocity", "VehicleName", "VehicleType"])
    a.registerAttributes("HLAobjectRoot.Vehicle", att_map)
    a.producesInteraction("HLAinteractionRoot.AddVehicle")
    class Vehicle:
        pass
    v = Vehicle()
    objinst_handle = a.createObject("HLAobjectRoot.Vehicle")
    time.sleep(1)

    b.subscribesObject("HLAobjectRoot.Vehicle", ["Velocity", "VehicleName", "Position"])
    b.subscribesInteraction("HLAinteractionRoot.AddVehicle")

    a.updateObject("HLAobjectRoot.Vehicle", objinst_handle, {"VehicleName" : "Banana", "Position" : Position(1.0, 1.0, 1.0), 'Velocity' : 25.5})

    addv = {}
    addv["VehicleName"] = "BananaCar"
    addv["VehicleType"] = "Fruit"
    addv["DestinationName"] = "Fridge"
    addv["Source"] = "Market"

    a.sendInteraction("HLAinteractionRoot.AddVehicle", addv)
    pass
    #JavaFederate.main([])