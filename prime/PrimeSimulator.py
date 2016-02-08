'''
Created on Jan 27, 2016

@author: Arthur Valadares
'''
from cadis.common.IFramed import IFramed, Producer, GetterSetter
from mobdat.simulator.DataModel import BusinessNode, SimulationNode,\
    ResidentialNode, Vehicle, Person
from prime.PrimeDataModel import PrimeNode, EmptyBusiness
import logging
from mobdat.common.ValueTypes import Vector3
import random

@Producer(Vehicle, BusinessNode, SimulationNode, PrimeNode)
@GetterSetter(Vehicle, Person, BusinessNode, ResidentialNode, SimulationNode, PrimeNode, EmptyBusiness)
class PrimeSimulator(IFramed):
    def __init__(self, settings, world, netsettings, cname, frame) :
        self.frame = frame
        self.world = world
        self.__Logger = logging.getLogger(__name__)
        self.schedule_deliveries = {}
        pass

    def initialize(self):
        self.mybusiness = None
        self.__Logger.warn('PrimeSimulator initialization complete')

    def update(self):
        self.CurrentStep = self.frame.step
        if not self.mybusiness:
            a = self.frame.get(EmptyBusiness)
            if len(a) > 0:
                pn = PrimeNode()
                pn.ID = a[0].ID
                pn.Name = "Amazon"
                pn.PeakCustomerCount = 0
                pn.Rezcap = a[0].Rezcap
                self.frame.add(pn)
                self.mybusiness = pn
                self.frame.disable_subset(EmptyBusiness)

        if self.mybusiness and not self.mybusiness.Customers:
            ppl = self.frame.get(Person)
            if len(ppl) > 40:
                customers = random.sample(ppl, 10)
                # Synchronized attribute
                self.mybusiness.Customers = [p.Name for p in customers]
                self.mybusiness.CustomerObjects = {}
                # Non-synchronized attribute
                for c in customers:
                    self.mybusiness.CustomerObjects[c.Name] = c
                    starttime = random.randint(50, 100)
                    if starttime not in self.schedule_deliveries:
                        self.schedule_deliveries[starttime] = []
                    self.schedule_deliveries[starttime].append(c)
                self.__Logger.info("Delivery schedule: %s", self.schedule_deliveries.keys())

        if self.CurrentStep in self.schedule_deliveries:
            for c in self.schedule_deliveries[self.CurrentStep]:
                if hasattr(c.LivesAt, "Rezcap"):
                    v = Vehicle()
                    v.Name = "AmazonTo%s" % c.Name
                    v.Type = c.Vehicle.VehicleType
                    v.Route = self.mybusiness.Rezcap.DestinationName
                    v.Target = c.LivesAt.Rezcap.SourceName
                    self.frame.add(v)
                    self.__Logger.info("starting amazon delivery to %s", c.Name)
                else:
                    print c.LivesAt



                #ppl = self.frame.get(Person)
                #business = self.frame.get(PrimeNode, self.mybusiness)
                #business.PeakCustomerCount += 1
                #business.Center = Vector3(self.CurrentStep, 0, 0)
                #business.Customers.append("worker%s" % self.CurrentStep)
