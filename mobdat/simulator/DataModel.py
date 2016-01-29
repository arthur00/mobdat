'''
Created on Dec 26, 2015

@author: arthurvaladares
'''
import logging
from cadis.language.schema import dimension, Set, SubSet, CADIS, dimensions, sets, subsets, primarykey,\
    Permutation, dimensionof, foreignkey
from collections import namedtuple
from cadis.frame import Frame
import uuid
from mobdat.common.ValueTypes import Quaternion, Vector3

logger = logging.getLogger(__name__)
LOG_HEADER = "[DATAMODEL]"

@Set
class SimulationNode(CADIS):
    def __init__(self):
        self.ID = uuid.uuid4()

    _Center = Vector3(0,0,0)
    @dimension
    def Center(self):
        return self._Center

    @Center.setter
    def Center(self, value):
        self._Center = value

    _Angle = 0
    @dimension
    def Angle(self):
        return self._Angle

    @Angle.setter
    def Angle(self, value):
        self._Angle = value

    _Type = {}
    @dimension
    def Type(self):
        return self._Type

    @Type.setter
    def Type(self, value):
        self._Type = value

    _Name = None
    @dimension
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    _Width = 0
    @dimension
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

@Set
class Road(CADIS):
    def __init__(self):
        self.ID = uuid.uuid4()

    _StartPoint = Vector3(0,0,0)
    @dimension
    def StartingPoint(self):
        return self._StartPoint

    @StartingPoint.setter
    def StartingPoint(self, value):
        self._StartPoint = value

    _EndPoint = Vector3(0,0,0)
    @dimension
    def EndPoint(self):
        return self._EndPoint

    @EndPoint.setter
    def EndPoint(self, value):
        self._EndPoint = value

    _Width = 0
    @dimension
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    _Type = None
    @dimension
    def Type(self):
        return self._Type

    @Type.setter
    def Type(self, value):
        self._Type = value

@Permutation(SimulationNode)
class BusinessNode(CADIS):
    ### Properties from SimulationNode
    __import_dimensions__ = [ SimulationNode.Center, SimulationNode.Angle, SimulationNode.Type, SimulationNode.Name ]
    ### New properties
    _CustomersPerNode = 0
    @dimension
    def CustomersPerNode(self):
        return self._CustomersPerNode

    @CustomersPerNode.setter
    def CustomersPerNode(self, value):
        self._CustomersPerNode = value

    _EmployeesPerNode = 0
    @dimension
    def EmployeesPerNode(self):
        return self._EmployeesPerNode

    @EmployeesPerNode.setter
    def EmployeesPerNode(self, value):
        self._EmployeesPerNode = value

    _PreferredBusinessTypes = 0
    @dimension
    def PreferredBusinessTypes(self):
        return self._PreferredBusinessTypes

    @PreferredBusinessTypes.setter
    def PreferredBusinessTypes(self, value):
        self._PreferredBusinessTypes = value

    _PeakEmployeeCount = 0
    @dimension
    def PeakEmployeeCount(self):
        return self._PeakEmployeeCount

    @PeakEmployeeCount.setter
    def PeakEmployeeCount(self, value):
        self._PeakEmployeeCount = value

    _PeakCustomerCount = 0
    @dimension
    def PeakCustomerCount(self):
        return self._PeakCustomerCount

    @PeakCustomerCount.setter
    def PeakCustomerCount(self, value):
        self._PeakCustomerCount = value


@Permutation(SimulationNode)
class ResidentialNode(CADIS):
    ### Properties from SimulationNode
    __import_dimensions__ = [ SimulationNode.Center, SimulationNode.Angle, SimulationNode.Type, SimulationNode.Name ]

        ### New properties
    _ResidentsPerNode = 0
    @dimension
    def ResidentsPerNode(self):
        return self._ResidentsPerNode

    @ResidentsPerNode.setter
    def ResidentsPerNode(self, value):
        self._ResidentsPerNode = value

    _ResidentCount = 0
    @dimension
    def ResidentCount(self):
        return self._ResidentCount

    @ResidentCount.setter
    def ResidentCount(self, value):
        self._ResidentCount = value

    _ResidenceList = []
    @dimension
    def ResidenceList(self):
        return self._ResidenceList

    @ResidenceList.setter
    def ResidenceList(self, value):
        self._ResidenceList = value


class JobDescription(object):
    def __init__(self, salary = 0, flexhours = False, schedule = None):
        self.Salary = salary
        self.FlexibleHours = flexhours
        self.Schedule = schedule

    def __json__(self):
        return self.__dict__

class VehicleInfo(object):
    def __init__(self, vname = None, vtype = None):
        self.VehicleName = vname
        self.VehicleType = vtype

    def __json__(self):
        return self.__dict__


@Set
class PersonNode(CADIS):
    def __init__(self):
        self.ID = uuid.uuid4()

    _Name = None
    @dimension
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    _JobDescription = None
    @dimension
    def JobDescription(self):
        return self._JobDescription

    @JobDescription.setter
    def JobDescription(self, value):
        self._JobDescription = value

    _Preference = None
    @dimension
    def Preference(self):
        return self._Preference

    @Preference.setter
    def Preference(self, value):
        self._Preference = value

    _Vehicle = None
    @dimension
    def Vehicle(self):
        return self._Vehicle

    @Vehicle.setter
    def Vehicle(self, value):
        self._Vehicle = value

    _EmployedBy = None
    @foreignkey(BusinessNode.Name)
    @dimension
    def EmployedBy(self):
        return self._EmployedBy

    @EmployedBy.setter
    def EmployedBy(self, value):
        self._EmployedBy = value

    _LivesAt = None
    @foreignkey(ResidentialNode.Name)
    @dimension
    def LivesAt(self):
        return self._LivesAt

    @LivesAt.setter
    def LivesAt(self, value):
        self._LivesAt = value

@Set
class Vehicle(CADIS):
    '''
    classdocs
    '''
    _Name = None
    @dimension
    def Name(self):
        return self._Name

    @Name.setter
    def Name(self, value):
        self._Name = value

    _Type = None
    @dimension
    def Type(self):
        return self._Type

    @Type.setter
    def Type(self, value):
        self._Type = value

    _Route = None
    @dimension
    def Route(self):
        return self._Route

    @Route.setter
    def Route(self, value):
        self._Route = value

    _Target = None
    @dimension
    def Target(self):
        return self._Target

    @Target.setter
    def Target(self, value):
        self._Target = value

    _Position = Vector3(0,0,0)
    @dimension
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    _Velocity = Vector3(0,0,0)
    @dimension
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    _Rotation = Quaternion(0,0,0,0)
    @dimension
    def Rotation(self):
        return self._Rotation

    @Rotation.setter
    def Rotation(self, value):
        self._Rotation = value

    def __init__(self):
        self.ID = uuid.uuid4()

@SubSet(Vehicle)
class MovingVehicle(Vehicle):
    @staticmethod
    def query():
        return [c for c in Frame.Store.get(Vehicle) if c.Position != None or c.Position == (0,0,0)]  # @UndefinedVariable

if __name__ == "__main__":
    test = ResidentialNode()
    test2 = BusinessNode()