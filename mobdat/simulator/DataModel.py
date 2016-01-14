'''
Created on Dec 26, 2015

@author: arthurvaladares
'''
import logging
from cadis.language.schema import dimension, Set, SubSet, CADIS, dimensions, sets, subsets, primarykey
from collections import namedtuple
from cadis.frame import Frame
import uuid

logger = logging.getLogger(__name__)
LOG_HEADER = "[DATAMODEL]"

#Vector3 = namedtuple("Vector3", ['X', 'Y', 'Z'])
class Vector3(object):
    X = 0
    Y = 0
    Z = 0

    def __init__(self, X, Y, Z):
        self.X = X
        self.Y = Y
        self.Z = Z

    def __json__(self):
        return self.__dict__

    def __str__(self):
        return self.__dict__.__str__()

    def __eq__(self, other):
        return (isinstance(other, Vector3) and (other.X == self.X and other.Y == self.Y and other.Z == self.Z))

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def __decode__(dic):
        return Vector3(dic['X'], dic['Y'], dic['Z'])

@Set
class Vehicle(CADIS):
    '''
    classdocs
    '''
    _ID = None
    @primarykey
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

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

    _Position = None
    @dimension
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    _Velocity = None
    @dimension
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    _Rotation = None
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
        return [c for c in Frame.Store.get(Vehicle) if c.Position != None]  # @UndefinedVariable
