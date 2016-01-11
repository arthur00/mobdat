'''
Created on Dec 15, 2015

@author: arthurvaladares
'''

import logging
from cadis.language.schema import dimension, Set, SubSet, CADIS, dimensions, sets, subsets, primarykey
from collections import namedtuple
from cadis.frame import Frame


logger = logging.getLogger(__name__)
LOG_HEADER = "[DATAMODEL]"
class Color:
    Red = 0
    Green = 1
    Blue = 2
    Yellow = 3
    Black = 4
    White = 5
    Grey = 6

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
class Car(CADIS):
    '''
    classdocs
    '''
    FINAL_POSITION = 500;
    SPEED = 40;

    _ID = 0
    @primarykey
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

    _Position = Vector3(0, 0, 0)
    @dimension
    def Position(self):
        return self._Position

    @Position.setter
    def Position(self, value):
        self._Position = value

    _Velocity = Vector3(0, 0, 0)
    @dimension
    def Velocity(self):
        return self._Velocity

    @Velocity.setter
    def Velocity(self, value):
        self._Velocity = value

    _Color = Color.White
    @dimension
    def Color(self):
        return self._Color

    @Color.setter
    def Color(self, value):
        self._Color = value

    _Length = 0
    @dimension
    def Length(self):
        return self._Length

    @Length.setter
    def Length(self, value):
        self._Length = value

    _Width = 0
    @dimension
    def Width(self):
        return self._Width

    @Width.setter
    def Width(self, value):
        self._Width = value

    def __init__(self, uid):
        self.ID = uid
        self.Length = 30;

@SubSet(Car)
class InactiveCar(Car):
    @staticmethod
    def query():
        return [c for c in Frame.Store.get(Car) if c.Position == Vector3(0,0,0)]  # @UndefinedVariable

    def start(self):
        logger.debug("[InactiveCar]: {0} starting".format(self.ID))
        self.Velocity = Vector3(self.SPEED, 0, 0)

@SubSet(Car)
class ActiveCar(Car):
    @staticmethod
    def query():  # @DontTrace
        return [c for c in Frame.Store.get(Car) if c.Velocity != Vector3(0,0,0)]  # @UndefinedVariable
    def move(self):
        self.Position = Vector3(self.Position.X + self.Velocity.X, self.Position.Y + self.Velocity.Y, self.Position.Z + self.Velocity.Z)
        logger.debug("[ActiveCar]: {0} New position {1}".format(self.ID, self.Position));

        # End of ride
        if (self.Position.X >= self.FINAL_POSITION or self.Position.Y >= self.FINAL_POSITION):
            self.stop();

    def stop(self):
        logger.debug("[ActiveCar]: {0} stopping".format(self.ID));
        self.Position.X = 0;
        self.Position.Y = 0;
        self.Position.Z = 0;
        self.Velocity.X = 0;
        self.Velocity.Y = 0;
        self.Velocity.Z = 0;


@Set
class Pedestrian(CADIS):
    INITIAL_POSITION = 650;
    SPEED = 10;

    #TODO: PrimaryKey
    @primarykey
    def ID(self):
        return self._ID

    _X = 0
    @dimension
    def X(self):
        return self._X

    @X.setter
    def X(self, value):
        self._X = value

    _Y = 0
    @dimension
    def Y(self):
        return self._Y

    @Y.setter
    def Y(self, value):
        self._Y = value

    def __init__(self, i=None):
        if i:
            self.ID = i
        self.X = self.INITIAL_POSITION;
        self.Y = 0;

    def move(self):
        self.X -= self.SPEED;

        logger.debug("[Pedestrian]: {0} New position <{1}, {2}>".format(self.ID, self.X, self.Y));

        # End of ride
        if self.X <= 0:
            self.stop();


    def stop(self):
        logger.debug("[Pedestrian]: {0} stopping".format(self.ID));
        self.X = self.INITIAL_POSITION;
        self.Y = 0;

    def setposition(self, x):
        self.X = x;


@SubSet(Pedestrian)
class StoppedPedestrian(Pedestrian):
    @classmethod
    def query():
        return [p for p in Frame.Store.get(Pedestrian) if p.X == Pedestrian.INITIAL_POSITION]  # @UndefinedVariable
    """() =>
        from p in Frame.Store.Get<Pedestrian>()
        where p.X.Equals(INITIAL_POSITION)
        select p;
    """


@SubSet(Pedestrian)
class Walker(Pedestrian):
    @classmethod
    def query():
        return [p for p in Frame.Store.get(Pedestrian) if p.X != Pedestrian.INITIAL_POSITION]  # @UndefinedVariable

    """() =>
        from p in Frame.Store.Get<Pedestrian>()
        where !p.X.Equals(INITIAL_POSITION)
        select p;
    """


@SubSet(Pedestrian)
class PedestrianInDanger(Pedestrian):
    def distance(self, p1, p2):
        return abs(self.p1.X - self.p2.X);
        #return Math.Sqrt(Math.Pow(Math.Abs(p1.X -p2.X), 2) +
        #    Math.Pow(Math.Abs(p1.Y -p2.Y), 2));
    @classmethod
    def query():
        result = []
        for p in Frame.Store.get(Pedestrian):  # @UndefinedVariable
            for c in Frame.Store.get(Car):  # @UndefinedVariable
                if abs(c.Position.X - p.X) < 130 and c.Position.Y == p.Y:
                    result.append(p)
        return result

    """() =>
        from p in Frame.Store.Get<Pedestrian>()
        let cpos = from c in Frame.Store.Get<Vehicle>()
                   select c.Position
        where cpos != null &&
        !cpos.FirstOrDefault(pos => pos.Y == p.Y && Math.Abs(pos.X - p.X) < 130).Equals(Vector3.Zero)
        select p;
    """

    def move(self):
        logger.debug("[Pedestrian]: {0} avoiding collision!".format(self.ID));
        self.Y += 50;

if __name__=="__main__":
    car = Car(10)
    print car.Color
    car.Color = Color.Red
    print dimensions
    print sets
    print subsets
    print car.Color
    print car.ID
