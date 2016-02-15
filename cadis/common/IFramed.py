'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import abc

def Producer(*args):
    def wrap(cls):
        cls._producer = set()
        for t in args:
            cls._producer.add(t)
        return cls
    return wrap

def GetterSetter(*args):
    def wrap(cls):
        cls._gettersetter = set()
        for t in args:
            cls._gettersetter.add(t)
        return cls
    return wrap

def Getter(*args):
    def wrap(cls):
        cls._getter = set()
        for t in args:
            cls._getter.add(t)
        return cls
    return wrap

class IFramed(object):
    __metaclass__ = abc.ABCMeta
    _producer = set()
    _getter = set()
    _gettersetter = set()

    @abc.abstractmethod
    def initialize(self):
        return

    @abc.abstractmethod
    def update(self):
        return

    @abc.abstractmethod
    def shutdown(self):
        return
