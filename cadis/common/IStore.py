'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import abc

class IStore(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def insert(self, obj):
        return

    @abc.abstractmethod
    def get(self, typeObj):
        return

    @abc.abstractmethod
    def close(self):
        return

    @abc.abstractmethod
    def delete(self, typeObj, primkey):
        return
