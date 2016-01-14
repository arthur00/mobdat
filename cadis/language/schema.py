'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import logging
from copy import deepcopy
import json
import threading
from uuid import UUID
logger = logging.getLogger(__name__)
LOG_HEADER = "[SCHEMA]"

# temporary solution!
PREFIX = "Frame.Simulation."

sets = set()
subsets = set()
# Dictionary of set -> subsets
subsetsof = {}
# Dictionary of subset -> set
setsof = {}
dimensions = {}

schema_data = threading.local()

def primarykey(func):
    prop = PrimaryProperty(func)
    return prop

def Set(cls):
    cls._dimensions = dimensions[cls]
    sets.add(cls)
    return cls

def SubSet(of):
    def wrapped(cls):
        subsets.add(cls)
        if of in subsets:
            subsetsof[of].append(cls)
        else:
            subsetsof[of] = [cls]
        setsof[cls] = of
        return cls
    return wrapped

def dimension(func):
    prop = Property(func)
    return prop

class MetaProperty(type):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, dict(namespace))
        result._dimension = True
        result._name = None
        return result

class MetaPropertyPrimaryKey(MetaProperty):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, dict(namespace))
        result._primarykey = True
        return result

class Property(property):
    __metaclass__ = MetaProperty
    def __init__(self, func, *args, **kwargs):
        if func:
            self._name = func.func_name
        #if prim:
        #    self._primarykey = True
        property.__init__(self, func, *args, **kwargs)

    def __set__(self, obj, value):
        if hasattr(schema_data, 'frame'):
            frame = schema_data.frame
            if frame.track_changes:
                if self._of in sets:
                    superset = self._of
                else:
                    superset = setsof[self._of]
                #Schema.frame.set_property(superset, obj, value, self._name)
                frame.set_property(superset, obj, value, self._name)
                #pushlist = Schema.frame.pushlist[superset]
                #if obj._primarykey not in pushlist:
                #    pushlist[superset][obj._primarykey] = {}

                # Save the property update
                #pushlist[superset][obj._primarykey][self._name] = value
        property.__set__(self, obj, value)

class PrimaryProperty(Property):
    __metaclass__ = MetaPropertyPrimaryKey

class MetaCADIS(type):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, dict(namespace))
        dimensions[result] = []
        for value in namespace.values():
            if hasattr(value, '_dimension'):
                dimensions[result].append(value)
                value._of = result
            if hasattr(value, '_primarykey'):
                result._primarykey = value
                dimensions[result].append(value)
                value._of = result
        #dimensions[result] = [
        #    value for value in namespace.values() if hasattr(value, '_dimension')]
        result._FULLNAME = PREFIX + name
        return result


class CADIS(object):
    __metaclass__ = MetaCADIS

    def __init__(self):
        pass

    #TODO: Implement deepcopy to copy only relevant properties

class CADISEncoder(json.JSONEncoder):
    def default(self, obj):
        obj_dict = {}
        if isinstance(obj, CADIS):
            for dim in obj._dimensions:
                prop = getattr(obj, dim._name)
                if hasattr(prop, "__json__"):
                    obj_dict[dim._name] = prop.__json__()
                else:
                    if isinstance(prop, UUID):
                        obj_dict[dim._name] = str(prop)
                    else:
                        obj_dict[dim._name] = prop
            return obj_dict
