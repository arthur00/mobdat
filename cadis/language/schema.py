'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import logging
from copy import deepcopy, copy
import json
import threading
from uuid import UUID
import sys
logger = logging.getLogger(__name__)
LOG_HEADER = "[SCHEMA]"

# temporary solution!
PREFIX = "Frame.Simulation."

sets = set()
subsets = set()
permutationsets = set()
# Dictionary of set -> subsets
subsetsof = {}
# Dictionary of subset -> set
setsof = {}
dimensions = {}

# Class permuted from -> permuted class
permutations = {}

# Permuted class -> class permuted from
permutedclss = {}

schema_data = threading.local()

def foreignkey(relatedto):
    def wrapped(prop):
        prop._relatedto = relatedto
        return prop
    return wrapped

def primarykey(func):
    prop = PrimaryProperty(func)
    return prop

def PermutedSet(cls):
    cls._dimensions = dimensions[cls]
    permutedclss[cls] = []
    for of in cls.__dimensiontable__.values():
        if of in permutations:
            permutations[of].append(cls)
        else:
            permutations[of] = [cls]
        permutedclss[cls].append(of)
        permutationsets.add(cls)
    return cls

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
        cls._subset = True
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
            if frame.track_changes and not hasattr(self, "_primarykey"):
                if self._of in sets or self._of in permutedclss:
                    superset = self._of
                else:
                    superset = setsof[self._of]
                frame.set_property(superset, obj, value, self._name)
        property.__set__(self, obj, value)

class PrimaryProperty(Property):
    __metaclass__ = MetaPropertyPrimaryKey

class MetaCADIS(type):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, dict(namespace))
        result._FULLNAME = PREFIX + name
        if name.startswith("__Storage__") or name.startswith("__Permutation__"):
            return result
        else:
            dimensions[result] = []

            for value in namespace.values():
                if hasattr(value, '_dimension'):
                    dimensions[result].append(value)
                    value._of = result
                if hasattr(value, '_primarykey'):
                    result._primarykey = value
                    dimensions[result].append(value)
                    value._of = result
        return result


class CADIS(object):
    __metaclass__ = MetaCADIS

    def __init__(self):
        pass

    _ID = None
    @primarykey
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value


class MetaPermutation(type):
    def __new__(cls, name, bases, namespace, **kwds):
        result = type.__new__(cls, name, bases, dict(namespace))
        dimensions[result] = []
        if "__import_dimensions__" in namespace:
            result.__dimensiontable__ = {}
            for prop in namespace["__import_dimensions__"]:
                result.__dimensiontable__[prop._name] = prop._of
                # TODO: Allow a class to be passed here, meaning "import all properties from class"
                dimensions[result].append(prop)
                setattr(result, "_" + prop._name, prop.fget(prop._of))
                setattr(result, prop._name, prop)

            #result._dimensions = dimensions[cls]
        for value in namespace.values():
            if hasattr(value, '_dimension'):
                dimensions[result].append(value)
                value._of = result
            if hasattr(value, '_primarykey'):
                result._primarykey = value
                dimensions[result].append(value)
                value._of = result
        result._FULLNAME = PREFIX + name
        return result


class Permutation(object):
    __metaclass__ = MetaPermutation

    def __init__(self):
        pass

    _ID = None
    @primarykey
    def ID(self):
        return self._ID

    @ID.setter
    def ID(self, value):
        self._ID = value

permutedtypes = {}
storagetypes = {}

#===============================================================================
# def PermutedObjectFactory(obj):
#     typestr = "__Permuted__" + obj._FULLNAME
#     if typestr not in permutedtypes:
#         newclass = type("__Permuted__" + obj._FULLNAME, (CADIS,), {"__init__" :  CADIS.__init__})
#         permutedtypes[typestr] = newclass
#     else:
#         newclass = permutedtypes[typestr]
#     newclass.__dimensiontable__ = copy(obj.__dimensiontable__)
#     newobj = newclass()
#     newobj.ID = obj.ID
#     for prop in dimensions[obj.__class__]:
#         if prop._of == obj.__class__:
#             setattr(newobj, prop._name, getattr(obj, prop._name))
#     return newobj
#===============================================================================

def StorageObjectFactory(obj):
    # Build the storage object
    typestr = "__Storage__" + obj._FULLNAME
    if typestr not in storagetypes:
        newstclass = type("__Storage__" + obj._FULLNAME, (CADIS,), {"__init__" :  CADIS.__init__})
        newstclass.__dimensiontable__ = copy(obj.__dimensiontable__)
        storagetypes[typestr] = newstclass
    else:
        newstclass = storagetypes[typestr]
    stoobj = newstclass()
    stoobj.ID = obj.ID
    stoobj._storageobj = True
    stoobj._originalcls = obj.__class__
    stoobj.objectlinks = {}

    #Build the permuted version of the object
    typestr = "__Permuted__" + obj._FULLNAME
    if typestr not in permutedtypes:
        newpclass = type("__Permuted__" + obj._FULLNAME, (CADIS,), {"__init__" :  CADIS.__init__})
        permutedtypes[typestr] = newpclass
    else:
        newpclass = permutedtypes[typestr]
    newobj = newpclass()
    newobj._dimensions = set()
    newobj.ID = obj.ID
    for prop in dimensions[obj.__class__]:
        if prop._of == obj.__class__:
            setattr(newobj, prop._name, getattr(obj, prop._name))
            newobj._dimensions.add(prop)

    stoobj.objectlinks[obj.__class__] = newobj
    return stoobj

def PermutationObjectfactory(sobj):
    ret_obj = sobj._originalcls()
    for o in sobj.objectlinks.values():
        for prop in o._dimensions:
            value = getattr(o, prop._name)
            setattr(ret_obj, prop._name, value)
    return ret_obj

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