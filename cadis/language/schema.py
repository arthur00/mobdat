'''
Created on Dec 14, 2015

@author: Arthur Valadares
'''

import logging
from copy import deepcopy, copy
import json
import threading
from uuid import UUID, uuid4
import sys
from json.encoder import JSONEncoder
from json.decoder import JSONDecoder
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
    def wrapped(func):
        prop = Property(func)
        setattr(prop, "_foreignkey", True)
        setattr(prop, "_relatedto", relatedto._in)
        setattr(prop, "_foreignprop", relatedto)
        setattr(relatedto,'_keyof', prop)
        return prop
    return wrapped

def primarykey(func):
    prop = Property(func)
    setattr(prop, "_primarykey", True)
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

class Property(property):
    def __init__(self, getter, setter=None, *args, **kwargs):
        if getter:
            #self._name = getter.func_name
            setattr(self, "_name", getter.func_name)
        setattr(self, "_dimension", True)
        property.__init__(self, getter, setter, *args, **kwargs)

    def setter(self, fset):
        prop = Property(self.fget, fset)
        for a in self.__dict__:
            setattr(prop, a, self.__dict__[a])
        return prop

    def __copy__(self):
        prop = Property(self.fget, self.fset)
        prop.__dict__.update(self.__dict__)
        return prop

    def __set__(self, obj, value):
        if hasattr(schema_data, 'frame'):
            frame = schema_data.frame
            if frame.track_changes:
                if not hasattr(self, "_primarykey"):
                    if self._of in sets or self._of in permutedclss:
                        superset = self._of
                    else:
                        superset = setsof[self._of]
                    frame.set_property(superset, obj, value, self._name)
                if hasattr(self, "_relatedto"):
                    fname = self._foreignprop._name
                    res = frame.findproperty(self._relatedto, fname, value)
                    if not res:
                        self.__Logger.error("could not match foreign key %s = %s to existing object of type %s", self._name, value, self._relatedto)
        property.__set__(self, obj, value)

    def __relatedto__(self, relatedto):
        self._relatedto = relatedto._of

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
                    setattr(value, '_of', result)
                    setattr(value, '_in', result)
                if hasattr(value, '_primarykey'):
                    setattr(result, '_primarykey', value)
                    dimensions[result].append(value)
                    setattr(value, '_of', result)
                    setattr(value, '_in', result)
                if hasattr(value, '_foreignkey'):
                    if not hasattr(result, "_foreignkeys"):
                        setattr(result, '_foreignkeys', {})
                    result._foreignkeys[value._name] = value._relatedto
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

class MetaPermutation(MetaCADIS):
    def __new__(cls, name, bases, namespace, **kwds):
        #result = type.__new__(cls, name, bases, dict(namespace))
        result = super(MetaPermutation, cls).__new__(cls, name, bases, namespace, **kwds)
        dimensions[result] = []
        if "__import_dimensions__" in namespace:
            result.__dimensiontable__ = {}
            for p in namespace["__import_dimensions__"]:
                prop = copy(p)
                result.__dimensiontable__[prop._name] = prop._of
                setattr(prop, '_in', result)
                # TODO: Allow a class to be passed here, meaning "import all properties from class"
                dimensions[result].append(prop)
                setattr(result, "_" + prop._name, prop.fget(prop._of))
                setattr(result, prop._name, prop)


        return result


class Permutation(CADIS):
    __metaclass__ = MetaPermutation

permutedtypes = {}
storagetypes = {}

def StorageObjectFactory(obj):
    # Build the storage object
    typestr = "__Storage__" + obj._FULLNAME
    if typestr not in storagetypes:
        newstclass = type(typestr, (CADIS,), {"__init__" :  CADIS.__init__})
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
        newpclass = type(typestr, (CADIS,), {"__init__" :  CADIS.__init__})
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

def PermutationObjectfactory(sobj, ret_obj = None):
    if not ret_obj:
        ret_obj = sobj._originalcls()
    for o in sobj.objectlinks.values():
        if hasattr(o, "objectlinks"):
            PermutationObjectfactory(o, ret_obj)
        else:
            for prop in o._dimensions:
                value = getattr(o, prop._name)
                setattr(ret_obj, prop._name, value)
            ret_obj.ID = o.ID
    return ret_obj

# class CADISDecoder(json.JSONEncoder):
#     def __init__(self):
#         JSONDecoder.__init__(self, object_hook=self.dict_to_object)
#
#     def dict_to_object(self, d):
#         return d

class CADISEncoder(json.JSONEncoder):
    def default(self, obj):
        #print "In CADIS Encoder!"
        if isinstance(obj, CADIS):
            try:
                obj_dict = {}
                for dim in obj._dimensions:
                    prop = getattr(obj, dim._name)
                    if hasattr(prop, "__json__"):
                        obj_dict[dim._name] = prop.__json__()
                    else:
                        if isinstance(prop, UUID):
                            obj_dict[dim._name] = str(prop)
                        else:
                            obj_dict[dim._name] = prop
                obj_dict["ID"] = obj.ID
                return obj_dict
            except:
                self.__Logger.debug("Could not convert encode object from Python -> JSON")
                raise
        elif isinstance(obj, UUID):
            return str(obj)
        elif hasattr(obj, "__json__"):
            return obj.__json__()
        else:
            return JSONEncoder.default(self, obj)
