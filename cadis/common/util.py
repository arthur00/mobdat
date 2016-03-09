#!/usr/bin/python
'''
Created on Mar 7, 2016

@author: arthur
'''
from functools import wraps
import time
import csv
import os
import platform
import sys

sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
from cadis.common.IFramed import IFramed

INSTRUMENT = True
INSTRUMENT_HEADERS = {}

def instrument_average(f):
    if not INSTRUMENT:
        return f
    else:
        if not f.__module__ in INSTRUMENT_HEADERS:
            INSTRUMENT_HEADERS[f.__module__] = []
        INSTRUMENT_HEADERS[f.__module__].append(f.func_name)
        @wraps(f)
        def instrument(*args, **kwds):
            obj = args[0]
            if isinstance(obj, IFramed):
                obj = obj.frame
            start = time.time()
            ret = f(*args, **kwds)
            end = time.time()
            if obj._instruments[f.__name__] == "":
                obj._instruments[f.__name__] = []
            obj._instruments[f.__name__].append((end - start) * 1000)
            return ret
        return instrument

def instrument(f):
    if not INSTRUMENT:
        return f
    else:
        if not f.__module__ in INSTRUMENT_HEADERS:
            INSTRUMENT_HEADERS[f.__module__] = []
        INSTRUMENT_HEADERS[f.__module__].append(f.func_name)
        @wraps(f)
        def instrument(*args, **kwds):
            obj = args[0]
            if isinstance(obj, IFramed):
                obj = obj.frame
            start = time.time()
            ret = f(*args, **kwds)
            end = time.time()
            if obj._instruments[f.__name__] == "":
                obj._instruments[f.__name__] = 0
            obj._instruments[f.__name__] += (end - start) * 1000
            return ret
        return instrument

class Instrument(object):
    def __init__(self, fname, headers=None, options=None):
        self.instruments = {}
        self.iteration = 0
        strtime = time.strftime("%Y-%m-%d_%H-%M-%S")
        if not os.path.exists('stats'):
            os.mkdir('stats')
        self.ifname = os.path.join('stats', "%s_%s.csv" % (strtime,fname))
        if platform.system() != "Windows":
            linkname = os.path.join('stats', "latest_%s" % fname)
            if os.path.exists(linkname):
                os.remove(linkname)
            os.symlink(os.path.abspath(self.ifname), linkname) # @UndefinedVariable only in Linux!
        with open(self.ifname, 'w', 0) as csvfile:
            if options:
                csvfile.write("########\n")
                csvfile.write("Options, %s\n" % ','.join(["%s:%s" % (k,v) for k,v in options.values()]))
                csvfile.write("########\n\n")
            self.headers = ['iteration']
            if headers:
                self.headers.extend(headers)
            self.headers.extend(INSTRUMENT_HEADERS.values())
            self.fieldnames = self.headers
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
            writer.writeheader()

    def clean(self):
        for k in self.instruments:
            self.instruments[k] = ""

    def add_instruments(self, instruments):
        self.instruments.update(instruments)

    def measure_call(self, f, header, *args, **kwargs):
        start = time.time()
        f(*args, **kwargs)
        end = time.time()
        self.instruments[header] = (end - start) * 1000

    def dump_stats(self):
        with open(self.ifname, 'a', 0) as csvfile:
            writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=self.fieldnames)
            #d['vehicles'] = self.frame.count(self.frame.name2class("Vehicle"))
            self.instruments['iteration'] = self.iteration
            for h in self.instruments:
                # if averaging, calculate it
                if isinstance(self.instruments[h], list):
                    if len(self.instruments[h]) == 0:
                        self.instruments[h] = ""
                    else:
                        self.instruments[h] = float(sum(self.instruments[h]))/len(self.instruments[h])
            writer.writerow(self.instruments)
            self.clean()
            self.iteration += 1
