#!/usr/bin/python
"""
Copyright (c) 2014, Intel Corporation

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.

* Neither the name of Intel Corporation nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@file    OpenSimBuilder.py
@author  Mic Bowman
@date    2013-12-03

This file defines the opensim builder class for mobdat traffic networks.
The functions in this file will rez a mobdat network in an OpenSim region.
"""

import os, sys
import logging
from mobdat.simulator.DataModel import Road, SimulationNode, BusinessNode,\
    ResidentialNode, Person, VehicleInfo, JobDescription, Capsule
from mobdat.common.ValueTypes import Vector3
from cadis.language.schema import CADISEncoder

# we need to import python modules from the $SUMO_HOME/tools directory
sys.path.append(os.path.join(os.environ.get("OPENSIM","/share/opensim"),"lib","python"))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "lib")))

from mobdat.common.graph.LayoutDecoration import EdgeMapDecoration
from mobdat.common.graph import Edge
from mobdat.common.Utilities import AuthByUserName, GenCoordinateMap,\
    CalculateOSCoordinates, CalculateOSCoordinatesFromOrigin
import json

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class PersonPicker :
    # -----------------------------------------------------------------
    def __init__(self, settings, world, laysettings) :
        self.Logger = logging.getLogger(__name__)

        self.World = world
        self.LayoutSettings = laysettings
        tnum =  settings["Experiment"]["NumberOfTravelers"]
        fpath = settings["Experiment"]["TravelerFilePath"]
        datafolder = settings["General"]["Data"]

        travelers = []
        count = 0
        for name,_ in self.World.IterNodes(nodetype = 'Person') :
            travelers.append(name)
            count += 1
            if count > tnum:
                break;

        f = open(os.path.join(datafolder,fpath), "w")
        f.write(json.dumps(travelers))
        f.close()
