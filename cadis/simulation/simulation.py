'''
Created on Dec 17, 2015

@author: Arthur Valadares
'''

import logging
import logging.handlers
import os
from cadis.frame import Frame
from cadis.simulation.trafficsim import TrafficSimulation
from cadis.simulation.pedestriansim import PedestrianSimulation
from time import sleep
from cadis.store.remotestore import RemoteStore

logger = None

class Simulation(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        frame_car = Frame(RemoteStore())
        frame_car.interval = 1.0
        frame_car.attach(TrafficSimulation(frame_car))
        
        frame_ped = Frame(frame_car.Store)
        frame_ped.interval = 1.0
        frame_ped.attach(PedestrianSimulation(frame_ped))
        
        frame_car.go()
        frame_ped.go()

def SetupLoggers():
    global logger
    logger = logging.getLogger()
    logging.info("testing before")
    logger.setLevel(logging.DEBUG)

    #logfile = os.path.join(os.path.dirname(__file__), "../../logs/CADIS.log")
    #flog = logging.handlers.RotatingFileHandler(logfile, maxBytes=10*1024*1024, backupCount=50, mode='w')
    #flog.setFormatter(logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
    #logger.addHandler(flog)

    clog = logging.StreamHandler()
    clog.addFilter(logging.Filter(name='CADIS'))
    clog.setFormatter(logging.Formatter('[%(name)s] %(message)s'))
    clog.setLevel(logging.DEBUG)
    logger.addHandler(clog)

if __name__== "__main__":
    SetupLoggers()
    sim = Simulation()
    while True:
        sleep(1)
