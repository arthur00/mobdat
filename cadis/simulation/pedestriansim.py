'''
Created on Jan 11, 2015

@author: Ian Carvalho
'''

import logging
from cadis.simulation.DataModel import Pedestrian, StoppedPedestrian, Walker, PedestrianInDanger
from cadis.common.IFramed import IFramed, Producer, GetterSetter

logger = logging.getLogger(__name__)
LOG_HEADER = "[PEDESTRIANS]"


@Producer(Pedestrian)
@GetterSetter(StoppedPedestrian, Walker, PedestrianInDanger)
class PedestrianSimulation(IFramed):
    '''
    classdocs
    '''

    frame = None
    ticks = 0
    TICKS_BETWEEN_PEDESTRIANS = 5
    pedestrians = []
    def __init__(self, frame):
        '''
        Constructor
        '''
        self.frame = frame

    def initialize(self):
        logger.debug("%s Initializing", LOG_HEADER)
        for i in xrange(1):
            self.frame.add(Pedestrian())

        self.pedestrians = self.frame.get(Pedestrian)

    def update(self):
        logger.info("%s Tick", LOG_HEADER)
        if self.ticks % self.TICKS_BETWEEN_PEDESTRIANS == 0:
            try:
                pass

            except Exception:
                logger.exception("Error: ")

        endangereds = self.frame.get(PedestrianInDanger)
        logger.debug("%s ************** PedestrianInDanger: %s", LOG_HEADER, len(endangereds))
        for pedestrian in endangereds:
            pedestrian.move()
        self.ticks += 1
