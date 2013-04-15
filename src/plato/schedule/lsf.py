'''
Implementation of plato scheduler for Platform LSF 
'''

import logging 
from plato.schedule import (Monitor, Scheduler, JobRunner)


class LsfRunner(JobRunner):
    pass


class LsfMonitor(Monitor):
    pass


class LsfScheduler(Scheduler):
    
    def __init__(self, runner, monitor, logger):
        runner = LsfRunner()
        monitor = LsfMonitor()
        logger = logging.getLogger('Lsf Job Scheduler') 
        Scheduler.__init__(runner, monitor, logger)
