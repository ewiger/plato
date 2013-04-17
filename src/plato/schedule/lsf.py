'''
Implementation of plato scheduler for Platform LSF 
'''
import re
import logging
from StringIO import StringIO
from plato import getBasicLogger 
from plato.schedule import (Monitor, Scheduler, JobRunner, JobResult)

from sh import bjobs, bsub, bkill


logger = getBasicLogger('lsf', logging.DEBUG)

#JOBID      USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
#19460073   yyauhen DONE  pub.1h     brutus4     a3241       *t.xml";fi Apr 17 14:54
job_expr = re.compile(r'(\d*)\s+(\w*)\s+(\w*)\s+(\w*)\s+(\w*)\s+(\w*)\s+(\w*)\s+(\S*)')
submit_expr = re.compile(
    r'Job <(\d*)> is submitted to queue <([^>]*)>', re.MULTILINE)

class LsfRunner(JobRunner):
    
    def is_running(self, job):
        lsf_id = job.info['lsf_id']
        if not lsf_id in self.all_jobs:
            return False
        found_job = self.all_jobs[lsf_id]
        return found_job.info['lsf_status'] == 'RUN'

    def execute(self, job):
        result = JobResult(has_failed=True, details=job.info.copy())
        stderr = StringIO()
        if job.info['queue'] == 'default':
            result.output = bsub(job.info['command'], _err=stderr).strip()
            result.error = stderr.getvalue().strip()
            self.logger.debug('bsub output: %s' % result.output)
            self.logger.debug('bsub error: %s' % result.error)
            match = submit_expr.search(result.output)
            if match:
                result.has_failed = False
                result.details['lsf_id'] = match.group(1)
                result.details['lsf_queue'] = match.group(2)
        else:
            result.error = 'Job info has unknown queue.'
            self.logger.warn(result.error)
        return result

    def list_jobs(self, mask=[]):
        result = dict()
        stderr = StringIO()
        if '*' in mask or 'all' in mask:
            output = bjobs('-a', _err=stderr)
        else:
            output = bjobs(_err=stderr)
        error_message = stderr.getvalue().strip()
        logger.debug(error_message)
        if re.search(r'No (?:\w* )job found', error_message):
            return result
        # Parse job info, packed it into dictionary
        print output
    


class LsfMonitor(Monitor):
    pass    


class LsfScheduler(Scheduler):
    
    def __init__(self, runner, monitor):
        Scheduler.__init__(self, runner, monitor, logger)
    
