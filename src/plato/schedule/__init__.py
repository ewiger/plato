import os
from plato.findutils import find_files, Match
import json
import logging


status_set = [
    'submit',
    'pending',
    'run',
    'failed',
    'done', # success
]


class Job(object):
    
    def __init__(self, id, batch_name, status='submit', is_attached=True):
        self.id = id
        self.status = status
        # is attached to the monitor
        self.is_attached = is_attached
        self.batch_name = batch_name
    

class Monitor(object):
    
    def __init__(self, state_path, job_prefix):
        self.state_path = state_path
        self.job_prefix = job_prefix
   
    def get_job_file(self, id='*'):
        return '%s_%s.st' % (self.job_prefix, id)
    
    def load_job(self, job_file):
        description = json.load(open(job_file))
        return Job(description.id, description.batch_name, 
                   description.status, description.result)

    def save_job(self, job_file, job, result=None):
        description = {
            'id': job.id,
            'status': job.status,
            'batch_name': job.batch_name,
            'result': result,
        }
        fhandle = open(job_file, 'w+')
        fhandle.write(json.dumps(description))
        fhandle.close()
    
    def load_jobs(self, status_mask=status_set, ):
        jobs = list()
        for status in status_mask:
            status_folder = os.path.join(self.state_path, status)
            job_file_mask = self.get_job_file()
            job_files = find_files(
                status_folder, Match(filetype='f', name=job_file_mask))
            for job_file in job_files:
                job = self.load_job(job_file)
                if not job:
                    continue
                jobs.append(job)
        return jobs
    
    def detach_job(self, job):
        '''Make job invisible for the monitor'''
        if not job.is_attached:
            # Job is already detached from the monitor.
            return
        job_file = os.path.join(self.state_path, job.status, 
                                self.get_job_file(job.id))
        os.remove(job_file)
        job.is_attached = False

    def attach_job(self, job):
        if job.is_attached:
            # Job is already attached to the monitor.
            return
        job_file = os.path.join(self.state_path, job.status, 
                                self.get_job_file(job.id))
        self.save_job(job_file, job)
        job.is_attached = True


class JobRunner(object):
    
    def execute(self, job):
        pass


class Scheduler(object):

    def __init__(self, runner, monitor, logger):
        self.runner = runner
        self.monitor = monitor        
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger('Batch Job Scheduler')
    
    def submit_job(self, job):
        # If we resubmit a failed job - we want to detach it.
        self.monitor.detach_job(job)
        job.status = 'submit'
        self.monitor.attach_job(job)
        
    def accept_job(self, job):
        if job.status != 'submit':
            self.logger.warn('Will only accept job that was previously submitted.')
        self.monitor.detach_job(job)
        job.status = 'pending'
        self.monitor.attach_job(job)

    def run_job(self, job):
        if job.status != 'submit':
            self.logger.warn('Will only run job that was previously pending.')
        self.monitor.detach_job(job)
        job.status = 'run'        
        self.monitor.attach_job(job)
        # This call can potentially block, but it should not.
        self.runner.execute(job)
        
    def complete_job(self, job, result):
        if job.status != 'run':
            self.logger.warn('Will only complete job that was previously running.')
        self.monitor.detach_job(job)
        if result.has_failed:
            job.status = 'failed'
        else:
            job.status = 'done'
        self.monitor.attach_job(job)

    def run_pending_jobs(self):
        pending_jobs = self.monitor.load_jobs(['pending'])
        
    def check_on_running_jobs(self):
        running_jobs = self.monitor.load_jobs(['run'])
        for job in running_jobs:
            if self.runner.is_running(job):
                continue
            # Job finished.
            result = self.runner.get_result(job)
            self.complete_job(job, result)
          