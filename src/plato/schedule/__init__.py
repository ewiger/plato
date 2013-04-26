import os
from plato.findutils import find_files, Match
import json
import logging
from time import time
import json


status_set = [
    'submit',
    'pending',
    'run',
    'failed',
    'done', # success
]

class JobResult(object):

    def __init__(self, has_failed, output='', error='', details={}):
        self.has_failed = has_failed
        self.output = output
        self.error = error
        self.details = details

    @classmethod
    def from_json(cls, json_string):
        data = json.load(json_string)
        if not data:
            # Job has no result information
            return
        result = Result(
            data['has_failed'],
            data['output'],
            data['error'],
            data['details']
        )

    def as_json(self):
        return json.dumps(self.__dict__)

class Job(object):
    
    def __init__(self, id, batch_name, status='submit', is_attached=True, 
                 info={}, result=None):
        self.id = id
        self.status = status
        # Is attached to the monitor?
        self.is_attached = is_attached
        self.batch_name = batch_name
        # Scheduler specific info
        self.info = info
        # If we already know the result
        self.result = result 
    
    def __repr__(self):
        return '[%d] <%s> batch: %s |%s|' % (
                self.id,
                self.status, 
                self.batch_name,
                self.info
            )

    def get_report_filename(self):
        return 'plato_job_report.%s' % self.id


class Monitor(object):

    def __init__(self, state_path, job_prefix, is_interactive=False):
        self.state_path = state_path
        self.job_prefix = job_prefix
        self.is_interactive = is_interactive
 
    @property
    def logger(self):
        return self.scheduler.logger

    def init_db(self):
        '''Init persistence database'''
        for folder in status_set:
            db_pathname = os.path.join(self.state_path, folder)
            if not os.path.exists(db_pathname):
                if not self.is_interactive:
                    raise Exception('Missing persistence DB. Please initialize..')
                self.logger.info('Creating missing db folder: %s' % db_pathname)
                os.makedirs(db_pathname)

    def new_job(self, batch_name):
        # Get last issued job id.
        last_id = 0
        last_id_filepath = os.path.join(self.state_path, 'last_id')
        if os.path.exists(last_id_filepath):
            last_id = int(file(last_id_filepath).read().strip())
        # Increment id, save it.
        new_id = last_id + 1
        last_id_file = file(last_id_filepath, 'w+')
        last_id_file.write(str(new_id))
        last_id_file.close()
        # Create and return job template.
        return Job(new_id, batch_name, is_attached=False)

    def get_job_file(self, id='*'):
        return '%s_%s.st' % (self.job_prefix, id)
    
    def load_job(self, job_file):
        description = json.load(open(job_file))
        self.logger.debug(
            'Loading job with persistence monitor: %s', description)
        result = description.get('result', None)
        if result is not None:
            result = JobResult.from_json(result)
        return Job(description['id'], description['batch_name'],
                   description['status'], info=description['info'],
                   result=result) 

    def save_job(self, job_filename, job, info=None):
        self.logger.debug(
            'Saving job with persistence monitor into: %s', job_filename)
        description = {
            'id': job.id,
            'status': job.status,
            'batch_name': job.batch_name,
            'info': job.info,            
        }
        if job.result is not None:
            description['result'] = job.result.as_json()
        json.dump(description, open(job_filename, 'w+'))
    
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
            self.logger.warn('Job is already attached to the monitor.')
            return
        job_file = os.path.join(self.state_path, job.status, 
                                self.get_job_file(job.id))
        job.is_attached = True
        return self.save_job(job_file, job)


class JobRunner(object):
    
    def __init__(self, report_path=None):
        self.report_path = report_path 
        if report_path is None: 
            self.report_path = os.getcwd()
        self.__all_jobs = None

    @property
    def logger(self):
        return self.scheduler.logger

    @property
    def all_jobs(self):
        '''Jobs that run by the underlining scheduler's runner (cluster).'''
        if self.__all_jobs is None:
            self.__all_jobs = self.scheduler.runner.list_jobs('*')
        return self.__all_jobs

    def forget_job_list(self):
        self.__all_jobs = None

    def get_report_filepath(self, job):
        '''Return path to the report file of the job'''
        return os.path.join(self.report_path, job.get_report_filename())

    def report_file_exists(self, job):
        '''True if report file exists for the job'''
        return os.path.exists(self.get_report_filepath(job))

    def is_running(self, job):
        pass

    def execute(self, job):
        pass

    


class Scheduler(object):

    def __init__(self, runner, monitor, logger):
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger('Batch Job Scheduler')
        self.runner = runner
        self.runner.scheduler = self
        self.monitor = monitor
        self.monitor.scheduler = self
        self.monitor.init_db()
    
    def submit_job(self, job, resubmit=False):
        success = True
        # If we resubmit a failed job - we want to detach it.
        if resubmit:
            self.monitor.detach_job(job)
        job.status = 'submit'
        job.info['submitted_at'] = time()
        self.monitor.attach_job(job)
        return success
        
    def accept_job(self, job):
        if job.status != 'submit':
            self.logger.error('Will only accept jobs that were previously submitted.')
            return
        # This call can potentially block, but it should not.
        result= self.runner.execute(job)
        if result.has_failed:
            self.complete_job(job, result)
            return
        # Job was executed and is now pending
        self.monitor.detach_job(job)
        job.info.update(result.details)
        job.status = 'pending'
        job.info['pending_since'] = time()
        self.monitor.attach_job(job)

    def update_job(self, job):
        if not job.status in ('pending', 'run'):
            self.logger.error('Will only update jobs that are already pending.')
            return
        if self.runner.is_running(job):
            # Update status if necessary.
            if job.status == 'pending':
                self.logger.info('Job is now running: %s ' % job)
                self.monitor.detach_job(job)
                job.status = 'run'
                self.monitor.attach_job(job)
            else:
                self.logger.info('Job is still running: %s ' % job)
            return
        if self.runner.is_done(job) and self.runner.report_file_exists(job):
            self.logger.info('Job is (already) done: %s ' % job)
            # Job finished.
            result = self.runner.get_result(job)
            if not result:
                self.error('Failed to obtain a result for: %s' % job)
            self.complete_job(job, result)
            return
        self.logger.info('Job is still pending: %s' % job)
    
    def complete_job(self, job, result):
        if not job.status in ('submit', 'pending', 'run'):
            self.logger.error('We can only complete jobs that were submitted, pending or running.')
            return
        self.monitor.detach_job(job)
        job.result = result
        if not result or result.has_failed:
            job.status = 'failed'
        else:
            job.status = 'done'
        self.monitor.attach_job(job)
   
    def submitt_jobs(self):
        '''
        Make all freshly submitted plato jobs pending, i.e.
        Use actual low-level scheduler to submit them.
        '''
        submitted_jobs = self.monitor.load_jobs(['submit'])
        self.logger.info('Processing submitted jobs.. found (%d) jobs',
                         len(submitted_jobs))
        for job in submitted_jobs:
            # TODO: check what we can before submitting the job into the runner
            self.accept_job(job)

    def process_jobs(self):
        '''
        Check status of all pending jobs whether or not they are running.
        Change their status if necessary. Resolve jobs that has been complete or
        failed.
        '''
        pending_jobs = self.monitor.load_jobs(['pending', 'run'])
        self.logger.info('Processing pending and running jobs.. found (%d) jobs',
                         len(pending_jobs))
        for job in pending_jobs:
            self.update_job(job)

