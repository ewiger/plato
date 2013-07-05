import os
from plato.shell.findutils import find_files, Match
import json
import logging
from time import time
import json
from ConfigParser import ConfigParser
from importlib import import_module


logger = logging.getLogger(__name__)


class PlatoException(Exception):
    '''Base class for plato exceptions'''


class NoSchedulerFound(PlatoException):   
    '''Failed to find scheduler'''


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
        data = json.loads(json_string)
        if not data:
            # Job has no result information
            return
        return JobResult(
            data['has_failed'],
            data['output'],
            data['error'],
            data['details']
        )

    def as_json(self):
        return json.dumps(self.__dict__)


class Job(object):
    
    def __init__(self, id, batch_name, status='submit', is_attached=True, 
                 info={}, result=None, file_attachments={}):
        self.id = id
        self.status = status
        # Is attached to the monitor?
        self.is_attached = is_attached
        self.batch_name = batch_name
        # Scheduler specific info
        self.info = info
        # If we already know the result
        self.result = result
        self.file_attachments = file_attachments 
    
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
        self.scheduler = None  # Set it after initialization
        self.state_path = state_path
        self.job_prefix = job_prefix
        self.is_interactive = is_interactive
 
    @property
    def logger(self):
        return self.scheduler.logger

    def init_db(self):
        '''Init persistence database'''
        folders = ['pidfiles', 'attachments', 'reports']
        folders.extend(status_set)
        for folder in folders:
            db_pathname = os.path.join(self.state_path, folder)
            if not os.path.exists(db_pathname):
                if not self.is_interactive:
                    raise Exception('Missing persistence DB. Please initialize..')
                logger.info('Creating missing db folder: %s' % db_pathname)
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
        logger.debug(
            'Loading job with persistence monitor: %s', description)
        result = description.get('result', None)        
        if result is not None:
            result = JobResult.from_json(result)                                 
        return Job(description['id'], description['batch_name'],
                   description['status'], info=description['info'],
                   result=result, file_attachments=description['file_attachments']) 

    def save_job(self, job_filename, job, info=None):
        logger.debug(
            'Saving job with persistence monitor into: %s', job_filename)
        description = {
            'id': job.id,
            'status': job.status,
            'batch_name': job.batch_name,
            'info': job.info,
            'file_attachments':job.file_attachments, 
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
            logger.warn('Job is already attached to the monitor.')
            return
        job_file = os.path.join(self.state_path, job.status, 
                                self.get_job_file(job.id))
        job.is_attached = True
        return self.save_job(job_file, job)


class JobRunner(object):
    
    def __init__(self, report_path=None, attachment_path=None):
        self.report_path = report_path
        self.attachment_path = attachment_path
        self.__all_jobs = None

    def init_db(self):
        config = self.scheduler.config 
        if self.report_path is None: 
            self.report_path = config.get('scheduler', 'reports_path')
        if self.attachment_path is None: 
            self.attachment_path = config.get('scheduler', 'attachments_path')

    @property
    def logger(self):
        return self.scheduler.logger

    @property
    def all_jobs(self):
        '''Jobs that run by the underling scheduler's runner (cluster).'''
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
        '''
        Inherit this method to check for scheduler-specific parameters.
        '''
        pass

    def save_attachments(self, job):
        '''
        If job has any attachments in it, now is the moment to create them
        as well as to pass info about them into the command.
        '''
        if not job.file_attachments:
            return
        file_pathnames = dict()
        for file_name in job.file_attachments:
            file_path = os.path.join(self.attachment_path, file_name) 
            file_path += '.%s' % job.id
            data = job.file_attachments[file_name]
            attachment_file = open(file_path, 'w+')
            attachment_file.write(data)
            attachment_file.close()
            file_pathnames[file_name] = file_path
        # Substitute all filename entries in the command with real pathnames.        
        job.info['command'] = job.info['command'].format(**file_pathnames)

    def execute(self, job):
        '''
        Override this to implement job execution using concrete scheduler.
        '''
        self.save_attachments(job)



class Scheduler(object):

    def __init__(self, runner, monitor, config=None):

        self.config = config
        if self.config is None:
            self.config = ConfigParser()
        self.runner = runner
        self.runner.scheduler = self
        self.monitor = monitor
        self.monitor.scheduler = self
        self.init_db()
    
    def init_db(self):
        '''Override to implement custom db initialization'''
        self.runner.init_db()
        self.monitor.init_db()
        
    def validate_config(self):
        '''
        Override to implement scheme-specific validation. This validation 
        method can not be invoked by Scheduler__init__, but rather later,
        after config files are loaded. 
        '''
        
    @classmethod
    def create(cls, scheme_name, state_path, config, is_interactive=None, logger=None):
        '''Create scheduler by its scheme name'''
        assert len(scheme_name) > 1
        scheme = import_module('plato.schedule.' + scheme_name.lower())        
        # E.g. 'LSF' -> 'Lsf'
        prefix = scheme_name[0].upper() + scheme_name[1:].lower() 
        RunnerCls = getattr(scheme, prefix + 'Runner')
        MonitorCls = getattr(scheme, prefix + 'Monitor')
        SchedulerCls = getattr(scheme, prefix + 'Scheduler')
        runner = RunnerCls()
        if is_interactive is None:
            is_interactive = config.getboolean('scheduler', 'isinteractive') 
        monitor = MonitorCls(state_path, scheme_name, is_interactive)
        return SchedulerCls(runner, monitor, config=config)
    
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
            logger.error('Will only accept jobs that were previously submitted.')
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
            logger.error('Will only update jobs that are already pending.')
            return
        if not self.runner.is_running(job):
            logger.info('Job is still pending: %s' % job)            
        # Update status if necessary.
        if job.status == 'pending':
            logger.info('Job is now running: %s ' % job)
            self.monitor.detach_job(job)
            job.status = 'run'
            self.monitor.attach_job(job)
            return
        if self.runner.is_done(job) and self.runner.report_file_exists(job):
            logger.info('Job is (already) done: %s ' % job)
            # Job finished.
            # TODO: check that file is not written into anymore
            result = self.runner.get_result(job)
            if not result:
                self.error('Failed to obtain a result for: %s' % job)
            self.complete_job(job, result)
        logger.info('Job is still running: %s ' % job)
            
        
    
    def complete_job(self, job, result):
        if not job.status in ('submit', 'pending', 'run'):
            logger.error('We can only complete jobs that were submitted, pending or running.')
            return
        self.monitor.detach_job(job)
        job.result = result
        if not result or result.has_failed:
            job.status = 'failed'
        else:
            job.status = 'done'
        self.monitor.attach_job(job)
   
    def submit_jobs(self):
        '''
        Make all freshly submitted plato jobs pending, i.e.
        Use actual low-level scheduler to submit them.
        '''
        submitted_jobs = self.monitor.load_jobs(['submit'])        
        logger.info('Processing submitted jobs.. found (%d) jobs',
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
        logger.info('Processing pending and running jobs.. found (%d) jobs',
                         len(pending_jobs))
        for job in pending_jobs:
            self.update_job(job)

