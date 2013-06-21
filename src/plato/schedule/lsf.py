'''
Implementation of plato scheduler for Platform LSF 
'''
import re
import logging
from StringIO import StringIO
from plato import getBasicLogger 
from plato.schedule import (Monitor, Scheduler, JobRunner, JobResult,
    NoSchedulerFound)

try:
    from sh import bjobs, bsub, bkill
except ImportError:
    raise NoSchedulerFound('Failed to locate LSF commands like bjobs, '
                           'bsub, bkill. Is LSF installed?')


NEW_LINE = '\n'
NUM_SECTION_LINES = 1000

logger = getBasicLogger('lsf', logging.DEBUG)


#JOBID      USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
#19460073   yyauhen DONE  pub.1h     brutus4     a3241       *t.xml";fi Apr 17 14:54
job_expr = re.compile(r'(\d+)\s+(\w+)\s+(\w+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.*)')
submit_expr = re.compile(
    r'Job <(\d*)> is submitted to queue <([^>]*)>', re.MULTILINE)


class LsfRunner(JobRunner):
    
    def is_running(self, job):
        lsf_id = job.info['lsf_id']
        if not lsf_id in self.all_jobs:
            return False
        found_job_info = self.all_jobs[lsf_id]
        self.logger.debug('LSF status of the job [%s]: %s' 
            % (job.id, found_job_info['lsf_status']))
        return found_job_info['lsf_status'].lower() == 'run'

    def is_done(self, job):
        lsf_id = job.info['lsf_id']
        if not lsf_id in self.all_jobs:
            # Check if report file is found
            if self.report_file_exists(job):
                report = self.get_report_filepath(job)
                self.logger.info('Found a report file for job [%d]: %s' % (
                                 job.id, report))
                return True
            return False
        found_job_info = self.all_jobs[lsf_id]
        return found_job_info['lsf_status'].lower() == 'done'

    def execute(self, job):
        super(LsfRunner, self).execute(job)
        result = JobResult(has_failed=True, details=job.info.copy())
        stderr = StringIO()
        if job.info['queue'] == 'default':
            report = self.get_report_filepath(job)
            result.output = bsub(
                '-o', report,
                _in=job.info['command'], 
                _err=stderr,
            ).strip()
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

    def parse_report(self, report_filename, result):
        self.logger.info('Parsing report file: ' + report_filename)
        with open(report_filename, 'r') as report:
            report_text = report.read().split(NEW_LINE)
        section_name = 'header'
        section = list()
        for line in report_text:
            if re.match(r'^Your job looked like:', line):
                result.details[section_name] = section[:NUM_SECTION_LINES]
                section = list()
                section_name = 'input'
                continue
            if section_name == 'input':
                if re.match(r'^Successfully completed.', line):
                    result.has_failed = False
                    continue
                if re.match(r'^Resource usage summary:', line):
                    result.input = section[:NUM_SECTION_LINES]
                    section = list()
                    section_name = 'resource_usage'  
                    continue
            elif section_name == 'resource_usage':
                if re.match(r'^The output (if any) follows:', line):
                    result.details[section_name] = section[:NUM_SECTION_LINES]
                    section = list()
                    section_name = 'output'  
                    continue
            section.append(line)
        if section_name == 'output':
            result.output = section[:NUM_SECTION_LINES]

    def get_result(self, job):
        if job.status not in ('pending', 'run', 'done', 'failed') \
            or self.report_file_exists(job) is False:
            self.logger.error('Job has no result/report yet.')
            return
        # Parse report
        result = JobResult(has_failed=True)
        self.parse_report(self.get_report_filepath(job), result)
        return result


    def list_jobs(self, mask=['all']):
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
        # Parse job info, pack it into dictionary
        job_lines = output.split('\n')[1:]  # skip column headers
        for job_line in job_lines:
            job_line = job_line.strip()
            if not job_line:
                continue
            match = job_expr.search(job_line)
            if not match:
                self.logger.warn('Failed to parse job line: %s' % job_line)
                continue
            lsf_info = dict()
            lsf_info['lsf_id'] = match.group(1)
            lsf_info['lsf_user'] = match.group(2)
            lsf_info['lsf_status'] = match.group(3)
            lsf_info['lsf_queue'] = match.group(4)
            lsf_info['lsf_from_host'] = match.group(5)
            lsf_info['lsf_exec_host'] = match.group(6)
            lsf_info['lsf_job_name'] = match.group(7)
            lsf_info['lsf_submitted_time'] = match.group(8)
            result[lsf_info['lsf_id']] = lsf_info
        return result

class LsfMonitor(Monitor):
    pass    


class LsfScheduler(Scheduler):
    pass
    
