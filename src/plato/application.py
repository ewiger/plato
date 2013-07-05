import ConfigParser
from plato.schedule import Scheduler
import os
import time
import logging


logger = logging.getLogger('plato.application')


USER_HOME_DIR = os.path.expanduser('~/')
# TODO: use dictConfig http://docs.python.org/2/library/logging.config.html
LOGGING_LEVELS = {
    'NOTSET': logging.NOTSET,
    'WARN': logging.WARN,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,                  
}


class PlatoApp(object):

    def __init__(self, name, config_name, schedulername, debug=False, 
                 is_interactive=False, logger=None):
        self.name = name        
        self.config_name = config_name
        self.debug = debug
        self.is_interactive = is_interactive
        self.__state_path = None #  Must be set for get_state_path().
        self.config = ConfigParser.ConfigParser(
            defaults=self.get_config_vars(schedulername=schedulername))
        self.init_config()        
        self.scheduler = Scheduler.create(
            self.schedulername, self.state_path, self.config)
    
    @property
    def schedulername(self):
        return self.config.get('app', 'scheduler')
    
    def get_config_vars(self, schedulername):
        '''Mainly defined shared variables with defaults'''
        return dict(
            # Fallback to default folder in user home.
            statepath=os.path.expanduser('~/.' + self.name),
            schedulername=schedulername,
        )
    
    def init_config(self):
        # Start with building some defaults.
        # Init/set application options.
        self.config.add_section('app')
        #self.config.set('app', 'statepath', '%(default_statepath)s')
        self.config.set('app', 'scheduler', '%(schedulername)s')   
        # Init/set dameon options.
        self.config.add_section('daemon')
        self.config.set('daemon', 'stdin_path', '/dev/null')
        self.config.set('daemon', 'stdout_path',
                        '/dev/tty' if self.debug else '/dev/null')
        self.config.set('daemon', 'stderr_path',
                        '/dev/tty' if self.debug else '/dev/null')
        self.config.set('daemon', 'pidfile_path', 
                        '%(statepath)s' + self.name + '.pid')
        self.config.set('daemon', 'pidfile_timeout', '5')
        self.config.set('daemon', 'sleepping_pause', '10')
        # Init/set scheduler options.
        self.config.add_section('scheduler')
        self.config.set('scheduler', 'isinteractive', 
                        str(int(self.is_interactive)))
        self.config.set('scheduler', 'attachments_path', 
                        '%(statepath)s/attachments')        
        self.config.set('scheduler', 'reports_path', 
                        '%(statepath)s/reports')
        # local
        self.config.add_section('local')
        self.config.set('local', 'pidfiles_path', 
                        '%(statepath)s/pidfiles')
    
    def load_config_file(self, config_name, config_dir):
        '''
        Load configuration from supplied filename. By default configuration
        directory is set to point to '.../bin/<APP>.conf'
        '''
        config_path = os.path.join(config_dir, config_name)
        if os.path.exists(config_path):
            logger.info('Loading %s' % config_path)
            self.config.read(config_path)

    def load_config(self, app_bin_dir, config_name=None):
        '''Load more configuration from default locations'''
        if config_name is None:
            config_name = self.config_name 
        # Load defaults from .../bin/<APP>.conf
        self.load_config_file(config_name, app_bin_dir)
        # Override with user config which is ~/<APP>.conf
        self.load_config_file(config_name, USER_HOME_DIR)

    def validate_config(self):
        '''(Optional) validate config for arguments'''
        # First run optional (method can be empty) config check for scheduler 
        # components.
        self.scheduler.validate_config()
        # Assert configuration for correctness.
        self.config.has_section('app')
        assert self.config.get('app', 'statepath') is not None

    @property
    def state_path(self):
        if self.__state_path is None:
            # Try to get configuration option.
            self.__state_path = self.config.get('app', 'statepath')
            if not os.path.exists(self.__state_path):
                logger.warn('Creating a missing dir: %s' % self.__state_path)
                os.makedirs(self.__state_path)
        return self.__state_path
    
    @property
    def stdin_path(self):
        '''Part of DaemonRunner protocol'''
        return self.config.get('daemon', 'stdin_path')

    @property
    def stdout_path(self):
        '''Part of DaemonRunner protocol'''
        return self.config.get('daemon', 'stdout_path')
    
    @property
    def stderr_path(self):
        '''Part of DaemonRunner protocol'''
        return self.config.get('daemon', 'stderr_path')
    
    @property    
    def pidfile_path(self):
        '''Part of DaemonRunner protocol'''
        return self.config.get('daemon', 'pidfile_path')
    
    @property
    def pidfile_timeout(self):
        '''Part of DaemonRunner protocol'''
        return self.config.get('daemon', 'pidfile_timeout')

    def step(self):
        self.scheduler.submit_jobs()
        self.scheduler.process_jobs()
        self.scheduler.runner.forget_job_list()

    def run(self):
        while True:
            self.step()
            # Sleep x seconds.
            sleepping_pause = self.config.getint('daemon', 'sleepping_pause')
            logger.info('Sleeping for %d (sec)..' % sleepping_pause)
            time.sleep(sleepping_pause)
