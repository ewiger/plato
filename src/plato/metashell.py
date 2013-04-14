import subprocess
import logging

# Stream codes
stdin = 0
stdout = 1
stderr = 2

# Literals
white_space = ' '
vertical_pipe = '|'
double_quote = '"'


class Command(object):
    
    def __init__(self, tokens,
                 input_stream=stdin, output_stream=stdout, error_stream=stderr,  
                 shell=False, command=False, separator=white_space):         
        self.tokens = list() if not tokens else tokens
        if type(self.tokens) == tuple:
            self.tokens = list(self.tokens)
        if command is not False:            
            tokens.extend(self.parse(command))
        self.input = input_stream
        self.output = output_stream   
        self.error = error_stream           
        self.shell = shell
        self.separator = separator
        self.is_composed = False             

    def parse(self, command):
        # TODO: parse < > 2>&
        if type(command) == str:
            return command.split(white_space)
        raise Exception('Unsupported command type')

    def compose(self):
        if not self.is_composed:            
            if self.shell is not False:
                self.tokens.insert(0, self.shell)                        
            self.is_composed = True
        # Stringify.
        tokens = [str(token) for token in self.tokens]
        # Compile sequence of tokens.         
        return self.separator.join(tokens)

    def __call__(self):
        self.run()

    def run(self):
        command = self.compose()
        if not command:
            raise Exception("Empty command")
        return self.run_command(command)

    @classmethod            
    def run_command(cls, command):
        if not command:
            raise Exception('Trying to run an empty command!')
        logging.info('RUNNING: ' + command)
        p = subprocess.Popen(command, shell=True, 
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, close_fds=True)
        output = p.stdout.read()
        errors = p.stderr.read()
        if len(errors) > 0:
            logging.info('ERRORS')
            logging.info(errors)
        if len(output) > 0:
            logging.info('OUTPUT') 
            logging.info(output)
        else:
            logging.info('EMPTY OUTPUT')
        return (output, errors)
    
    def __or__(self, right):
        if type(self) == Pipeline and type(right) != Pipeline:
            # Append pipeline with command.
            self.tokens.append(right)            
            return self
        elif type(self) != Pipeline and type(right) == Pipeline:
            # Prepend pipeline with command.
            right.tokens.insert(0, self)
            return right
        return Pipeline(self, right)

    def __str__(self):
        return self.compose()


class Program(Command):
    
    def __init__(self, name, *args, **kwargs):
        self.name = name                   
        self.arguments = list() if len(args) == 0 else args
        if type(self.arguments) == tuple:
            self.arguments = list(self.arguments)
        super(Program, self).__init__(False, **kwargs)

    def compose(self):
        if not self.is_composed:            
            self.tokens.insert(0, self.name)
            self.tokens.extend(self.arguments)
            # Quote arguments containing a white space.            
            for index in range(len(self.tokens)):
                if not white_space in self.tokens[index]:
                    continue
                if self.tokens[index][0] != double_quote:
                    self.tokens[index] = double_quote + self.tokens[index]
                if self.tokens[index][-1] != double_quote:    
                    self.tokens[index] = self.tokens[index] + double_quote
        return super(Program, self).compose()


class Pipeline(Command):
    
    def __init__(self, *programs, **kwargs):
        if not 'separator' in kwargs:
            kwargs['separator'] = white_space + vertical_pipe + white_space
        super(Pipeline, self).__init__(programs, **kwargs)
