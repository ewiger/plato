import subprocess


PIPE_CHAR = ' | '


class Command(object):
    
    def __init__(self):
        


def run(command, is_verbose=False):
    if not command:
        raise Exception('Trying to run an empty command!')
    if is_verbose:
        print('RUNNING: ' + command)
    p = subprocess.Popen(command, shell=True, 
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, close_fds=True)
    output = p.stdout.read()
    errors = p.stderr.read()
    if is_verbose:
        if len(errors) > 0:
            print('ERRORS')
            print(errors)
        if len(output) > 0:
            print('OUTPUT')
            print(output)
        else:
            print('EMPTY OUTPUT')
    return output


def pipe(*args)
    if len(args) == 0:
        raise Exception('Nor arguments')
    return run(PIPE_CHAR.join(args)) 
