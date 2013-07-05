import os
from contextlib import contextmanager


def touch(fname, times = None):
    fhandle = file(fname, 'a')
    try:
        os.utime(fname, times)
    finally:
        fhandle.close()


@contextmanager
def change_dir(*args, **kwds):
    last_working_dir = os.getcwd()
    try:
        path = os.path.join(*args)
        if len(kwds) > 0:
            path = path.format(**kwds)
        os.chdir(path)
        yield path
    finally:
        os.chdir(last_working_dir)
