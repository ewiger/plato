#!/usr/bin/env python

from glob import glob
#from distutils.core import setup
from setuptools import setup

setup(
    name='Plato',
    version='0.1.1',
    description='Python Batching Utilities',
    author='Yauhen Yakimovich',
    author_email='yauhen.yakimovich@uzh.ch',
    url='https://github.com/ewiger/plato',
    scripts=glob('bin/*'),
    #data_files=glob('libexec/*'),
    packages=['plato'],
    package_dir={
        'plato': 'src/plato',
    },)
