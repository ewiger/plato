#!/usr/bin/env python
# TODO: migrate to distribute?
#import distribute_setup
#distribute_setup.use_setuptools()



import os.path
import sys
from glob import glob

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def readme():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
            return f.read()
    except (IOError, OSError):
        return ''


setup(
    name='PyPlato',
    version='0.1.2',
    description='Python Batching Utilities',
    long_description=readme(),
    author='Yauhen Yakimovich',
    author_email='eugeny.yakimovitch@gmail.com',
    url='https://github.com/ewiger/plato',
    license='GPL',
    scripts=glob('bin/*'),
    #data_files=glob('libexec/*'),
    packages=['plato', 'plato.shell', 'plato.schedule'],
    package_dir={
        'plato': 'src/plato',
    },
    download_url='https://github.com/ewiger/plato/tarball/master',
)
