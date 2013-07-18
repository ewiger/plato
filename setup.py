#!/usr/bin/env python
# TODO: migrate to distribute?
#import distribute_setup
#distribute_setup.use_setuptools()
from glob import glob
#from distutils.core import setup
from setuptools import setup

setup(
    name='PyPlato',
    version='0.1.2',
    description='Python Batching Utilities',
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
