'''
PyPlato is set of batching utilies useful in HPC scheduling context
and shell (bash-like) automation in python.


Copyright (C) 2013  Yauhen Yakimovich

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
'''
import logging


def getBasicLogger(name, level):
    logging.basicConfig(level=level,
                    format='%(asctime)s %(name)-20s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
    console = logging.StreamHandler()
    console.setLevel(level)
    logger = logging.getLogger(name)
    return logger
