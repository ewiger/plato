import logging


def getBasicLogger(name, level):
    logging.basicConfig(level=level,
                    format='%(asctime)s %(name)-20s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
    console = logging.StreamHandler()
    console.setLevel(level)
    logger = logging.getLogger(name)
    return logger
