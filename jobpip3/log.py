
# wrapper module for panoracle logging

import logging
import inspect


logger = None

# custom loglevels
logging.NOISY = logging.DEBUG - 1
logging.VERYNOISY = logging.DEBUG - 2


def _construct(msg, level):
    """construct log output"""
    # return bare message in "info" loglevel
    if level == logging.INFO:
        return msg
    # get filename
    (frame, filename, line_number, function_name, lines, index) = \
        inspect.getouterframes(inspect.currentframe())[2]
    # get loglevel name
    lname = logging.getLevelName(level)
    # strip path
    filename = filename.split('/')[-1]
    return '(' + lname + ') ' + filename + "[" + str(line_number) + "]: " + msg


# ------------------------------------------------------------------------------
def init(instance="jobpip3", level="info", console=False):
    """initialize logging mechanism for an instance.
       :param instance Name of this instance (e.g. program name)
       :param level The current loglevel (s. _level_to_string)
       :param console Output to stderr if true"""

    # add custom levels
    logging.addLevelName(logging.NOISY, "NOISY")
    logging.addLevelName(logging.VERYNOISY, "VERYNOISY")

    # main logger
    global logger
    logger = logging.getLogger(instance)

    # console logger
    if console is True:
        console = logging.StreamHandler()
        console.setLevel(logging.getLevelName(level.upper()))
        console.setFormatter(
            logging.Formatter('%(levelname)-8s|||%(message)s')
        )
        logger.addHandler(console)


def log(level, msg):
    """ wrapper to call logger directly"""
    if logger is None: init()
    # need to convert loglevel ?
    if not isinstance(level, int):
        # convert to int
        level = logging.getLevelName(
            level.strip().upper()
        )
    # pass through to logger
    logger.log(level, msg)


def verynoisy(msg):
    """custom level logger"""
    if logger is None:
        init()
    logger.log(logging.VERYNOISY, _construct(msg, logging.VERYNOISY))


def noisy(msg):
    """custom level logger"""
    if logger is None:
        init()
    logger.log(logging.NOISY, _construct(msg, logging.NOISY))


def debug(msg):
    """wrapper"""
    if logger is None:
        init()
    logger.log(logging.DEBUG, _construct(msg, logging.DEBUG))


def info(msg):
    """wrapper"""
    if logger is None:
        init()
    logger.log(logging.INFO, _construct(msg, logging.INFO))


def warn(msg):
    """wrapper"""
    if logger is None:
        init()
    logger.log(logging.WARN, _construct(msg, logging.WARN))


def error(msg):
    """wrapper"""
    if logger is None:
        init()
    logger.log(logging.ERROR, _construct(msg, logging.ERROR))


def critical(msg):
    """wrapper"""
    if logger is None:
        init()
    logger.log(logging.CRITICAL, _construct(msg, logging.CRITICAL))


def setLevel(level):
    """set maximum loglevel"""
    if logger is None:
        init()
    logger.setLevel(level.upper())
    console.setLevel(level.upper())


def getLevel():
    """get current loglevel"""
    if logger is None:
        init()
    return logging.getLevelName(logger.getEffectiveLevel())


