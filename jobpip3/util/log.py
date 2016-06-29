
# wrapper module for logging

import os
import logging
import inspect
from . import fs


# main logger instance
logger = None
# stderr logger
console_handler = None
# stream logger (parseable output to be sent within a stream)
stream_handler = None

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
def init(
    instance="jobpip3",
    path=".",
    filename=None,
    level="info",
    file=False,
    console=False,
    stream=False):
    """initialize logging mechanism for an instance.
       :param instance: Name of this instance (e.g. program name)
       :param level: The current loglevel (s. _level_to_string)
       :param path: The default directory to place file into
       :param filename: name of logfile, if None defaults to <instance>.log
       :param file: output to file if True
       :param console: output to stderr if true
       :param stream: output to stderr in parseable format"""

    # add custom levels
    logging.addLevelName(logging.NOISY, "NOISY")
    logging.addLevelName(logging.VERYNOISY, "VERYNOISY")

    # environment variable takes precedence
    level = os.getenv('LOG_LEVEL', level)
    console = bool(os.getenv('LOG_CONSOLE', console))
    file = bool(os.getenv('LOG_FILE', file))

    # main logger
    global logger
    logger = logging.getLogger(instance)

    # file logger ?
    if file:
        # mkdir path
        fs.mkdir_p(path)

        # bild filename
        if filename is None: filename = instance + ".log"

        # configure logger
        logging.basicConfig(
            filename=path + '/' + filename,
            level=level.upper(),
            format='%(asctime)s %(message)s'
        )

    # stream logger ?
    if stream:
        global stream_handler
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(
            logging.Formatter('LOG:%(levelname)s|||%(message)s')
        )
        logger.addHandler(stream_handler)

    # console logger
    elif console:
        global console_handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(
            logging.Formatter('%(message)s')
        )
        logger.addHandler(console_handler)

    # create null-handler if there's no other handler
    if not stream and not console:
        logger.addHandler(logging.NullHandler())

    # set level
    setLevel(level)


def log(level, msg):
    """ wrapper to call logger directly"""
    if logger is None: init()
    # convert level if necessary
    if not isinstance(level, int): level = convertLevel(level)

    # pass through to logger
    logger.log(level, msg)


def verynoisy(msg):
    """custom level logger"""
    log(logging.VERYNOISY, _construct(msg, logging.VERYNOISY))


def noisy(msg):
    log(logging.NOISY, _construct(msg, logging.NOISY))


def debug(msg):
    """wrapper"""
    log(logging.DEBUG, _construct(msg, logging.DEBUG))


def info(msg):
    """wrapper"""
    log(logging.INFO, _construct(msg, logging.INFO))


def warn(msg):
    """wrapper"""
    log(logging.WARN, _construct(msg, logging.WARN))


def error(msg):
    """wrapper"""
    log(logging.ERROR, _construct(msg, logging.ERROR))


def critical(msg):
    """wrapper"""
    log(logging.CRITICAL, _construct(msg, logging.CRITICAL))


def setLevel(level):
    """set maximum loglevel"""
    if logger is None: init()

    if not isinstance(level, int): level = convertLevel(level)

    if console_handler is not None:
        console_handler.setLevel(level)

    if stream_handler is not None:
        stream_handler.setLevel(level)

    logger.setLevel(level)


def getLevel():
    """get current loglevel"""
    if logger is None: init()
    return convertLevel(logger.getEffectiveLevel())


def convertLevel(level):
    """convert level from string to int or from int to string"""

    # convert to string ?
    if isinstance(level, int):
        return logging.getLevelName(level)

    # convert to int
    return logging.getLevelName(level.strip().upper())
