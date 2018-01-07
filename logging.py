import logging
import sys
from time import gmtime

def disable_logger(name):
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False

def setup_root_logger(log_level):
    logger = logging.root
    logger.setLevel(log_level)
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
