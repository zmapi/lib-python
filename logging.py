import logging
import sys
from time import gmtime

class UpperCapFilter(logging.Filter):
    """Filter used to pick only records that have levelno below cutofflevel."""

    def __init__(self, cutofflevel):
        self.cutofflevel = cutofflevel

    def filter(self, record):
        return record.levelno < self.cutofflevel

def disable_logger(name):
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.propagate = False

def setup_root_logger(log_level, **kwargs):

    fmt = kwargs.pop(
            "fmt", "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s")
    datefmt = kwargs.pop("datefmt", "%H:%M:%S")
    if kwargs:
        raise ValueError("unexpected kwargs: {}".format(sorted(kwargs.keys())))

    logger = logging.root
    logger.setLevel(log_level)
    logger.handlers.clear()
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime

    stdout = logging.StreamHandler(stream=sys.stdout)
    stdout.name = "stdout"
    stdout.addFilter(UpperCapFilter(logging.WARNING))
    stdout.setFormatter(formatter)
    logger.addHandler(stdout)

    stderr = logging.StreamHandler(stream=sys.stderr)
    stderr.name = "stderr"
    stderr.level = logging.WARNING
    stderr.setFormatter(formatter)
    logger.addHandler(stderr)


def setup_basic_logging(log_level, **kwargs):
    # parso.* loggers are annoying when using ipdb
    disable_logger("parso.python.diff")
    disable_logger("parso.cache")
    return setup_root_logger(log_level, **kwargs)



