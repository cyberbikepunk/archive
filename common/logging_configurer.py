""" This module configures the loggers.

Created on Jul 9, 2015
@author: nicolasguenon
Last modified by loic on Jan 14 2015
"""


from inspect import stack
from logging import Formatter, StreamHandler, getLogger, INFO, DEBUG, FileHandler
from os import makedirs
from os.path import isdir, join, dirname
from sys import stdout

from common.settings import ROOT_DIR


LOG_DIR = '/var/log/internal-tools'


# TODO: phase out this module in favour of logger.py which configures the logger from a YAML file.


# This is a wrapper around the original logging configurer.
# It sets defaults before calling the configure function.


def configure_default(name=None, debug=False, verbose=True, **kwargs):
    """ Return a logger with sensible defaults

        * Expect /var/log/internal-tools owned by the user
        * Register the name of the calling package
        * Produce verbose but no debug file
    """
    if not name:
        # The default name for the logger is the name of the calling module
        name = [s.filename for s in stack() if ROOT_DIR in s.filename][-1].split('/')[-1]

    info_file = join(LOG_DIR, name + '.info.log')

    if debug:
        debug_file = join(LOG_DIR, name + '.debug.log')
    else:
        debug_file = None

    if verbose:
        console_log_level = DEBUG
    else:
        console_log_level = INFO

    return configure(name,
                     debug_file,
                     info_file,
                     console_log_level)


# This is basically the original logger configurer that Nicolas wrote,
# with small differences: it creates log folders if they don't exist
# and the formatter has minor changes.

def configure(root_name,
              debug_file,
              info_file,
              console_log_level):

    if not root_name:
        root_name = 'root'

    for file in [info_file, debug_file]:
        if file:
            folder = dirname(file)
            if not isdir(folder):
                makedirs(folder)

    log = getLogger()
    log.setLevel(DEBUG)

    # shared by all handlers
    formatter = Formatter(
        '[%(asctime)s] '
        '[%(process)d] '
        '[%(name)s] '
        '[%(levelname)s] '
        '%(message)s '
    )

    if debug_file:
        debug = FileHandler(debug_file, mode='a+')
        debug.setLevel(DEBUG)
        debug.setFormatter(formatter)
        log.addHandler(debug)

    if info_file:
        info = FileHandler(info_file, mode='a+')
        info.setLevel(INFO)
        info.setFormatter(formatter)
        log.addHandler(info)

    if console_log_level:
        console = StreamHandler(stream=stdout)
        console.setLevel(console_log_level)
        console.setFormatter(formatter)
        log.addHandler(console)

    return log
