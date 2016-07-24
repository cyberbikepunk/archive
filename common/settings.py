""" Global settings. """


from os import getenv
from os.path import abspath, join, dirname

ROOT_DIR = abspath(join(dirname(__file__), '..'))
CONFIG_DIR = getenv('INTERNAL_TOOLS_CONFIG_DIR')
CACHE_DIR = join(CONFIG_DIR, 'cache')
ASSETS_DIR = join(ROOT_DIR, 'assets')

CONNECTOR_CONFIG_FILE = join(CONFIG_DIR, 'connectors.yaml')
LOGGING_CONFIG_FILE = join(CONFIG_DIR, 'loggers.yaml')

if not CONFIG_DIR:
    raise ValueError('Please set $INTERNAL_TOOLS_CONFIG_DIR')
