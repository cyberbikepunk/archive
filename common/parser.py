""" A parser base class for command line interfaces. """


from argparse import ArgumentParser
from yaml import load

from common.settings import CONNECTOR_CONFIG_FILE


def read_config_from_yaml():
    with open(CONNECTOR_CONFIG_FILE) as f:
        contents = f.read()

    return load(contents)


class InternalToolsParser(ArgumentParser):
    epilog = 'To read arguments from file pass @/absolute/path/to/file.ini'
    fromfile_prefix_chars = '@'

    def __init__(self, *args, **kwargs):
        super(InternalToolsParser, self).__init__(*args, **kwargs)

        # All base parser arguments default to None.
        # None values don't override YAML parameters.

        self.add_argument(
            '-d', '--debug',
            action='store_true',
            dest='debug',
            default=None,
            help='produce a debug a log file',
        )
        self.add_argument(
            '-v', '--verbose',
            action='store_true',
            dest='verbose',
            default=None,
            help='print verbose to the console',
        )
        self.add_argument(
            '-p', '--password',
            action='store',
            dest='password',
            type=str,
            help='override the configuration file',
        )
        self.add_argument(
            '-u', '--username',
            action='store',
            type=str,
            dest='username',
            help='override the configuration file',
        )
