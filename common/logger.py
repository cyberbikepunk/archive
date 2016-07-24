""" This module configures the logger from the YAML file. """


from logging import Filter
from logging.config import dictConfig
from yaml import load

from common.settings import LOGGING_CONFIG_FILE


def configure_logger():
    with open(LOGGING_CONFIG_FILE) as f:
        yaml = f.read()
    config = load(yaml)
    dictConfig(config)


class MultilineFilter(Filter):
    # http://stackoverflow.com/questions/22934616/multi-line-logging-in-python
    # This class is useful when outputting PETL tables or Pandas dataframes.
    def filter(self, record):
        message = str(record.msg)
        if '\n' in message:
            record.msg = '...\n\t' + message.replace('\n', '\n\t')
        return super(MultilineFilter, self).filter(record)

