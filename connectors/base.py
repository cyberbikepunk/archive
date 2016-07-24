""" This module defines connector base classes. """


from logging import getLogger
from sqlalchemy.exc import OperationalError
from yaml import load

from common.settings import CONNECTOR_CONFIG_FILE


log = getLogger(__name__)


class BaseConnector(object):
    """ The base class for all connections. """

    def __init__(self, **options):
        self.name = self.__class__.__name__
        self.config = self._configure(**options)
        self.db = self._authenticate()

    def _load_config(self):
        with open(CONNECTOR_CONFIG_FILE) as f:
            contents = f.read()
            config = load(contents)

        return config[self.name]

    def _configure(self, **options):
        parameters = self._load_config()
        config = dict()

        for key, value in parameters.items():
            # Command line arguments override YAML
            # parameters if they are not empty
            if key in options and options[key]:
                value = options[key]
            config[key] = value

        return config

    @property
    def safe_config(self):
        # For the logger
        return {k: v for k, v
                in self.config.items()
                if k != 'password'}

    def __repr__(self):
        return '<{cls} ({user} connected to {host})>'.format(cls=self.name,
                                                             host=self.host,
                                                             user=self.username)

    # Handy shortcuts
    def __getattr__(self, key):
        return self.config[key]

    def execute_test(self):
        log.debug('Connector = %s' % self.name)
        log.debug('Configuration = %s', self.safe_config)

        try:
            dataframe = self._execute_test()
            log.debug('Dataframe shape = %s', dataframe.shape)
            log.debug('Result = success!')

        except Exception as error:
            log.error(error, exc_info=True)
            log.debug('Result = failure!')

    def _authenticate(self):
        pass

    def _execute_test(self):
        pass
