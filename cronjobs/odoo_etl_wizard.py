"""Extract, transform and load an Odoo table into the data warehouse.

Usage:
    cronjob.odoo_etl_wizard SOURCE TARGET [--echo]
                                          [--chunksize=N]
                                          [--query=FILTER]...
                                          [--replace]
                                          [--cache]
                                          [--grant=USER]...

Arguments:
    SOURCE                  Source table to extract from Odoo
    TARGET                  Target table in the postgres warehouse

Options:
    --help                  Show this message
    --echo                  SQLAlchemy in mode debug
    --cache                 Use the cache as a source (SOURCE.pickle in CACHE_DIR)
    --replace               Drop and re-create the database table
    --chunksize=N           Size of SQLAlchemy inserts [default: 100]
    --query=FILTER          One or more Odoo filter (e.g. "active = True")
    --grant=USER            Grant SELECT access to selected users

"""


from os.path import join, isfile
from collections import defaultdict
from datetime import datetime
from logging import getLogger, DEBUG
from docopt import docopt
from pandas import DataFrame, read_pickle, to_datetime
from petl import fromdataframe, look
from sqlalchemy.types import DATE, TIMESTAMP, INTEGER, TEXT, BOOLEAN, NUMERIC

from common.settings import CACHE_DIR
from connectors import OdooConnector, WarehouseConnector
from common.logger import configure_logger, MultilineFilter


ALL_FIELDS = []
NOW = datetime.now()
TRUNCATE = 30
MISSING_INT = -999999

configure_logger()
log = getLogger('odoo_etl_wizard')
log.addFilter(MultilineFilter())


class OdooETL(object):
    """ This class extracts, transforms and loads Odoo tables.

    It creates a postgres table with the proper data types
    if at all possible, unwraps foreign key columns (adds
    the display name field of the linked table) and grants
    access to selected users.

    """
    def __init__(self,
                 source,
                 target,
                 replace='append',
                 chunksize=100,
                 echo=False,
                 query=[],
                 cache=False,
                 grant=None):

        self.odoo_to_sqlalchemy = defaultdict(lambda: 'unsupported')
        self.odoo_to_sqlalchemy.update({
            'char': TEXT,
            'boolean': BOOLEAN,
            'text': TEXT,
            'datetime': TIMESTAMP,
            'date': DATE,
            'integer': INTEGER,
            'selection': TEXT,
            'reference': TEXT,
            'many2one': TEXT,
            'many2many': TEXT,
            'one2many': TEXT,
            'float': NUMERIC,
            'html': TEXT
        })

        self.odoo_table = source
        self.dwh_table = target
        self.db_echo = echo
        self.db_chunksize = int(chunksize)
        self.query = query
        self.use_cache = cache
        self.users = grant
        self.db_mode = 'replace' if replace else 'append'

        self.odoo = OdooConnector()
        self.dwh = WarehouseConnector()
        self.filepath = join(CACHE_DIR, self.odoo_table + '.pickle')

        self.client = self.odoo.db
        self.model = self.client.model(source)
        self.fields = self.model.fields()
        self.column_types = self.get_column_types()

        self.many2ones = self.get_foreign_keys('many2one')
        self.many2manys = self.get_foreign_keys('many2many')
        self.one2manys = self.get_foreign_keys('one2many')
        self.foreign_keys = self.many2manys | self.one2manys | self.many2ones

        self.df = DataFrame()
        self.timestamp = NOW

    def get_foreign_keys(self, relationship):
        return {key for key in self.model.keys()
                if self.fields[key]['type'] == relationship}

    def get_column_types(self):
        column_types = {key: self.odoo_to_sqlalchemy[self.fields[key]['type']]
                        for key in self.fields.keys()}

        log.debug('Odoo %s data types: %s', self.odoo_table, column_types)
        return column_types

    def drop_unsupported_colums(self):
        dropped = []
        for column, data_type in self.column_types.items():
            if data_type == 'unsupported':
                dropped.append(column)
                del self.df[column]
                log.debug('Dropped column: %s (%s)', column)

        for column in dropped:
            self.column_types.pop(column)

    def coerce_datetimes(self):
        for column, data_type in self.column_types.items():
            if data_type in (TIMESTAMP, DATE):
                self.df[column] = to_datetime(self.df[column], errors='coerce')

    def coerce_integers(self):
        for column, data_type in self.column_types.items():
            if data_type == INTEGER:
                # If I leave empty strings in a INTEGER column,
                # psycopg2 will issue a ProgrammingError.
                # If I replace empty strings with None,
                # SQLAlchemy will cast them as floats,
                # so I use an improbable integer instead.
                self.df[column].replace('', MISSING_INT, inplace=True)

    def pad_strings_with_nan(self):
        for column, data_type in self.column_types.items():
            if data_type == TEXT:
                self.df[column].replace(False, '', inplace=True)

    def grant_access(self):
        parameters = {'users': ', '.join(self.users),
                      'schema': self.dwh.schema,
                      'table': self.dwh_table}

        sql = 'GRANT SELECT ON TABLE {schema}.{table} TO {users}; COMMIT;'
        grant_access = sql.format(**parameters)
        self.dwh.db.execute(grant_access)

        log.info('SELECT access granted to %s', self.users)

    @staticmethod
    def index(i):
        return lambda x: x[i] if isinstance(x, list) and len(x) is 2 else ''

    def unwrap_foreign_keys(self):
        for key in self.foreign_keys:
            if key in self.many2ones:
                value = key + '_value'

                self.df[value] = self.df[key].map(self.index(1))
                self.df[key] = self.df[key].map(self.index(0))
                self.column_types.update({value: TEXT, key: INTEGER})
                log.debug('Unwrapped foreign key %s value (many2mone)', key)

            elif key in self.one2manys | self.many2manys:
                self.df[key] = self.df[key].map(str)
                log.debug('Serialized foreign key ids: %s (*2many)', key)

    def extract(self):
        if self.use_cache:
            if isfile(self.filepath):
                self.df = read_pickle(self.filepath)
                log.info('Unpickled %s from %s', self.odoo_table, self.filepath)
            else:
                raise FileNotFoundError('%s has no cache yet', self.odoo_table)

        else:
            records = self.client.read(self.odoo_table, self.query)
            self.df = self.df.from_records(records)
            self.df.to_pickle(self.filepath)
            log.info('Cached %s to %s', self.odoo_table, self.filepath)

        log.info('Extracted in %s rows, %s columns from %s',
                 self.df.shape[0],
                 self.df.shape[1],
                 self.odoo_table)
        log.debug(look(fromdataframe(self.df), truncate=TRUNCATE))

    def transform(self):
        self.unwrap_foreign_keys()
        self.drop_unsupported_colums()
        self.coerce_datetimes()
        self.coerce_integers()
        self.pad_strings_with_nan()

        log.info('Transformed %s', self.odoo_table)
        log.debug(look(fromdataframe(self.df), truncate=TRUNCATE))

    def load(self):
        if self.db_echo:
            getLogger('sqlalchemy.engine').setLevel(DEBUG)

        self.df['etl_timestamp'] = NOW
        log.info('ETL cronjob timestamp: %s', NOW)

        self.df.to_sql(self.dwh_table,
                       self.dwh.db,
                       index=False,
                       schema=self.dwh.schema,
                       if_exists=self.db_mode,
                       dtype=self.column_types,
                       chunksize=self.db_chunksize)

        log.info('Loaded (%s) %s rows, %s columns into %s.%s',
                 self.db_mode,
                 self.df.shape[0],
                 self.df.shape[1],
                 self.dwh.schema,
                 self.dwh_table)
        log.debug(look(fromdataframe(self.df), truncate=TRUNCATE))
        self.grant_access()


def extract_transform_and_load(*io, **options):
    """ ETL job for Odoo tables. """

    log.info('Starting ETL job: %s into %s', *io)
    etl = OdooETL(*io, **options)
    etl.extract()
    etl.transform()
    etl.load()
    log.info('Completed ETL job: %s into %s', *io)


if __name__ == '__main__':
    kwargs = {k.replace('--', ''): v for k, v in docopt(__doc__).items()}
    args = (kwargs.pop('SOURCE'), kwargs.pop('TARGET'))
    log.info('ETL source and target: %s', args)
    log.info('ETL options: %s', kwargs)
    extract_transform_and_load(*args, **kwargs)
