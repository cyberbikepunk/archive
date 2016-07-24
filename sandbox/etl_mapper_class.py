""" This module reinvents the wheels and was dropped. """


from datetime import datetime
from inspect import getmembers
from os.path import join
from pandas import read_csv, DataFrame, concat


class BaseETL(object):
    csv_filename = ''
    csv_seperator = ''
    header = 'infer'
    dates = []
    primary_key = ''
    foreign_key = ''
    selected_fields = []
    connector = None

    def __init__(self, csv_folder):
        self.csv_dir = csv_folder
        self.df_raw = None
        self.df = DataFrame()

    def extract_from_csv(self):
        csv_filepath = join(self.csv_dir, self.csv_filename)
        self.df_raw = read_csv(csv_filepath, sep=self.csv_seperator,
                                             header=self.header,
                                             parse_dates=self.dates)
        return self

    def _filter_rows(self):
        self.df_raw['mask'] = self._row_mask
        self.df_raw = self.df_raw[self._row_mask]

    @property
    def _row_mask(self):
        # The default mask keeps all the rows
        return self.df_raw.index.map(lambda x: True)

    def extract_table(self):
        self._filter_rows()
        columns = list(self._exported_columns)
        self.df = concat(columns, axis=1)
        return self.df

    @property
    def _exported_columns(self):
        for _, attribute in getmembers(self):
            if hasattr(attribute, 'export_name'):
                column = attribute()
                column.name = attribute.export_name
                yield column
        for field in self.selected_fields:
            yield self.df_raw[field]


def select_as(export_name):
    def decorator(f):
        f.export_name = export_name
        return f
    return decorator


class BackendETL(BaseETL):
    primary_key = 'Username'
    foreign_key = 'Driver App username'
    csv_filename = 'backend.csv'
    csv_seperator = ','
    dates = ['created_at', 'deleted_at']
    selected_fields = [
        'Username',
        'Name',
        'Fleet name',
    ]

    @property
    def _row_mask(self):
        return self.df_raw['deleted_at'] > datetime.now()


class PlandayWebConnecor(BaseETL):
    primary_key = 'salary_id'
    foreign_key = 'Planday username'
    csv_filename = 'planday.csv'
    csv_seperator = ';'
    header = None
    selected_fields = []

    @select_as('fullname')
    def fullname(self):
        return self.df_raw[1] + self.df_raw[2]

    @select_as('planday_username')
    def username(self):
        return self.df_raw[3]

    @select_as('salary_id')
    def salary_id(self):
        return self.df_raw[4]

class OdooETL(BaseETL):
    primary_key = 'odoo_id'
    csv_filename = 'odoo.csv'
    csv_seperator = ','

    selected_fields = [
        'External ID',
        'Name',
        'Fleet/Name',
        'Planday username',
        'Salary ID',
        'Driver App username'
    ]

    @select_as('Odoo ID')
    def odoo_id(self):
        return self.df_raw['External ID'].fillna('').map(lambda x: x.replace('__export__.res_partner_', ''))

    @property
    def _row_mask(self):
        def is_driver(x): return True if 'driver' in x.lower() else False
        return self.df_raw['Job Position'].fillna('').map(is_driver) & self.df_raw['Active']
