""" WELCOME TO THE REPORT FACTORY

    The report factory mass produces reports according to a blueprint.

    From the internal-tools package root directory, run something like:
        $ python -m reports -i 2015-11-16 2015-11-30 -b uk_restaurant_deliveries
                            -s restaurant_name -o gastronomic_day -g fleet_city


    You can also load arguments from file:
        $ python -m reports @absolute/path/to/file.ini

    Or:
        $ python -m reports -h

    To produce a new type of report, copy the 'example' blueprint and modify it.
    Every blueprint directory must include the following files:

        * customizer.py: table columns definitions
        * query.sql: SQL database query
        * template.html: Jinja2
        * stylesheet.css: stylesheet for the pdf engine

    The output is a tree of reports: ~reports/blueprint/group_by/select_by/output.pdf.
    Caution! Any existing file will be overwritten.

    If no date arguments are specified, the program will process the last half-month,
    which is either from the 1st to the 15th or from the 16th to the end of the month.

    @author: loic.jounot@valkfleet.com

"""

from argparse import RawDescriptionHelpFormatter
from calendar import monthrange
from datetime import date
from importlib import import_module
from logging import getLogger
from os import makedirs, remove
from os.path import join, isdir, isfile, dirname, abspath, expanduser
from textwrap import dedent
from time import strptime
from bunch import Bunch
from jinja2 import FileSystemLoader, Environment
from pandas import read_sql_query, DataFrame, to_datetime, ExcelWriter
from slugify import slugify
from weasyprint import HTML

from common.logger import configure_logger
from common.parser import InternalToolsParser
from common.settings import ROOT_DIR
from common.sqlreader import SQLReader
from connectors import WarehouseConnector


OUTPUT_DIR = join(expanduser('~'), 'pdf_report_blueprints')
REPORTS_DEBUG_LOG = join(OUTPUT_DIR, 'debug.log')
REPORTS_INFO_LOG = join(OUTPUT_DIR, 'info.log')


blueprints_dir = dirname(abspath(__file__))
blueprints_dir = join(ROOT_DIR, 'assets/pdf_report_blueprints')

INTERVAL_BEGIN = 0
INTERVAL_END = 1
LINE = '=' * 100


class ReportError(Exception):
    pass


def mass_produce_reports(**kwargs):
    """ Mass produce pdf_report_blueprints according to a blueprint. """

    configure_logger()
    log = getLogger(__name__)

    log.info(LINE)
    for key, value in kwargs.items():
        log.info('Settings: %s = %s', key, value)

    options = Bunch(**kwargs)
    factory = Factory(options)

    with factory.engine.connect() as connection, connection.begin():
        factory.load_data()
        factory.check_data()

    log.info('Data loaded from %s', factory.engine)
    log.info('Processing %d %s pdf_report_blueprints', factory.total_reports, options.blueprint)

    for name, data in factory.extract_report_data():
        report = Report(name, data, options)
        report.create_folder()

        table_df = report.build_custom_table()

        if options.format == 'pdf':
            table_html = table_df.to_html(index=False, justify='left')
            report_html = factory.jinja_template.render(options=options, header=report.header, table=table_html)
            HTML(string=report_html).write_pdf(report.filepath, stylesheets=factory.css_stylesheets)

        elif options.format == 'xlsx':
            writer = ExcelWriter(report.filepath)
            table_df.to_excel(writer, name)
            writer.save()

        else:
            raise ReportError('Unsupported output file format')

        log.debug('Saved %s', report.filepath)

    log.info('Done processing %s pdf_report_blueprints', options.blueprint)
    log.info(LINE)


class Factory(object):
    """ This class handles stuff that is common to all pdf_report_blueprints. """

    def __init__(self, options):
        self.options = options
        self.data = None
        self.engine = WarehouseConnector().db

    def load_data(self):
        sql_file = 'assets.pdf_report_blueprints.%s.query' % self.options.blueprint
        dynamic_sql = SQLReader(sql_file).statements[0]
        self.data = read_sql_query(dynamic_sql, self.engine, params=self.options)
        return self._detect_datetimes(self.data)

    @staticmethod
    def _detect_datetimes(df):
        # This sucks:
        for column in df.columns.values:
            if 'timestamp' in column:
                df[column] = to_datetime(df[column])
        return df

    def check_data(self):
        for key in ('select_by', 'group_by'):
            if self.options[key] not in self.data.columns.values:
                raise ReportError('%s not in query (%s option)' % (self.options[key], key))
        for field in self.options.order_by:
            if field not in self.data.columns.values:
                raise ReportError('%s not in query (order_by option)' % field)

    @property
    def report_names(self):
        report_names = self.data[self.options.select_by]
        return set(report_names)

    def extract_report_data(self):
        for report_name in self.report_names:
            if report_name:
                report = self.data[self.data[self.options.select_by] == report_name]
                yield report_name, report

    @property
    def total_reports(self):
        return 0 if self.report_names == [None] else len(self.report_names)

    @property
    def jinja_template(self):
        jinja_loader = FileSystemLoader(self._blueprint_folder)
        environment = Environment(loader=jinja_loader)
        return environment.get_template('template.html')

    @property
    def css_stylesheets(self):
        return [join(self._blueprint_folder, 'stylesheet.css')]

    @property
    def _blueprint_folder(self):
        return join(blueprints_dir, self.options.blueprint)


class Report(object):
    """ This class spits out pdf_report_blueprints as pandas DataFrames. """

    def __init__(self, name, data, options):
        self.options = options
        self.name = name
        self.header = Header(data)
        self.data = data.sort_values(by=self.options.order_by)

    def build_custom_table(self):
        module = import_module('assets.pdf_report_blueprints.%s.customizer' % self.options.blueprint)
        columns = module.Customizer(self.data, self.options)
        return columns.build()

    def create_folder(self):
        if not isdir(self._output_folder):
            makedirs(self._output_folder)
        if isfile(self.filepath):
            remove(self.filepath)

    @property
    def filepath(self):
        return join(self._output_folder, self._filename)

    @property
    def _output_folder(self):
        return join(
            OUTPUT_DIR,
            self.options.blueprint,
            slugify(getattr(self.header, self.options.group_by)),
            slugify(self.name)
        )

    @property
    def _filename(self):
        parts = [
            slugify(self.name),
            'from', str(self.options.start_date),
            'to', str(self.options.stop_date),
        ]
        return '_'.join(parts) + '.' + self.options.format


class Header(object):
    """ This class provides key/value access to invariant columns. """

    def __init__(self, data):
        self.data = data

    def __getattr__(self, key):
        try:
            return self.data.iloc[0][key]
        except KeyError:
            raise ReportError('Header field %s not in query' % key)


def catch_query_mismatch(cls):
    """ Provide feedback if the Customizer uses fields that are not in the query.

    :param cls: Customizer class object
    """

    def decorate(column_maker):
        def wrapper(customizer):
            try:
                return column_maker(customizer)
            except AttributeError:
                raise ReportError('Missing query field in %s' % column_maker.__name__)
        return wrapper

    for column_name in cls.ordered_columns:
            setattr(cls, column_name, decorate(cls.__dict__[column_name]))

    return cls


class BaseCustomizer(object):
    """ This class customizes the report columns. """

    ordered_columns = ()

    def __init__(self, data, options):
        self.options = options
        self.data = data
        self.report = DataFrame()

    def build(self):
        for column_name in self.ordered_columns:
            column_maker = getattr(self, column_name)
            self.report[self._to_title(column_name)] = column_maker()
        return self.report

    @staticmethod
    def _to_title(class_name):
        return class_name.capitalize().replace('_', ' ')


def start_stop(interval):
    if interval[INTERVAL_BEGIN] > interval[INTERVAL_END]:
        raise ValueError('Breaking the 3rd law of thermodynamics')
    return {
        'start_date': interval[INTERVAL_BEGIN],
        'stop_date': interval[INTERVAL_END]
    }


def day(date_as_string):
    t = strptime(date_as_string, '%Y-%m-%d')
    date_ = date(t.tm_year, month=t.tm_mon, day=t.tm_mday)
    if date_ > date.today():
        raise ValueError('Cannot return to the future')
    return date_


def blueprint(folder):
    files = (
        'customizer.py',
        'query.sql',
        'stylesheet.css',
        'template.html'
    )
    print(blueprints_dir)
    for file in files:
        if not isfile(join(blueprints_dir, folder, file)):
            raise ValueError(file)
    return folder


def get_defaut_time_interval(today=date.today()):
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        if 1 <= today.day <= 15:
            month = today.month - 1
        else:
            month = today.month

    if 1 <= today.day <= 15:
        bounds = (16, monthrange(year, month)[INTERVAL_END])
    else:
        bounds = (1, 15)

    return [date(year, month, bounds[INTERVAL_BEGIN]), date(year, month, bounds[INTERVAL_END])]


def build_parser():
    p = InternalToolsParser(
        prog='python -m report',
        description=dedent(__doc__),
        formatter_class=RawDescriptionHelpFormatter,
    )

    p.add_argument(
        '-i', '--interval',
        help='time period to process e.g. 01-03-2016 01-04-2016',
        type=day,
        default=get_defaut_time_interval(),
        dest='interval',
        nargs=2
    )

    p.add_argument(
        '-b', '--blueprint',
        help='name of the report blueprint directory',
        type=blueprint,
        dest='blueprint',
        required=True
    )

    p.add_argument(
        '-s', '--select_by',
        help='e.g. restaurant_name to produce restaurant pdf_report_blueprints',
        type=str,
        dest='select_by',
        required=True
    )

    p.add_argument(
        '-g', '--group_by',
        help='e.g. fleet_city to group the output files by city',
        type=str,
        dest='group_by',
        required=True
    )

    p.add_argument(
        '-o', '--order_by',
        help='e.g. gastronomic_day start_route_timestamp',
        type=str,
        dest='order_by',
        nargs='*',
        required=True
    )

    p.add_argument(
        '-f', '--format',
        help='output file format (pdf or xlsx)',
        type=str,
        dest='format',
        default='pdf',
        choices=('pdf', 'xlsx')
    )

    return p


def mass_produce_example_reports():
    parameters = {
        'blueprint': 'uk_restaurant_deliveries',
        'interval': [date(2015, 11, 15), date(2015, 11, 30)],
        'select_by': 'restaurant_name',
        'group_by': 'fleet_city',
        'order_by': 'start_route_timestamp',
        'output_by': 'fleet_city',

        # Constructed by the command line parser:
        'start_date': date(2015, 11, 15),
        'stop_date': date(2015, 11, 30)
    }
    mass_produce_reports(**parameters)


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    options = start_stop(args.interval)
    options.update((vars(args)))
    mass_produce_reports(**options)

#    mass_produce_example_reports()
