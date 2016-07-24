""" This modules helps analyse UK driver information discrepancies between data sources. """

import os.path
from datetime import datetime
from logging import getLogger
from os import makedirs
from re import compile, IGNORECASE
from uuid import UUID

from pandas import DataFrame, read_excel, Series, concat
from petl import fromdataframe, frompickle, join, antijoin, outerjoin

from common.logger import configure_logger
from common.settings import CACHE_DIR
from common.sqlreader import SQLReader
from connectors import WarehouseConnector, CloudSQLConnector, ValkfleetConnector, OdooConnector


configure_logger()
log = getLogger(__name__)
timestamp = datetime.now()


# In offline mode we grab data from the cache folder.
# This is useful for debugging, since some queries
# take a while. Remember to set to false for production.
OFFLINE = False


DRIVERS_IN_ODOO_FILEPATH = os.path.join(CACHE_DIR, 'drivers_from_odoo.pickle')
USERS_IN_BACKEND_FILEPATH = os.path.join(CACHE_DIR, 'users_from_backend.pickle')
DRIVERS_IN_BACKEND_FILEPATH = os.path.join(CACHE_DIR, 'drivers_from_backend.pickle')
FLEETS_IN_BACKEND_FILEPATH = os.path.join(CACHE_DIR, 'fleets_from_backend.pickle')
DRIVERS_IN_PLANDAY_FILEPATH = os.path.join(CACHE_DIR, 'all_employees_from_planday.xlsx')

ODOO_WITH_BACKEND_FILENAME = os.path.join(CACHE_DIR, 'odoo_with_backend.xlsx')
ODOO_WITH_PLANDAY_FILENAME = os.path.join(CACHE_DIR, 'odoo_with_planday.xlsx')


if not os.path.isdir(CACHE_DIR):
    makedirs(CACHE_DIR)


def strip(x): return x.strip()


def load_into_warehouse():
    api = WarehouseConnector()

    odoo = extract_odoo()
    backend = extract_backend()
    planday = extract_planday()

    # Make a flat table to compare
    odoo_with_backend = outerjoin(odoo, backend, lkey='backend_username', rkey='backend_username')
    odoo_with_planday = outerjoin(odoo, planday, key='salary_id')

    odoo_with_backend = odoo_with_backend.addfield('etl_timestamp', timestamp)
    odoo_with_planday = odoo_with_planday.addfield('etl_timestamp', timestamp)

    write_to_log(odoo_with_backend, 'odoo_with_backend', 'outerjoin')
    write_to_log(odoo_with_planday, 'odoo_with_planday', 'outerjoin')

    odoo_with_backend.toxlsx(ODOO_WITH_BACKEND_FILENAME)
    odoo_with_planday.toxlsx(ODOO_WITH_PLANDAY_FILENAME)

    odoo_with_planday.appenddb(api.db, 'sync_police_uk_drivers_planday_vs_odoo', schema='tableau')
    odoo_with_backend.appenddb(api.db, 'sync_police_uk_drivers_backend_vs_odoo', schema='tableau')


def extract_odoo(offline=OFFLINE):
    if not offline:
        api = OdooConnector()

        filters = [('supplier', '=', True),
                   ('active', '=', True),
                   ('company_id', '=', 3)]

        dataframe = api.extract('res.partner', filters)
        drivers = fromdataframe(dataframe)

        mappings = {
            'backend_username': 'x_driver_app_username',
            'backend_uuid': 'x_backend_uuid',
            'salary_id': 'x_salary_id',
            'odoo_id': 'id',
            'fleetname': lambda rec: rec['x_fleet'][1].replace('_', ' '),
            'fullname': lambda rec: rec['display_name'].strip()
        }

        drivers = drivers.fieldmap(mappings)
        drivers = drivers.suffixheader('_in_odoo')
        drivers.topickle(DRIVERS_IN_ODOO_FILEPATH)

    else:
        drivers = frompickle(DRIVERS_IN_ODOO_FILEPATH)

    drivers = drivers.addfield('backend_username', lambda rec: rec['backend_username_in_odoo'])
    drivers = drivers.addfield('salary_id', lambda rec: rec['salary_id_in_odoo'])

    drivers = standardize_missing_values(drivers)
    write_to_log(drivers, 'drivers', 'odoo')

    return drivers


def extract_backend(offline=OFFLINE):
    # Done in 4 steps: (1) grab the driver table from the CloudSQL,
    # (2) use the user uuids to query for users one by one through
    # the API, (3) get the fleet table from CloudSQL and (4) join
    # everything together.

    def extract_drivers():
        query = SQLReader('sql.drivers_from_cloudsql')
        drivers_df = sql.execute(query.statements[0])
        drivers_tb = fromdataframe(drivers_df)

        mappings = {
            'driver_uuid': lambda rec: str(UUID(bytes=rec['uuid'], version=4)),
            'fleet_uuid': lambda rec: str(UUID(bytes=rec['fleet_uuid'], version=4)),
            'user_uuid': lambda rec: str(UUID(bytes=rec['user_ds_uuid'], version=4)),
            'fullname': lambda rec: rec['last_name'].strip() + ', ' + rec['first_name'].strip(),
        }

        drivers_tb = drivers_tb.fieldmap(mappings)
        drivers_tb = drivers_tb.suffixheader('_in_backend')

        return drivers_tb

    def extract_users():
        users_records = [api.get_record('users', driver.user_uuid_in_backend)
                         for driver in drivers.namedtuples()]
        users_df = DataFrame().from_records(users_records)
        users_tb = fromdataframe(users_df)

        mappings = {
            'driver_uuid': 'driver',
            'user_uuid': 'uuid',
            'backend_username': 'username'
        }

        users_tb = users_tb.fieldmap(mappings)
        users_tb = users_tb.suffixheader('_in_backend')

        return users_tb

    def extract_fleets_from_dwh():
        query = SQLReader('sql.fleets_from_tableau')
        fleets_df = dwh.execute(query.statements[0])
        fleets_tb = fromdataframe(fleets_df)

        mappings = {
            'fleet_uuid': 'uuid',
            'fleetname': lambda rec: rec['backend_name'].replace('_', ' '),
            'country_code': 'country_code',
        }

        fleets_tb = fleets_tb.cutout('country_code')
        fleets_tb = fleets_tb.fieldmap(mappings)
        fleets_tb = fleets_tb.suffixheader('_in_backend')

        return fleets_tb

    if not offline:
        sql = CloudSQLConnector()
        api = ValkfleetConnector()
        dwh = WarehouseConnector()

        drivers = extract_drivers()
        fleets = extract_fleets_from_dwh()
        users = extract_users()

        drivers.topickle(DRIVERS_IN_BACKEND_FILEPATH)
        fleets.topickle(FLEETS_IN_BACKEND_FILEPATH)
        users.topickle(USERS_IN_BACKEND_FILEPATH)

    else:
        drivers = frompickle(DRIVERS_IN_BACKEND_FILEPATH)
        fleets = frompickle(FLEETS_IN_BACKEND_FILEPATH)
        users = frompickle(USERS_IN_BACKEND_FILEPATH)

    write_to_log(drivers, 'drivers', 'backend')
    write_to_log(fleets, 'fleets', 'backend')
    write_to_log(users, 'users', 'backend')

    drivers_without_fleet = antijoin(drivers, fleets, key='fleet_uuid_in_backend')
    drivers_without_user = antijoin(drivers, users, key='user_uuid_in_backend')
    write_to_log(drivers_without_fleet, 'drivers without fleet', 'backend')
    write_to_log(drivers_without_user, 'drivers without user', 'backend')

    drivers_n_fleets = join(drivers, fleets, key='fleet_uuid_in_backend').cutout('fleet_uuid_in_backend')
    backend_drivers = join(drivers_n_fleets, users, key='user_uuid_in_backend')
    backend_drivers = backend_drivers.addfield('backend_username', lambda rec: rec['backend_username_in_backend'])
    backend_drivers = backend_drivers.cutout('driver_uuid_in_backend')

    backend_drivers = standardize_missing_values(backend_drivers)
    write_to_log(backend_drivers, 'drivers', 'backend')

    return backend_drivers


def extract_planday():
    def get_fleet_columns(df):
        for column in df.columns:
            label = column.lower()
            if 'employee group' in label and 'driver' in label and 'test' not in label:
                yield column

    def reduce_fleets_to_single_column(df):
        fleets = list(get_fleet_columns(df))
        fleets_dict = {}

        for employee_index, employee_row in df[fleets].iterrows():
            employee_fleets = employee_row.dropna()

            fleets = []
            for fleet, _ in employee_fleets.iteritems():
                # The label is not exactly the fleet
                # name: it needs a little grooming.
                fleet = fleet.split(': ')[1]
                fleets.append(fleet)

            fleets = ', '.join(fleets)
            fleets_dict.update({employee_index: fleets})

        return Series(fleets_dict)

    def process_fleetname(text):
        valk_driver_re = compile('valk(.?)driver(s?)\s', IGNORECASE)
        match = valk_driver_re.search(text)
        if match:
            return text.replace(match.group(0), '').lower()
        else:
            return text

    planday = read_excel(DRIVERS_IN_PLANDAY_FILEPATH)
    planday = planday[planday['Employee group: Managers'] != 0]

    fullname = planday['Last name'].map(strip) + ', ' + planday['First name'].map(strip)
    salary_id = planday['Salary identifier'].map(lambda x: x if x else None)
    fleetname = reduce_fleets_to_single_column(planday).map(process_fleetname).replace('', None)
    username = planday['Username']

    username.name = 'planday_username_in_planday'
    fullname.name = 'fullname_in_planday'
    salary_id.name = 'salary_id_in_planday'
    fleetname.name = 'fleetname_in_planday'

    planday = concat([fullname, salary_id, fleetname, username], axis=1)
    planday = fromdataframe(planday)
    planday = standardize_missing_values(planday)
    planday = planday.addfield('salary_id', lambda rec: rec['salary_id_in_planday'])

    write_to_log(planday, 'drivers', 'planday')

    return planday


def write_to_log(table, table_name, source):
    table_size = table.nrows()
    log.debug('%d %s extracted from %s', table_size, table_name, source)

    if table_size:
        # report missing values
        for field in table.fieldnames():
            count = table.valuecount(field, None)
            missing = '{field} has {count} ({percent}%) missing values'.format(field=field,
                                                                               count=count[0],
                                                                               percent=int(count[1] * 100))
            log.debug(missing)

    # print the top of the table
    for line in str(table.look()).split('\n'):
        log.debug(line)


def standardize_missing_values(table):
    table = table.replaceall(False, None)
    table = table.replaceall('', None)
    table = table.replaceall('0', None)
    table = table.replaceall('None', None)
    table = table.replaceall('False', None)

    return table


if __name__ == '__main__':
    load_into_warehouse()
