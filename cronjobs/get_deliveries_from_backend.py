'''
Gets the deliveries from the Backend reporting API and inserts them
both in the public.delivery and tableau.delivery tables of the DH BI
PostgreSQL DB
'''
import argparse
import csv
import datetime
import io
import json
import logging
import os
import pprint
import sys
import time
import urllib.request

import arrow
import psycopg2

from common.logging_configurer import LOG_DIR
from scripts import get_fleets_from_backend as fleets
from connectors.backend import BackendConnector
from common import logging_configurer

MODULE_NAME = 'get_deliveries_from_backend'
LOGIN_URL = 'https://api.valkfleet.com/login'
FLEETS_URL = 'https://api.valkfleet.com/fleets/'
DELIVERIES_URL = 'https://api.valkfleet.com/reporting/deliveries/'
REPORTS_URL = 'https://api.valkfleet.com/reporting/'


FLEETS_TO_SKIP = {
    '20be27c1-bf0d-423b-9bb2-59b4c2a10414': 'golden_grill',
    '261fadd0-0ada-4f20-9cb9-ce3d2bcc95f7': 'narnia',
    '90467dd4-ef75-4b38-8be9-0f0dcc84ec1b': 'nottingham_not_yet',
    '954a7525-54da-4b6c-8d6a-634e65a8944d': 'uk_not_yet',
    'a3d6c82b-ca57-4d5c-8f1e-a9c8806aa375': 'dragon_palace',
    'cc292253-54bf-4904-8ea4-c740c32b47f3': 'chunky_chicken',
    'ecfb3947-6908-4998-be81-0e98af1af21e': 'currylicious',

    'd07eefe7-d971-495d-90c3-913bd03a2b66': 'Turin',

    'f42e0a6d-eced-42aa-bbc7-a5885eee9e1b': 'vienna',
    }

EXPECTED_FIELDS = ('fleet', 'timezone', 'order_uuid', 'delivery_uuid',
                   'delivery_short_id', 'source_name', 'source_id',
                   'route_id', 'ordering_in_route', 'source_transaction_id',
                   'restaurant_uuid', 'restaurant_name', 'restaurant_city',
                   'restaurant_zipcode', 'restaurant_source_id',
                   'driver_username', 'battery_status',
                   'customer_phone_number', 'customer_street',
                   'customer_number', 'customer_zipcode', 'customer_city',
                   'customer_country', 'customer_raw_address',
                   'distance_to_customer', 'distance_traveled_to_customer',
                   'distance_to_restaurant', 'distance_traveled_to_restaurant',
                   'total', 'payment_type', 'delivery_fee',
                   'last_delivery_status', 'last_delivery_status_timestamp',
                   'transaction_timestamp', 'created_at_timestamp',
                   'assigned_timestamp', 'fc_reaction_timestamp',
                   'assignment_accuracy', 'accepted_timestamp',
                   'driver_reaction_timestamp', 'at_restaurant_timestamp',
                   'start_route_timestamp', 'real_pickup_timestamp',
                   'requested_pickup_timestamp', 'confirmed_pickup_timestamp',
                   'requested_delivery_timestamp',
                   'expected_delivery_timestamp', 'waiting_time',
                   'pick_up_eta', 'delivery_at_eta', 'cancellation_reason',
                   'assigned_by', 'cancelled_by', 'reassigned_by',
                   'unassigned_by', 'gastronomic_day', 'driver_uuid',
                   'air_distance_to_customer', 'customer_lat', 'customer_lng',
                   'sla_met'
                   )

# Legacy table: public.delivery, still used by most reports. The last field
# is the driver_uuid. SOon to be implemented as a view on the new table.
# New table: tableau.delivery, used by fewer reports, but contains all the
# fields

COUNT_EXISTING_SQL_PUBLIC = """
SELECT COUNT(*)
FROM public.delivery
WHERE delivery_uuid = %s;
"""

INSERT_SQL_PUBLIC = """
INSERT INTO public.delivery
(fleet, timezone, order_uuid, delivery_uuid,
delivery_short_id, source_name, source_id,
route_id, ordering_in_route, source_transaction_id,
restaurant_uuid, restaurant_name, restaurant_city,
restaurant_zipcode, restaurant_source_id,
driver_username, battery_status,
customer_phone_number, customer_street,
customer_number, customer_zipcode, customer_city,
customer_country, customer_raw_address,
distance_to_customer, distance_traveled_to_customer,
distance_to_restaurant, distance_traveled_to_restaurant,
total, payment_type, delivery_fee,
last_delivery_status, last_delivery_status_timestamp,
transaction_timestamp, created_at_timestamp,
assigned_timestamp, fc_reaction_timestamp,
assignment_accuracy, accepted_timestamp,
driver_reaction_timestamp, at_restaurant_timestamp,
start_route_timestamp, real_pickup_timestamp,
requested_pickup_timestamp, confirmed_pickup_timestamp,
requested_delivery_timestamp,
expected_delivery_timestamp, waiting_time,
pick_up_eta, delivery_at_eta, cancellation_reason,
assigned_by, cancelled_by, reassigned_by,
unassigned_by, gastronomic_day, driver_uuid)
VALUES (
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, date_trunc('day', %s), %s
);
"""

UPDATE_SQL_PUBLIC = """
UPDATE public.delivery SET
fleet = %s,
timezone = %s,
order_uuid = %s,
delivery_short_id = %s,
source_name = %s,
source_id = %s,
route_id = %s,
ordering_in_route = %s,
source_transaction_id = %s,
restaurant_uuid = %s,
restaurant_name = %s,
restaurant_city = %s,
restaurant_zipcode = %s,
restaurant_source_id = %s,
driver_username = %s,
battery_status = %s,
customer_phone_number = %s,
customer_street = %s,
customer_number = %s,
customer_zipcode = %s,
customer_city = %s,
customer_country = %s,
customer_raw_address = %s,
distance_to_customer = %s,
distance_traveled_to_customer = %s,
distance_to_restaurant = %s,
distance_traveled_to_restaurant = %s,
total = %s,
payment_type = %s,
delivery_fee = %s,
last_delivery_status = %s,
last_delivery_status_timestamp = %s,
transaction_timestamp = %s,
created_at_timestamp = %s,
assigned_timestamp = %s,
fc_reaction_timestamp = %s,
assignment_accuracy = %s,
accepted_timestamp = %s,
driver_reaction_timestamp = %s,
at_restaurant_timestamp = %s,
start_route_timestamp = %s,
real_pickup_timestamp = %s,
requested_pickup_timestamp = %s,
confirmed_pickup_timestamp = %s,
requested_delivery_timestamp = %s,
expected_delivery_timestamp = %s,
waiting_time = %s,
pick_up_eta = %s,
delivery_at_eta = %s,
cancellation_reason = %s,
assigned_by = %s,
cancelled_by = %s,
reassigned_by = %s,
unassigned_by = %s,
gastronomic_day = date_trunc('day', %s),
driver_uuid = %s
WHERE delivery_uuid = %s;
"""

COUNT_EXISTING_SQL_TABLEAU = """
SELECT COUNT(*)
FROM tableau.delivery
WHERE delivery_uuid = %s;
"""

INSERT_SQL_TABLEAU = """
INSERT INTO tableau.delivery
(fleet, timezone, order_uuid, delivery_uuid,
delivery_short_id, source_name, source_id,
route_id, ordering_in_route, source_transaction_id,
restaurant_uuid, restaurant_name, restaurant_city,
restaurant_zipcode, restaurant_source_id,
driver_username, battery_status,
customer_phone_number, customer_street,
customer_number, customer_zipcode, customer_city,
customer_country, customer_raw_address,
distance_to_customer, distance_traveled_to_customer,
distance_to_restaurant, distance_traveled_to_restaurant,
total, payment_type, delivery_fee,
last_delivery_status, last_delivery_status_timestamp,
transaction_timestamp, created_at_timestamp,
assigned_timestamp, fc_reaction_timestamp,
assignment_accuracy, accepted_timestamp,
driver_reaction_timestamp, at_restaurant_timestamp,
start_route_timestamp, real_pickup_timestamp,
requested_pickup_timestamp, confirmed_pickup_timestamp,
requested_delivery_timestamp,
expected_delivery_timestamp, waiting_time,
pick_up_eta, delivery_at_eta, cancellation_reason,
assigned_by, cancelled_by, reassigned_by,
unassigned_by, gastronomic_day, driver_uuid,

air_distance_to_customer, customer_lat,
customer_lng, sla_met, created_at_hour)
VALUES (
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, date_trunc('day', %s), %s,
%s, %s, %s, %s, date_part('hour', %s)
);
"""

UPDATE_SQL_TABLEAU = """
UPDATE tableau.delivery SET
fleet = %s,
timezone = %s,
order_uuid = %s,
delivery_short_id = %s,
source_name = %s,
source_id = %s,
route_id = %s,
ordering_in_route = %s,
source_transaction_id = %s,
restaurant_uuid = %s,
restaurant_name = %s,
restaurant_city = %s,
restaurant_zipcode = %s,
restaurant_source_id = %s,
driver_username = %s,
battery_status = %s,
customer_phone_number = %s,
customer_street = %s,
customer_number = %s,
customer_zipcode = %s,
customer_city = %s,
customer_country = %s,
customer_raw_address = %s,
distance_to_customer = %s,
distance_traveled_to_customer = %s,
distance_to_restaurant = %s,
distance_traveled_to_restaurant = %s,
total = %s,
payment_type = %s,
delivery_fee = %s,
last_delivery_status = %s,
last_delivery_status_timestamp = %s,
transaction_timestamp = %s,
created_at_timestamp = %s,
assigned_timestamp = %s,
fc_reaction_timestamp = %s,
assignment_accuracy = %s,
accepted_timestamp = %s,
driver_reaction_timestamp = %s,
at_restaurant_timestamp = %s,
start_route_timestamp = %s,
real_pickup_timestamp = %s,
requested_pickup_timestamp = %s,
confirmed_pickup_timestamp = %s,
requested_delivery_timestamp = %s,
expected_delivery_timestamp = %s,
waiting_time = %s,
pick_up_eta = %s,
delivery_at_eta = %s,
cancellation_reason = %s,
assigned_by = %s,
cancelled_by = %s,
reassigned_by = %s,
unassigned_by = %s,
gastronomic_day = date_trunc('day', %s),
driver_uuid = %s,

air_distance_to_customer = %s,
customer_lat = %s,
customer_lng = %s,
sla_met = %s,
created_at_hour = date_part('hour', %s)
WHERE delivery_uuid = %s;
"""


COUNT_EXISTING_SQL_DWH = """
SELECT COUNT(*)
FROM tableau.delivery_dwh
WHERE delivery_uuid = %s;
"""

INSERT_SQL_DWH = """
INSERT INTO tableau.delivery_dwh
(fleet_backend_name, timezone, order_uuid, delivery_uuid,
delivery_short_id, source_name, source_id,
route_id, ordering_in_route, source_transaction_id,
restaurant_uuid, restaurant_name, restaurant_city,
restaurant_zipcode, restaurant_source_id,
driver_username, battery_status,
customer_phone_number, customer_street,
customer_number, customer_zipcode, customer_city,
customer_country, customer_raw_address,
distance_to_customer, distance_traveled_to_customer,
distance_to_restaurant, distance_traveled_to_restaurant,
total, payment_type, delivery_fee,
last_delivery_status, last_delivery_status_timestamp,
transaction_timestamp, created_at_timestamp,
assigned_timestamp, fc_reaction_timestamp,
assignment_accuracy, accepted_timestamp,
driver_reaction_timestamp, at_restaurant_timestamp,
start_route_timestamp, real_pickup_timestamp,
requested_pickup_timestamp, confirmed_pickup_timestamp,
requested_delivery_timestamp,
expected_delivery_timestamp, waiting_time,
pick_up_eta, delivery_at_eta, cancellation_reason,
assigned_by, cancelled_by, reassigned_by,
unassigned_by, gastronomic_day, driver_uuid,

air_distance_to_customer, customer_lat,
customer_lng, sla_met, created_at_hour)
VALUES (
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, date_trunc('day', %s), %s,
%s, %s, %s, %s, date_part('hour', %s)
);
"""

UPDATE_SQL_DWH = """
UPDATE tableau.delivery_dwh SET
fleet_backend_name = %s,
timezone = %s,
order_uuid = %s,
delivery_short_id = %s,
source_name = %s,
source_id = %s,
route_id = %s,
ordering_in_route = %s,
source_transaction_id = %s,
restaurant_uuid = %s,
restaurant_name = %s,
restaurant_city = %s,
restaurant_zipcode = %s,
restaurant_source_id = %s,
driver_username = %s,
battery_status = %s,
customer_phone_number = %s,
customer_street = %s,
customer_number = %s,
customer_zipcode = %s,
customer_city = %s,
customer_country = %s,
customer_raw_address = %s,
distance_to_customer = %s,
distance_traveled_to_customer = %s,
distance_to_restaurant = %s,
distance_traveled_to_restaurant = %s,
total = %s,
payment_type = %s,
delivery_fee = %s,
last_delivery_status = %s,
last_delivery_status_timestamp = %s,
transaction_timestamp = %s,
created_at_timestamp = %s,
assigned_timestamp = %s,
fc_reaction_timestamp = %s,
assignment_accuracy = %s,
accepted_timestamp = %s,
driver_reaction_timestamp = %s,
at_restaurant_timestamp = %s,
start_route_timestamp = %s,
real_pickup_timestamp = %s,
requested_pickup_timestamp = %s,
confirmed_pickup_timestamp = %s,
requested_delivery_timestamp = %s,
expected_delivery_timestamp = %s,
waiting_time = %s,
pick_up_eta = %s,
delivery_at_eta = %s,
cancellation_reason = %s,
assigned_by = %s,
cancelled_by = %s,
reassigned_by = %s,
unassigned_by = %s,
gastronomic_day = date_trunc('day', %s),
driver_uuid = %s,

air_distance_to_customer = %s,
customer_lat = %s,
customer_lng = %s,
sla_met = %s,
created_at_hour = date_part('hour', %s)
WHERE delivery_uuid = %s;
"""

CLEANUP_SQL_DWH = """
UPDATE tableau.delivery_dwh
set last_delivery_status = 'cancelled',
cancellation_reason = 'Test Order'
where
(restaurant_name = 'New Box - automated tests'
or driver_username ILIKE 'demo%%'
or driver_username ILIKE '%%test%%'
or source_id ILIKE '%%test%%')
and cancellation_reason <> 'Test Order';
"""

ADD_FLEET_INFO_SQL_DWH = """
UPDATE tableau.delivery_dwh
SET fleet_display_name = tableau.fleet.display_name,
    fleet_uuid = tableau.fleet.uuid,
    fleet_country_code = tableau.fleet.country_code,
    fleet_country_name = tableau.fleet.country_name,
    fleet_city = tableau.fleet.city
FROM tableau.fleet
WHERE tableau.delivery_dwh.fleet_backend_name = tableau.fleet.backend_name;
"""


ONE_HOUR = datetime.timedelta(hours=1)
ONE_DAY = datetime.timedelta(days=1)
GASTRO_DATE_START_HOUR = datetime.time(hour=8)

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class ScriptError(Exception):
    pass


def parse_args():

    parser = argparse.ArgumentParser(
            description=('Get deliveries from backend and aggregates for '
                         'Tableau consumption'),
            epilog='')

    parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                        help='generates debug log file')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print debug logs on console')

    parser.add_argument('-u', '--username', action='store',
                        dest='username', required=True,
                        help='user to log into the backend')
    parser.add_argument('-p', '--password', action='store',
                        dest='password', required=True,
                        help='password to log into the backend')

    parser.add_argument('--dbuser', action='store',
                        dest='dbuser', required=True,
                        help='user to log into the DB')
    parser.add_argument('--dbpassword', action='store',
                        dest='dbpassword', required=True,
                        help='password to log into the DB')

    parsed_args = parser.parse_args()
    return parsed_args


def configure_logs(args):

    log_file_name = MODULE_NAME + '.log'
    debug_log_file_name = MODULE_NAME + '-debug.log'

    log_file_folder = LOG_DIR

    log_file_path = log_file_folder + '/' + log_file_name

    if args.debug:
        debug_log_file_path = log_file_folder + '/' + debug_log_file_name
    else:
        debug_log_file_path = None

    if args.verbose:
        logging_configurer.configure("", debug_log_file_path, log_file_path,
                                     logging.DEBUG)
    else:
        logging_configurer.configure("", debug_log_file_path, log_file_path,
                                     logging.WARNING)


def append_data(rows, backend_conn, fleets_dict, start_datetime,
                end_datetime):
    # iso_date = date.isoformat()
    # logger.info('Getting data for {}'.format(iso_date))
    for fleet_uuid in fleets_dict:
        fleet_name = fleets_dict[fleet_uuid]['name']
        if fleet_uuid in FLEETS_TO_SKIP:
            logger.info('Skipping fleet: {}'.format(fleet_name))
            continue

        logger.debug('Processing fleet: {}'.
                     format(fleet_name))
        url = (DELIVERIES_URL + '?' +
               urllib.parse.urlencode({'fleet': fleet_uuid,
                                       'start': start_datetime,
                                       'end': end_datetime}))

        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_conn.auth_token)
        logger.debug('Getting JSON data')
        try:
            with backend_conn.opener.open(request) as response:

                logger.debug('Retrieved JSON from this URL: {}'.
                             format(response.geturl()))

                # The response shouldn't be larger than 1 MiB
                reporting_json = response.read(1048576).decode('utf-8')

                if (response.read(1024) != b''):
                    raise ScriptError('Dowloaded JSON is larger than 1 MiB')
        except urllib.error.HTTPError as err:
            logger.warn('"{}" Error downloading data for Fleet = {} and '
                        'dates = {} to {}'.
                        format(err, fleet_name, start_datetime, end_datetime))
            continue

        data = json.loads(reporting_json)
        file_name = os.path.basename(data['filename'])
        logger.info('Retrieving file {}'.format(file_name))

        url = (REPORTS_URL + urllib.parse.quote(file_name))
        logger.debug('URL: ' + url)

        # fh = open(file_name, 'w')

        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_conn.auth_token)
        with backend_conn.opener.open(request) as response:

            logger.debug('Retrieved file from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 1 MiB
            payload = response.read(1048576).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded payload is larger than 1 MiB')

        with io.StringIO(payload) as f:
            csv_reader = csv.reader(f, dialect='excel')
            csv_fields = next(csv_reader)

            if tuple(csv_fields) != EXPECTED_FIELDS:
                logger.error('Expected fields : {}'.format(EXPECTED_FIELDS))
                logger.error('Input CSV fields: {}'.format(tuple(csv_fields)))
                raise ScriptError('The CSV does not contain the expected '
                                  'fields')

            num_rows = 0
            for row in csv_reader:
                num_rows += 1
                logger.debug('CSV created_at_timestamp={}, '
                             'gastronomic_day={}'.
                             format(row[34], row[55]))

                if len(row) == 0:
                    continue
                if len(row) != len(csv_fields):
                    raise ScriptError('The CSV contains rows with the wrong '
                                      'number of fields')
                # datetime
                for i in (32, 33, 34, 38, 40, 41, 42, 43, 44, 48, 49):
                    if row[i] == '':
                        row[i] = None
                    else:
                        row[i] = arrow.get(row[i]).datetime
                # int
                for i in (8, 25, 26, 27, 30):
                    if row[i] == '':
                        row[i] = None
                    else:
                        row[i] = int(row[i])
                # float
                for i in (16, 24, 28, 57, 58, 59):
                    if row[i] == '':
                        row[i] = None
                    else:
                        row[i] = float(row[i])
                # boolean
                for i in (60, ):
                    if row[i] == '':
                        row[i] = None
                    else:
                        row[i] = bool(row[i])

                # Fixing gastronomic_day issue here until it's fixed in backend
                created_at_timestamp = row[34]
                gastronomic_day = datetime.datetime(
                                        year=created_at_timestamp.year,
                                        month=created_at_timestamp.month,
                                        day=created_at_timestamp.day,
                                        tzinfo=created_at_timestamp.tzinfo)
                if created_at_timestamp.hour <= 7:
                    gastronomic_day -= ONE_DAY
                row[55] = gastronomic_day

                # Creating a copy of created_at_timestamp to ease insert
                # the create_at_hour
                row.append(row[34])

                logger.debug('Python created_at_timestamp={}, '
                             'gastronomic_day={}'.
                             format(row[34], row[55]))
                rows.append(row)

        logger.info('Retrieved {} row for fleet: {}'.
                    format(num_rows, fleet_name))

    logger.info('{} deliveries retrieved for all fleets'.format(len(rows)))
    return rows


def download_data(backend_conn, db_conn, start_date, end_date):

    logger.info('Getting fleet data')
    fleets_dict = fleets.download_data(backend_conn)

    logger.info('Begin download_data')

    gastro_date = start_date

    while gastro_date <= end_date:
        logger.info('Gastro date = {}'.format(gastro_date))
        start_datetime = datetime.datetime.combine(gastro_date,
                                                   GASTRO_DATE_START_HOUR)
        for _ in range(24):
            end_datetime = start_datetime + ONE_HOUR
            logger.info('Downloading data from {} to {}'.
                        format(start_datetime, end_datetime))

            rows = []
            append_data(rows, backend_conn, fleets_dict,
                        start_datetime, end_datetime)
            insert_in_db_tableau(db_conn, rows)
            insert_in_db_dwh(db_conn, rows)
            insert_in_db_public(db_conn, rows)

            start_datetime = end_datetime
        gastro_date += ONE_DAY

    return


def insert_in_db_public(conn, rows):

    cur = conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for row in rows:
        row = row[:57]  # Adapt row to the old table format
        if processed > 0 and processed % 100 == 0:
            logger.info('{} inserted and {} updated rows so far, committing'.
                        format(inserted, updated))
            conn.commit()
            logger.info('Committed. Continuing...')

        # row[3:4] is a list with just the delivery_uuid
        cur.execute(COUNT_EXISTING_SQL_PUBLIC, row[3:4])
        result = cur.fetchone()
        if int(result[0]) == 0:
            cur.execute(INSERT_SQL_PUBLIC, row)
            inserted += 1
        elif int(result[0]) == 1:
            # The row manipulation puts delivery_uuid last to match the UPDATE
            # query format
            cur.execute(UPDATE_SQL_PUBLIC, row[:3] + row[4:] + row[3:4])
            updated += 1
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))
        processed += 1

    logger.info('{} inserted and {} updated rows in this batch'.
                format(inserted, updated))
    conn.commit()
    cur.close()


def insert_in_db_tableau(conn, rows):

    cur = conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for row in rows:
        if processed > 0 and processed % 100 == 0:
            logger.info('{} inserted and {} updated rows so far, committing'.
                        format(inserted, updated))
            conn.commit()
            logger.info('Committed. Continuing...')

        # row[3:4] is a list with just the delivery_uuid
        cur.execute(COUNT_EXISTING_SQL_TABLEAU, row[3:4])
        result = cur.fetchone()
        if int(result[0]) == 0:
            cur.execute(INSERT_SQL_TABLEAU, row)
            inserted += 1
        elif int(result[0]) == 1:
            # The row manipulation puts delivery_uuid last to match the UPDATE
            # query format
            cur.execute(UPDATE_SQL_TABLEAU, row[:3] + row[4:] + row[3:4])
            updated += 1
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))
        processed += 1

    logger.info('{} inserted and {} updated rows in this batch'.
                format(inserted, updated))
    conn.commit()
    cur.close()


def insert_in_db_dwh(conn, rows):

    cur = conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for row in rows:
        if processed > 0 and processed % 100 == 0:
            logger.info('{} inserted and {} updated rows so far, committing'.
                        format(inserted, updated))
            conn.commit()
            logger.info('Committed. Continuing...')

        # row[3:4] is a list with just the delivery_uuid
        cur.execute(COUNT_EXISTING_SQL_DWH, row[3:4])
        result = cur.fetchone()
        if int(result[0]) == 0:
            cur.execute(INSERT_SQL_DWH, row)
            inserted += 1
        elif int(result[0]) == 1:
            # The row manipulation puts delivery_uuid last to match the UPDATE
            # query format
            cur.execute(UPDATE_SQL_DWH, row[:3] + row[4:] + row[3:4])
            updated += 1
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))
        processed += 1

    logger.info('{} inserted and {} updated rows in this batch'.
                format(inserted, updated))
    conn.commit()

    cur.close()


def enrich_dwh_table(conn):
    cur = conn.cursor()
    logger.info('Flagging test orders')
    cur.execute(CLEANUP_SQL_DWH)
    conn.commit()

    logger.info('Adding fleet info')
    cur.execute(ADD_FLEET_INFO_SQL_DWH)
    conn.commit()


def main(parsed_args):

    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=1)
    logger.debug('Start date = {}, end date = {}'.format(start_date, end_date))

    backend_conn = BackendConnector()
    backend_conn.authenticate(parsed_args.username, parsed_args.password)

    db_conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                                "dbname='valk_fleet' user='{}' "
                                "password='{}'").
                               format(parsed_args.dbuser,
                                      parsed_args.dbpassword))

    download_data(backend_conn, db_conn, start_date, end_date)
    enrich_dwh_table(db_conn)

    sys.stdout.flush()
    # Flushing is not enough (at least inside Eclipse)
    time.sleep(.1)


if __name__ == '__main__':

    parsed_args = parse_args()
    configure_logs(parsed_args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(parsed_args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
