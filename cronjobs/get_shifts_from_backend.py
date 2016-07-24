'''
Created on Jul 28, 2015

@author: nicolasguenon
'''
import argparse
import csv
import datetime
import io
import json
import logging
import math
import os
import pprint
import sys
import time
import urllib.request

import arrow
import pandas as pd
import psycopg2

from common.logging_configurer import LOG_DIR
from scripts import get_fleets_from_backend as fleets
from connectors.backend import BackendConnector
from common import logging_configurer

MODULE_NAME = 'get_shifts_from_backend'
LOGIN_URL = 'https://api.valkfleet.com/login'
FLEETS_URL = 'https://api.valkfleet.com/fleets/'
SHIFTS_URL = 'https://api.valkfleet.com/reporting/shifts/'
REPORTS_URL = 'https://api.valkfleet.com/reporting/'

FLEETS_TO_SKIP = {
    '20be27c1-bf0d-423b-9bb2-59b4c2a10414': 'golden_grill',
    '261fadd0-0ada-4f20-9cb9-ce3d2bcc95f7': 'narnia',
    '90467dd4-ef75-4b38-8be9-0f0dcc84ec1b': 'nottingham_not_yet',
    '954a7525-54da-4b6c-8d6a-634e65a8944d': 'uk_not_yet',
    'a3d6c82b-ca57-4d5c-8f1e-a9c8806aa375': 'dragon_palace',
    'cc292253-54bf-4904-8ea4-c740c32b47f3': 'chunky_chicken',
    'f42e0a6d-eced-42aa-bbc7-a5885eee9e1b': 'vienna',
    'ecfb3947-6908-4998-be81-0e98af1af21e': 'currylicious',

    'd07eefe7-d971-495d-90c3-913bd03a2b66': 'Turin'
    }

EXPECTED_FIELDS = ('driver_username', 'on_shift_timestamp',
                   'off_shift_timestamp', 'backend_closed_shift',
                   'app_version', 'hours_worked', 'on_break_timestamp',
                   'off_break_timestamp', 'no_of_breaks', 'break_time')

DB_FIELDS = ('actual_datetime', 'total_active_drivers')

COUNT_EXISTING_SQL = """SELECT COUNT(*)
                        FROM tableau.backend_hourly_drivers
                        WHERE gastronomic_day = %s
                            AND hour = %s
                            AND fleet_uuid = %s;"""
INSERT_SQL = """INSERT INTO tableau.backend_hourly_drivers
                   (gastronomic_day, hour, fleet_uuid, actual_datetime,
                   total_active_drivers)
                   VALUES (%s, %s, %s, %s, %s);"""
UPDATE_SQL = """UPDATE tableau.backend_hourly_drivers
                   SET total_active_drivers = %s
                   WHERE gastronomic_day = %s
                       AND hour = %s
                       AND fleet_uuid = %s;"""

UPDATE_FLEET_INFO_SQL = """
UPDATE tableau.backend_hourly_drivers
SET fleet_display_name = tableau.fleet.display_name,
    fleet_backend_name = tableau.fleet.backend_name,
    fleet_country_code = tableau.fleet.country_code,
    fleet_country_name = tableau.fleet.country_name
FROM tableau.fleet
WHERE tableau.backend_hourly_drivers.fleet_uuid = tableau.fleet.uuid;
"""

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class ScriptError(Exception):
    pass


def parse_args():

    parser = argparse.ArgumentParser(
            description='Get shifts from backend and aggregates for Tableau',
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


def append_data(rows, backend_connector, fleets_dict, date):
    iso_date = date.isoformat()
    logger.info('Getting data for {}'.format(iso_date))
    for fleet in fleets_dict:
        if fleet in FLEETS_TO_SKIP:
            continue
        fleet_name = fleets_dict[fleet]['name']
        logger.debug('Processing fleet: {}'.format(fleet_name))
        url = (SHIFTS_URL + '?' +
               urllib.parse.urlencode({'fleet': fleet,
                                       'date': iso_date}))

        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_connector.auth_token)
        logger.debug('Getting JSON data')
        try:
            with backend_connector.opener.open(request) as response:

                logger.debug('Retrieved JSON from this URL: {}'.
                             format(response.geturl()))

                # The response shouldn't be larger than 1 MiB
                fleet_json = response.read(1048576).decode('utf-8')

                if (response.read(1024) != b''):
                    raise ScriptError('Dowloaded JSON is larger than 1 MiB')
        except urllib.error.HTTPError as err:
            logger.warn('Error downloading fleet data for Fleet = {} and '
                        'date = {}. Error message: {}'.
                        format(fleet_name, iso_date, err))
            continue

        data = json.loads(fleet_json)
        logger.debug("Got data:\n{}".format(pp.pformat(fleet_json)))

        file_name = os.path.basename(data['filename'])
        logger.info('Saving file {}'.format(file_name))

        url = (REPORTS_URL + urllib.parse.quote(file_name))
        logger.debug('URL: ' + url)

        # fh = open(file_name, 'w')

        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_connector.auth_token)
        logger.debug('Getting JSON data')
        with backend_connector.opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
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

            for row in csv_reader:

                if len(row) == 0:
                    continue
                if len(row) != len(csv_fields):
                    raise ScriptError('The CSV contains rows with the wrong '
                                      'number of fields')
                row_dict = dict(zip(csv_fields, row))
                for key in row_dict:
                    row_dict[key] = row_dict[key].strip()
                row_dict['fleet_uuid'] = fleet
                # logger.debug('Appending row {}'.format(row_dict))
                rows.append(row_dict)

    logger.info('{} shifts retrieved'.format(len(rows)))
    return rows


def download_data(username, password, start_date, end_date):

    backend_connector = BackendConnector()
    backend_connector.authenticate(username, password)
    fleets_dict = fleets.download_data(backend_connector)

    logger.info('Begin download_data')

    date = start_date
    one_day = datetime.timedelta(days=1)

    rows = []
    while date <= end_date:
        append_data(rows, backend_connector, fleets_dict, date)
        date += one_day

    return (fleets_dict, rows)


def create_output_df(start_date, end_date, fleets_dict):

    one_day = datetime.timedelta(days=1)
    one_hour = datetime.timedelta(hours=1)
    hours = [_ for _ in range(8, 24)]
    hours.extend([_ for _ in range(8)])

    fleet_uuids = list(fleets_dict.keys())
    for fleet_uuid in FLEETS_TO_SKIP:
        fleet_uuids.remove(fleet_uuid)
    num_fleets = len(fleet_uuids)
    gastro_datetimes = []
    real_datetimes = []
    init_values = []
    current_gastro_date = start_date  # dates are immutable
    while current_gastro_date <= end_date:
        current_real_datetime = datetime.datetime(current_gastro_date.year,
                                                  current_gastro_date.month,
                                                  current_gastro_date.day,
                                                  8)

        for hour in hours:
            current_gastro_datetime = datetime.datetime(
                            current_gastro_date.year,
                            current_gastro_date.month,
                            current_gastro_date.day,
                            hour)
            gastro_datetimes.append(current_gastro_datetime)
            for _ in range(num_fleets):
                real_datetimes.append(current_real_datetime)
                # TODO get rid of this ugly thing
                init_values.append(0.0)

            current_real_datetime += one_hour
        current_gastro_date += one_day

    index = pd.MultiIndex.from_product((gastro_datetimes, fleet_uuids),
                                       names=['gastronomic_datetime',
                                              'fleet'])

    output_df = pd.DataFrame(index=index, columns=DB_FIELDS)
    output_df['actual_datetime'] = real_datetimes
    output_df['total_active_drivers'] = init_values

    logger.info('Output DataFrame created')
    logger.debug('Head:\n{}'.format(output_df.head(4)))
    logger.debug('Tail:\n{}'.format(output_df.tail(4)))

    return output_df


def aggregate_data(start_date, end_date, fleets_dict, rows):
    output_df = create_output_df(start_date, end_date, fleets_dict)

    for i in range(len(rows)):
        if i % 100 == 0:
            logger.info('Read {} input rows so far'.format(i))

        row = rows[i]
        try:
            start = arrow.get(row['on_shift_timestamp']).datetime
        except arrow.parser.ParserError as err:
            logger.error('on_shift_timestamp parse error "{}" in row: {}'.
                         format(err, row))
        try:
            end = arrow.get(row['off_shift_timestamp']).datetime
        except:
            end = start.replace(hour=7, minute=59, second=59, microsecond=9999)
        fleet_uuid = row['fleet_uuid']

        start_hour_contribution = (60 - start.minute) / 60
        end_hour_contribution = end.minute / 60
        # logger.debug(start.hour, start.minute, start_hour_contribution)
        # logger.debug(end.hour, end.minute, end_hour_contribution)

        gastro_date = datetime.date(start.year, start.month, start.day)
        if start.hour < 8:
            gastro_date -= datetime.timedelta(days=1)

        if end.hour < start.hour:
            hours = range(start.hour, end.hour + 25)
        else:
            hours = range(start.hour, end.hour + 1)
        for hour in hours:
            hour %= 24
            gastro_datetime = datetime.datetime(gastro_date.year,
                                                gastro_date.month,
                                                gastro_date.day,
                                                hour)

            try:
                current_drivers = output_df.loc[(gastro_datetime, fleet_uuid),
                                                'total_active_drivers']
            except:
                logger.error('Error with {}, {}'.format(gastro_datetime,
                                                        fleet_uuid))
                continue
            if math.isnan(current_drivers):
                current_drivers = 0.0
            if hour == start.hour:
                current_drivers += start_hour_contribution
            elif hour == end.hour:
                current_drivers += end_hour_contribution
            else:
                current_drivers += 1.0
            output_df.loc[(gastro_datetime, fleet_uuid),
                          'total_active_drivers'] = current_drivers
            # logger.debug((gastro_date, hour, fleet, current_drivers))

    logger.info(output_df.tail(24))
    return output_df


def insert_in_db(df, username, password):

    #conn = psycopg2.connect(("host='localhost' "
    conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                             "dbname='valk_fleet' user='{}' "
                             "password='{}'").format(username, password))
    cur = conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for i in df.index:
        if processed > 0 and processed % 100 == 0:
            logger.info('{} inserted and {} updated rows so far, committing'.
                        format(inserted, updated))
            conn.commit()
            logger.info('Committed. Continuing...')

        row = df.loc[i]
        gastro_datetime = i[0]
        fleet_uuid = i[1]
        hour = gastro_datetime.hour
        gastro_date = datetime.date(gastro_datetime.year,
                                    gastro_datetime.month,
                                    gastro_datetime.day)

        cur.execute(COUNT_EXISTING_SQL, (gastro_date, hour, fleet_uuid))
        result = cur.fetchone()
        if int(result[0]) == 0:
            inserted += 1
            cur.execute(INSERT_SQL,
                        (gastro_date, hour, fleet_uuid, row['actual_datetime'],
                         row['total_active_drivers']))
        elif int(result[0]) == 1:
            updated += 1
            cur.execute(UPDATE_SQL,
                        (row['total_active_drivers'],
                         gastro_date, hour, fleet_uuid))
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))
        processed += 1

    conn.commit()

    cur.execute(UPDATE_FLEET_INFO_SQL)
    conn.commit()
    cur.close()
    conn.close()


def main(parsed_args):
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=5)
    logger.debug('Start date = {}, end date = {}'.format(start_date, end_date))

    (fleets_dict, rows) = download_data(parsed_args.username,
                                        parsed_args.password,
                                        start_date, end_date)

    df = aggregate_data(start_date, end_date, fleets_dict, rows)
    insert_in_db(df, parsed_args.dbuser, parsed_args.dbpassword)

    # print(pp.pprint(fleets[0]))
    # generate_csv(fleets)
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
