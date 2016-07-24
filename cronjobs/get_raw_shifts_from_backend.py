'''
Copies shift data from backend into tableau.backend_shift table and enriches
it with data from the tableau.fleet and tableau.driver_rosetta tables
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

import psycopg2

from common.logging_configurer import LOG_DIR
from scripts import get_fleets_from_backend as fleets
from connectors.backend import BackendConnector
from common import logging_configurer

MODULE_NAME = 'get_raw_shifts_from_backend'
SHIFTS_URL = 'https://api.valkfleet.com/reporting/shifts/'
REPORTS_URL = 'https://api.valkfleet.com/reporting/'

EXPECTED_FIELDS = ('driver_username', 'on_shift_timestamp',
                   'off_shift_timestamp', 'backend_closed_shift',
                   'app_version', 'hours_worked', 'on_break_timestamp',
                   'off_break_timestamp', 'no_of_breaks', 'break_time')

DB_FIELDS = ('actual_datetime', 'total_active_drivers')

INSERT_SQL = """
INSERT INTO tableau.backend_shift
(
  fleet_uuid,
  driver_username,
  on_shift_timestamp,
  off_shift_timestamp,
  backend_closed_shift,
  app_version,
  hours_worked,
  on_break_timestamp,
  off_break_timestamp,
  no_of_breaks,
  break_time
)
VALUES (
%s, %s, %s, %s, %s,
%s, %s, %s, %s, %s,
%s);
"""

COUNT_EXISTING_SQL = """
select count(*)
from tableau.backend_shift
WHERE
  driver_username = %s
  AND on_shift_timestamp = %s
;
"""

UPDATE_SQL = """
UPDATE tableau.backend_shift
SET
  fleet_uuid = %s,
  off_shift_timestamp = %s,
  backend_closed_shift = %s,
  app_version = %s,
  hours_worked = %s,
  on_break_timestamp = %s,
  off_break_timestamp = %s,
  no_of_breaks = %s,
  break_time = %s
WHERE
  driver_username = %s
  AND on_shift_timestamp = %s
;"""

UPDATE_FLEET_INFO_SQL = """
UPDATE tableau.backend_shift
SET fleet_display_name = tableau.fleet.display_name,
    fleet_backend_name = tableau.fleet.backend_name,
    fleet_country_code = tableau.fleet.country_code,
    fleet_country_name = tableau.fleet.country_name,
    fleet_city = tableau.fleet.city
FROM tableau.fleet
WHERE tableau.backend_shift.fleet_uuid = tableau.fleet.uuid;
"""

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class ScriptError(Exception):
    pass


def parse_args():

    parser = argparse.ArgumentParser(
            description='Get shifts from backend and enriches with fleet data',
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


def download_data(backend_conn, fleets_dict, date):
    rows = []
    iso_date = date.isoformat()
    logger.info('Getting data for {}'.format(iso_date))
    for fleet in fleets_dict:
        fleet_name = fleets_dict[fleet]['name']
        logger.debug('Processing fleet: {}'.format(fleet_name))
        url = (SHIFTS_URL + '?' +
               urllib.parse.urlencode({'fleet': fleet,
                                       'date': iso_date}))

        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_conn.auth_token)
        logger.debug('Getting JSON data')
        try:
            with backend_conn.opener.open(request) as response:

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
        request.add_header('Authorization', backend_conn.auth_token)
        logger.debug('Getting JSON data')
        with backend_conn.opener.open(request) as response:

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

                for key in row_dict:
                    if row_dict[key] == '':
                        row_dict[key] = None
                # logger.debug('Appending row {}'.format(row_dict))
                rows.append(row_dict)

    logger.info('{} shifts retrieved'.format(len(rows)))
    return rows


def insert_in_db(db_conn, rows):
    cur = db_conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for row in rows:
        if processed > 0 and processed % 100 == 0:
            logger.info('{} inserted and {} updated rows so far, committing'.
                        format(inserted, updated))
            db_conn.commit()
            logger.info('Committed. Continuing...')

        cur.execute(COUNT_EXISTING_SQL, (row['driver_username'],
                                         row['on_shift_timestamp']))
        result = cur.fetchone()
        if int(result[0]) == 0:
            inserted += 1
            cur.execute(INSERT_SQL,
                        (row['fleet_uuid'],
                         row['driver_username'],
                         row['on_shift_timestamp'],
                         row['off_shift_timestamp'],
                         row['backend_closed_shift'],
                         row['app_version'],
                         row['hours_worked'],
                         row['on_break_timestamp'],
                         row['off_break_timestamp'],
                         row['no_of_breaks'],
                         row['break_time']))
        elif int(result[0]) == 1:
            updated += 1
            cur.execute(UPDATE_SQL,
                        (row['fleet_uuid'],
                         row['off_shift_timestamp'],
                         row['backend_closed_shift'],
                         row['app_version'],
                         row['hours_worked'],
                         row['on_break_timestamp'],
                         row['off_break_timestamp'],
                         row['no_of_breaks'],
                         row['break_time'],
                         row['driver_username'],
                         row['on_shift_timestamp']))
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))
        processed += 1

    db_conn.commit()
    cur.close()


def main(parsed_args):
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=1)
    logger.debug('Start date = {}, end date = {}'.format(start_date, end_date))

    backend_conn = BackendConnector()
    backend_conn.authenticate(parsed_args.username, parsed_args.password)

    # db_conn = psycopg2.connect(("host='localhost' "
    db_conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                                "dbname='valk_fleet' user='{}' "
                                "password='{}'").
                               format(parsed_args.dbuser,
                                      parsed_args.dbpassword))

    logger.info('Getting fleet data')
    fleets_dict = fleets.download_data(backend_conn)

    date = start_date
    one_day = datetime.timedelta(days=1)
    while date <= end_date:
        rows = download_data(backend_conn, fleets_dict, date)
        insert_in_db(db_conn, rows)
        date += one_day

    cur = db_conn.cursor()
    cur.execute(UPDATE_FLEET_INFO_SQL)
    db_conn.commit()
    cur.close()
    db_conn.close()
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
