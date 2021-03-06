'''
Created on Sep 23, 2015

@author: nicolasguenon
'''
import argparse
import csv
import datetime
import http.cookiejar
import io
import json
import logging
import math
import numpy as np
import pandas as pd
import pprint
import psycopg2
import sys
import time
import urllib.request
from common import logging_configurer
from locale import currency
from numpy import nan, NaN
from numpy.distutils.lib2def import output_def
from time import sleep
from pandas.tseries.offsets import Hour

# See http://pandas.pydata.org/pandas-docs/stable/indexing.html#indexing-view-versus-copy
from common.logging_configurer import LOG_DIR

pd.set_option('mode.chained_assignment', 'raise')

MODULE_NAME = 'sync_shift_data'
GASTRO_DAY_FIRST_HOUR = 8

FLEETS = ('Basildon', 'Birmingham', 'Manchester', 'Nottingham')
GROUP_TO_FLEET = {'Valk Drivers Basildon': 'Basildon',
                  'Valk Drivers Birmingham': 'Birmingham',
                  'Valk Drivers Manchester': 'Manchester',
                  'Valk Drivers Nottingham': 'Nottingham',
                  'Valk Team Leader Nottingham': 'Nottingham'}
GROUP_TO_CAPTAIN = {'Valk Drivers Basildon': False,
                    'Valk Drivers Birmingham': False,
                    'Valk Drivers Manchester': False,
                    'Valk Drivers Nottingham': False,
                    'Valk Team Leader Nottingham': True}
SALARY_TO_ON_CALL = {1201: False,
                     'UK Driver Half Salary': True}
DB_FIELDS = ('actual_datetime', 'regular_drivers', 'team_leaders',
             'total_active_drivers', 'on_call_drivers',
             'total_drivers')
DB_REAL_FIELDS = ('regular_drivers', 'team_leaders',
                  'total_active_drivers', 'on_call_drivers',
                  'total_drivers')
COUNT_EXISTING_SQL = """SELECT COUNT(*)
                        FROM tableau.planday_hourly_drivers
                        WHERE gastronomic_date = %s
                            AND hour = %s
                            AND fleet = %s;"""
INSERT_SQL = """INSERT INTO tableau.planday_hourly_drivers
                   (gastronomic_date, hour, fleet, actual_datetime,
                   regular_drivers, team_leaders, total_active_drivers,
                   on_call_drivers, total_drivers)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
UPDATE_SQL = """UPDATE tableau.planday_hourly_drivers
                   SET regular_drivers = %s,
                       team_leaders = %s,
                       total_active_drivers = %s,
                       on_call_drivers = %s,
                       total_drivers = %s
                   WHERE gastronomic_date = %s
                       AND hour = %s
                       AND fleet = %s;"""

file_path = ('/Users/nicolasguenon/Downloads/' +
             'Dynamic_31-08-2015_04-10-2015_(CQ58BQ).xls')
            # 'Dynamic_01-01-2015_04-10-2015_(U70K3K).xls')

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class ScriptError(Exception):
    pass


def parse_args():

    parser = argparse.ArgumentParser(
            description='Get restaurants from backend and outputs them as CSV',
            epilog='')

    parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                        help='generates debug log file')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                        help='print debug logs on console')

#     parser.add_argument('-u', '--username', action='store',
#                         dest='username', required=True,
#                         help='user to log into the backend')
#     parser.add_argument('-p', '--password', action='store',
#                         dest='password', required=True,
#                         help='password to log into the backend')

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

    logger.debug('Script args={}'.format(args))

    # print(pp.pprint(restaurants[0]))


def create_output_df():
    #start_date = datetime.date(2015, 4, 6)
    start_date = datetime.date(2015, 8, 31)
    end_date = datetime.date(2015, 10, 4)
    one_day = datetime.timedelta(days=1)
    one_hour = datetime.timedelta(hours=1)
    hours = [_ for _ in range(8, 24)]
    hours.extend([_ for _ in range(8)])
    print(hours)

    dates = []
    real_datetimes = []
    init_values = []
    current_gastro_date = start_date  # dates are immutable
    while current_gastro_date <= end_date:
        dates.append(current_gastro_date)
        current_real_datetime = datetime.datetime(current_gastro_date.year,
                                                  current_gastro_date.month,
                                                  current_gastro_date.day,
                                                  8)

        for hour in hours:
            for fleet in FLEETS:
                real_datetimes.append(current_real_datetime)
                # TODO get rid of this ugly thing
                init_values.append(0.0)

            current_real_datetime += one_hour
        current_gastro_date += one_day

    index = pd.MultiIndex.from_product((dates, hours, FLEETS),
                                       names=['gastronomic_date',
                                              'hour', 'fleet'])

    output_df = pd.DataFrame(index=index, columns=DB_FIELDS)
    output_df['actual_datetime'] = real_datetimes
    for field_name in DB_REAL_FIELDS:
        output_df[field_name] = init_values

    # print(input_df.head(3))
    # print(output_df.head(11))
    # print(output_df.tail(11))

    return output_df


def read_data():
    input_df = pd.read_excel(file_path, sheet_name='Valkfleet')
    output_df = create_output_df()

    for i in input_df.index:
        print(i)
        if i % 100 == 0:
            print('Read {} input rows so far'.format(i))

        row = input_df.ix[i]
        start = row['start date']
        end = row['End date']
        role = row['Employee Group (role)']
        if role in ('sdjfjds', 'Month salary'):
            continue
        salary_type = row['Salary type']
        if salary_type == 1:
            continue

        fleet = GROUP_TO_FLEET[role]

        captain = GROUP_TO_CAPTAIN[role]
        on_call = SALARY_TO_ON_CALL[salary_type]

        if on_call:
            relevant_field = 'on_call_drivers'
        elif captain:
            relevant_field = 'team_leaders'
        else:
            relevant_field = 'regular_drivers'

        # logger.debug((start, end, role, fleet, captain, on_call))

        start_hour_contribution = (60 - start.minute) / 60
        end_hour_contribution = end.minute / 60
        # logger.debug(start.hour, start.minute, start_hour_contribution)
        # logger.debug(end.hour, end.minute, end_hour_contribution)

        gastro_date = datetime.date(start.year, start.month, start.day)
        print(gastro_date)

        if end.hour < start.hour:
            hours = range(start.hour, end.hour + 25)
        else:
            hours = range(start.hour, end.hour + 1)
        for hour in hours:
            hour %= 24
            current_drivers = output_df.loc[(gastro_date, hour, fleet),
                                            relevant_field]
            if math.isnan(current_drivers):
                current_drivers = 0.0
            if hour == start.hour:
                current_drivers += start_hour_contribution
            elif hour == end.hour:
                current_drivers += end_hour_contribution
            else:
                current_drivers += 1.0
            output_df.loc[(gastro_date, hour, fleet),
                          relevant_field] = current_drivers
            # logger.debug((gastro_date, hour, fleet, current_drivers))

    print('Computing totals')
    output_df['total_active_drivers'] = (output_df['regular_drivers'] +
                                         output_df['team_leaders'])
    output_df['total_drivers'] = (output_df['total_active_drivers'] +
                                  output_df['on_call_drivers'])
    print(output_df.head(11))
    print(output_df.tail(11))
    return output_df


def insert_in_db(df, username, password):

    conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                             "dbname='valk_fleet' user='{}' "
                             "password='{}'").format(username, password))
    cur = conn.cursor()

    processed = 0
    inserted = 0
    updated = 0
    for i in df.index:
        processed += 1
        if processed % 100 == 0:
            print('{} inserted and {} updated rows so far, committing...'.
                  format(inserted, updated))
            conn.commit()
            print('Committed. Continuing...')

        row = df.loc[i]
        gastronomic_date = i[0]
        hour = i[1]
        fleet = i[2]

        cur.execute(COUNT_EXISTING_SQL, i)
        result = cur.fetchone()
        if int(result[0]) == 0:
            cur.execute(INSERT_SQL,
                        (gastronomic_date, hour, fleet, row['actual_datetime'],
                         row['regular_drivers'], row['team_leaders'],
                         row['total_active_drivers'], row['on_call_drivers'],
                         row['total_drivers']))
        elif int(result[0]) == 1:
            cur.execute(UPDATE_SQL,
                        (row['regular_drivers'], row['team_leaders'],
                         row['total_active_drivers'], row['on_call_drivers'],
                         row['total_drivers'],
                         gastronomic_date, hour, fleet))
        else:
            raise ScriptError('SQL COUNT() returned {}'.format(result[0]))

    conn.commit()
    cur.close()
    conn.close()


def main(parsed_args):
    read_data()
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
