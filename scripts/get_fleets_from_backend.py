'''
Created on Jul 28, 2015

@author: nicolasguenon
'''
import argparse
import csv
import io
import json
import logging
import pprint
import sys
import time
import urllib.request

from common.logging_configurer import configure_default, LOG_DIR
from connectors.backend import BackendConnector
from common import logging_configurer

MODULE_NAME = 'get_fleets_from_backend'
LOGIN_URL = 'https://api.valkfleet.com/login'
FLEETS_URL = 'https://api.valkfleet.com/fleets/'
# RESTAURANTS_URL = 'http://localhost:8080/restaurants/'

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)


class ScriptError(Exception):
    pass


def _parse_args():

    parser = argparse.ArgumentParser(
            description='Get fleets from backend and outputs them as CSV',
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

    parsed_args = parser.parse_args()
    return parsed_args


def _configure_logs(args):

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


def download_data(backend_connector):

    logger.info('Getting the list of fleets UUIDs')

    more = True
    fleet_uuids = []
    cursor = None
    while more:
        if cursor is None:
            url = FLEETS_URL
        else:
            # request = API_DRIVERS_URL + '?cursor={}'.format(cursor)
            url = (FLEETS_URL + '?' +
                   urllib.parse.urlencode({'cursor': cursor}))
            logger.debug('URL: ' + url)

        print(url)
        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_connector.auth_token)
        logger.debug('Getting JSON data')
        with backend_connector.opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 1 MiB
            fleet_json = response.read(1048576).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded JSON is larger than 1 MiB')

        data = json.loads(fleet_json)
        fleet_uuids.extend(data['items'])
        more = data['more']
        cursor = data['cursor']

    logger.info('Getting all fleet data for each fleet')

    fleets_dict = {}
    for fleet_uuid in fleet_uuids:
        url = (FLEETS_URL + urllib.parse.quote_plus(fleet_uuid))
        logger.debug('URL: ' + url)
        print(url)
        request = urllib.request.Request(url)
        request.add_header('Authorization', backend_connector.auth_token)
        logger.debug('Getting JSON data for fleet = {}'.format(fleet_uuid))
        with backend_connector.opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 1 MiB
            fleet_json = response.read(1048576).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded JSON is larger than 1 MiB')

        data = json.loads(fleet_json)
        # Structure of each "data" dict:

        # {   'cc_phone': '0800 612 6220',
        #     'created_at': '2015-10-28T10:19:25+00:00',
        #     'deleted_at': None,
        #     'drivers': [],
        #     'modified_at': '2015-11-02T16:28:11+00:00',
        #     'name': 'leeds',
        #     'restaurants': [],
        #     'uuid': '0f7d2ef4-e397-4979-8051-0057becfac23'}

        fleets_dict[fleet_uuid] = data
    return fleets_dict


def generate_csv(fleets_dict):

    with io.StringIO(newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('Fleet Name', 'Fleet UUID', 'Customer Care phone',
                         'created_at', 'modified_at', 'deleted_at'))
        for fleet_uuid in fleets_dict:
            fleet = fleets_dict[fleet_uuid]
            writer.writerow((fleet['name'],
                             fleet['uuid'],
                             fleet['cc_phone'],
                             fleet['created_at'],
                             fleet['modified_at'],
                             fleet['deleted_at']))
        csv_str = f.getvalue()
    return csv_str


def generate_drivers_csv(fleets_dict):

    with io.StringIO(newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('Fleet Name', 'Fleet UUID', 'Driver UUID'))
        for fleet_uuid in fleets_dict:
            fleet = fleets_dict[fleet_uuid]
            for driver_uuid in fleet['drivers']:
                writer.writerow((fleet['name'],
                                 fleet['uuid'],
                                 driver_uuid))
        csv_str = f.getvalue()
    return csv_str


def generate_restaurants_csv(fleets_dict):

    with io.StringIO(newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('Fleet Name', 'Fleet UUID', 'Restaurant UUID'))
        for fleet_uuid in fleets_dict:
            fleet = fleets_dict[fleet_uuid]
            for restaurant_uuid in fleet['restaurants']:
                writer.writerow((fleet['name'],
                                 fleet['uuid'],
                                 restaurant_uuid))
        csv_str = f.getvalue()
    return csv_str


def main(parsed_args):
    backend_connector = BackendConnector()
    backend_connector.authenticate(parsed_args.username, parsed_args.password)
    fleets_dict = download_data(backend_connector)
    print('\nFleets')
    print(generate_csv(fleets_dict))
    print('\nDrivers')
    print(generate_drivers_csv(fleets_dict))
    print('\nRestaurants')
    print(generate_restaurants_csv(fleets_dict))
    sys.stdout.flush()
    # Flushing is not enough (at least inside Eclipse)
    time.sleep(.1)


if __name__ == '__main__':

    parsed_args = _parse_args()
    _configure_logs(parsed_args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(parsed_args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
