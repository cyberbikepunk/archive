""" This script gets the list of drivers from the backend and outputs it in CSV format.

Created on Jul 28, 2015
@author: nicolasguenon

Modified on Jan 6, 2016
@author: loicjounot

"""

import csv
import http.cookiejar
import io
import json
import logging
import pprint
import sys
import urllib.request
import urllib.parse

from common import logging_configurer
from textwrap import dedent
from urllib.error import HTTPError

from common.logging_configurer import LOG_DIR
from common.parser import InternalToolsParser

MODULE_NAME = 'get_drivers_from_backend'
LOGIN_URL = 'https://api.valkfleet.com/login'
DRIVERS_URL = 'https://api.valkfleet.com/drivers/'
USERS_URL = 'https://api.valkfleet.com/users/'
FLEETS_URL = 'https://api.valkfleet.com/fleets/'

# __name__ is the name of the module or '__main__'
logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=4)

class ScriptError(Exception):
    pass


def build_parser():
    return InternalToolsParser(description=dedent(__doc__))


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


def get_fleets(opener, auth_data):

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

        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_data['token'])
        logger.debug('Getting JSON data')
        with opener.open(request) as response:

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

    # Getting fleet names from Fleets API endpoint
    fleets_dict = {}
    for fleet_uuid in fleet_uuids:
        url = (FLEETS_URL + urllib.parse.quote_plus(fleet_uuid))
        logger.debug('URL: ' + url)

        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_data['token'])
        logger.debug('Getting JSON data')
        with opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 1 MiB
            fleet_json = response.read(1048576).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded JSON is larger than 1 MiB')

        data = json.loads(fleet_json)
        logger.debug(data)
        fleets_dict[data['uuid']] = data['name']
    return fleets_dict


def download_data(username, password):
    logger.info('Begin download_data')

    # In reality the backend doesn't use a cookie, but a JSON token
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
                             urllib.request.HTTPCookieProcessor(cookie_jar))

    login_params = {'username': username,
                    'password': password}
    post_data = json.dumps(login_params)
    post_data = post_data.encode('utf-8')

    request1 = urllib.request.Request(LOGIN_URL)
    request1.add_header('content-type', 'application/json')
    request1.add_header('accept', 'application/json')

    logger.debug('Authenticating...')
    with opener.open(request1, post_data) as response1:

        response1_url = response1.geturl()
        logger.debug('URL of the response: {}'.format(response1_url))
        if (response1_url != LOGIN_URL):
            msg = ('Received response from an unexpected URL: {}'.
                   format(response1_url))
            logger.error(msg)
            raise ScriptError(msg)
        else:
            logger.debug('Response received from the expected URL')

        for cookie in cookie_jar:
            logger.debug('Received cookie: {}'.format(cookie))

        # The response shouldn't be larger than 1 MiB
        auth_json = response1.read(1048576).decode('utf-8')

        if (response1.read(1024) != b''):
            raise ScriptError('Dowloaded JSON is larger than 1 MiB')

    auth_data = json.loads(auth_json)
    # Sample JSON of the authenticate response
    #   {
    #       "token": "a6fbc3a1-3a21-424c-bc0e-650049ee602f",
    #       "user": {
    #           "username": "nicolas-uk-fc",
    #           "uuid": "c915b2b1-dfaa-40fc-b9d9-fca04bb1108c",
    #           "roles": [
    #               "fleetcontroller"
    #           ],
    #           "fleet_controller": "b6e3de73-24c6-40ac-8d1b-aad87f82754f"
    #       }
    #   }

    # Getting Fleets
    fleets_dict = get_fleets(opener, auth_data)

    # Getting Drivers
    more = True
    drivers = []
    cursor = None

    while more:
        if cursor is None:
            url = DRIVERS_URL
        else:
            url = (DRIVERS_URL + '?' +
                   urllib.parse.urlencode({'cursor': cursor}))
            logger.debug('URL: ' + url)

        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_data['token'])
        logger.debug('Getting JSON data')

        try:
            with opener.open(request) as response:

                logger.debug('Retrieved JSON from this URL: {}'.
                             format(response.geturl()))

                # The response shouldn't be larger than 10 MiB
                drivers_json = response.read(10485760).decode('utf-8')

                if (response.read(1024) != b''):
                    raise ScriptError('Dowloaded JSON is larger than 10 MiB')

            data = json.loads(drivers_json)
            drivers.extend(data['items'])
            more = data['more']
            cursor = data['cursor']

        except HTTPError:
            logger.warning('could not retrieve JSON from this URL: {} (HTTPError)'.format(url))

    # Getting usernames from Users API endpoint (one by one :-( )
    for driver in drivers:
        user_uuid = driver['user']
        url = (USERS_URL + urllib.parse.quote_plus(user_uuid))
        logger.debug('URL: ' + url)

        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_data['token'])
        logger.debug('Getting JSON data')
        with opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 10 MiB
            users_json = response.read(10485760).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded JSON is larger than 10 MiB')

        data = json.loads(users_json)
        driver['username'] = data['username']

    return (fleets_dict, drivers)


def sanitize_phone_number(phone_number):
    return (phone_number.replace(' ', '').replace(',', '').
            replace('.', '').replace('-', ''))


def generate_csv(fleets_dict, drivers):

    # Structure of each "driver" dict:
    #
    # {   'created_at': '2015-07-02T12:06:07.015050+00:00',
    #     'current_shift': '1f929d30-d68d-4a29-94e3-3e447291547b',
    #     'deleted_at': None,
    #     'first_name': 'Andrzej',
    #     'fleet': '23e072c7-fee7-4fc3-963a-5359011d0ae1',
    #     'last_name': 'Pakulski',
    #     'last_position': {   'accuracy': 22.0,
    #                          'altitude': 186.0,
    #                          'battery': 61.0,
    #                          'bearing': 73.0,
    #                          'created_at': '2015-07-30T12:25:40.8570+00:00',
    #                          'datetime': '2015-07-30T12:25:34.010000+00:00',
    #                          'deleted_at': None,
    #                          'device': 'f479c857-8889-4c1d-a7df-aa1ac1b68d6',
    #                          'driver': 'af6ad651-9835-4a96-83bf-05de8fb6aea',
    #                          'gsm_signal': 26.0,
    #                          'lat': 52.2283737,
    #                          'lng': 21.0041043,
    #                          'location': {'lat': 52.2283737,
    #                                       'lng': 21.0041043},
    #                          'location_provider': 'fused',
    #                          'modified_at': '2015-07-30T12:25:40.857+00:00',
    #                          'network_type': 'MOBILE HSPA+',
    #                          'num_satelites': None,
    #                          'route': '12195ee3-ae93-46ab-8a46-53f78c4233a6',
    #                          'shift': '1f929d30-d68d-4a29-94e3-3e447291547b',
    #                          'speed': 2.3001957,
    #                          'ts_milisecond': 1438259134010,
    #                          'uuid': '2a67d9b3-fc66-45e5-b713-d3d7f6145a5d'},
    #     'modified_at': '2015-07-02T12:06:07.015070+00:00',
    #     'phone_number': '609419411',
    #     'user': '3ff1a639-9537-44be-b5e8-531a11c13753',
    #     'uuid': 'af6ad651-9835-4a96-83bf-05de8fb6aea6'}

    with io.StringIO(newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('Username', 'Name', 'Fleet name', 'Phone number',
                         'Driver UUID', 'User UUID', 'Fleet UUID',
                         'created_at', 'deleted_at'))
        for driver in drivers:

            name = '{}, {}'.format(driver['last_name'], driver['first_name'])
            phone_number = sanitize_phone_number(driver['phone_number'])

            writer.writerow((driver['username'], name,
                             fleets_dict.get(driver['fleet'], 'Not found'),
                             phone_number,
                             driver['uuid'], driver['user'],
                             driver['fleet'], driver['created_at'],
                             driver['deleted_at']))

        print(f.getvalue())


def main(parsed_args):
    (fleets_dict, drivers) = download_data(
                                parsed_args.username, parsed_args.password)

    generate_csv(fleets_dict, drivers)
    sys.stdout.flush()


if __name__ == '__main__':

    parser = build_parser()
    args = parser.parse_args()
    configure_logs(args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
