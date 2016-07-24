'''
Created on Jul 28, 2015

@author: nicolasguenon
'''
import argparse
import csv
import http.cookiejar
import io
import json
import logging
import pprint
import sys
import time
import urllib.request
from common import logging_configurer
from common.logging_configurer import configure_default, LOG_DIR

MODULE_NAME = 'get_restaurants_from_backend'
LOGIN_URL = 'https://api.valkfleet.com/login'
RESTAURANTS_URL = 'https://api.valkfleet.com/restaurants/'
FLEETS_URL = 'https://api.valkfleet.com/fleets/'

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

    parser.add_argument('-u', '--username', action='store',
                        dest='username', required=True,
                        help='user to log into the backend')
    parser.add_argument('-p', '--password', action='store',
                        dest='password', required=True,
                        help='password to log into the backend')

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
        fleets_dict[data['uuid']] = data
    return fleets_dict


def download_data(username, password):

    logger.info('Begin download_data')

    # In reality the backend doesn't use a cookie, but a JSON token
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
                             urllib.request.HTTPCookieProcessor(cookie_jar))

    # No need to log right now, but will be needed soon
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

    fleets_dict = get_fleets(opener, auth_data)

    more = True
    restaurants = []
    cursor = None

    while more:
        if cursor is None:
            url = RESTAURANTS_URL
        else:
            # request = API_DRIVERS_URL + '?cursor={}'.format(cursor)
            url = (RESTAURANTS_URL + '?' +
                   urllib.parse.urlencode({'cursor': cursor}))
            logger.debug('URL: ' + url)

        request = urllib.request.Request(url)
        request.add_header('Authorization', auth_data['token'])
        logger.debug('Getting JSON data')
        with opener.open(request) as response:

            logger.debug('Retrieved JSON from this URL: {}'.
                         format(response.geturl()))

            # The response shouldn't be larger than 10 MiB
            rest_json = response.read(10485760).decode('utf-8')

            if (response.read(1024) != b''):
                raise ScriptError('Dowloaded JSON is larger than 10 MiB')

        data = json.loads(rest_json)
        restaurants.extend(data['items'])
        more = data['more']
        cursor = data['cursor']

    logger.info('Got info for {} restaurants'.format(len(restaurants)))

    return (fleets_dict, restaurants)


def sanitize_phone_number(phone_number):
    output = []
    for char in phone_number:
        if char in ' ,.-':
            continue
        else:
            output.append(char)
    return ''.join(output)


def generate_csv(fleets_dict, restaurants):

    # Structure of each "restaurant" dict:
    #
    # {   'address': {   'city': 'Warszawa',
    #                    'comment': None,
    #                    'country': 'Polska',
    #                    'created_at': '2015-07-22T14:56:04.678740+00:00',
    #                    'deleted_at': None,
    #                    'formatted_address': 'Świętokrzyska 16, 00-001 '
    #                                         'Warszawa, Polska',
    #                    'geocoding_log': [],
    #                    'location': {'lat': 52.236431, 'lng': 21.01274599999},
    #                    'location_type': None,
    #                    'modified_at': '2015-07-22T14:56:04.718900+00:00',
    #                    'number': '16',
    #                    'phone_number': '+48 228261338 -- 20007144',
    #                    'raw_address': 'Świętokrzyska,16,00-001,Warszawa,'
    #                                   'Polska',
    #                    'street': 'Świętokrzyska',
    #                    'uuid': None,
    #                    'zipcode': '00-001'},
    #     'city': 'c5871194-ebd9-4af1-8235-750738306cf5',
    #     'created_at': '2015-03-29T20:06:26.849900+00:00',
    #     'deleted_at': None,
    #     'dont_process': False,
    #     'ignore': False,
    #     'metadata': {   'created_at': '2015-07-22T14:56:04.678830+00:00',
    #                     'deleted_at': None,
    #                     'id_9C': None,
    #                     'modified_at': '2015-07-22T14:56:04.718980+00:00',
    #                     'source_data': None,
    #                     'source_id': '20006932',
    #                     'source_name': 'admin_warsaw',
    #                     'uuid': None,
    #                     'yReceipts_id': None},
    #     'modified_at': '2015-07-22T14:56:04.719010+00:00',
    #     'name': 'Bobby Burger - Mazowiecka',
    #     'short_name': '',
    #     'uuid': 'e89410f4-9ad6-48b2-9109-dfbf98578634'}

    rest_dict = {}
    for restaurant in restaurants:
        rest_dict[restaurant['uuid']] = restaurant

    with io.StringIO(newline='') as f:
        writer = csv.writer(f)
        writer.writerow(('Fleet name', 'Restaurant Name', 'Country', 'City',
                         'ZIP code', 'Street',
                         'Number', 'Formatted Address',
                         'Raw Address', 'Phone number',
                         'Latitude', 'Longitude',
                         "Don't process", "UUID"))

        for fleet_uuid in fleets_dict:
            fleet = fleets_dict[fleet_uuid]
            logger.debug(fleet)
            for rest_uuid in fleet['restaurants']:
                restaurant = rest_dict[rest_uuid]
                #pp.pprint(restaurant)
                #sys.exit()
                address = restaurant['address']

                location = address['location']
                phone_number = sanitize_phone_number(address['phone_number'])
                writer.writerow((fleet['name'], restaurant['name'],
                                 address['country'],
                                 address['city'], address['zipcode'],
                                 address['street'], address['number'],
                                 address['formatted_address'],
                                 address['raw_address'],
                                 phone_number,
                                 location['lat'], location['lng'],
                                 restaurant['dont_process'],
                                 restaurant['uuid']))

        print(f.getvalue())
    # print(pp.pprint(restaurants[0]))


def main(parsed_args):
    (fleets_dict, restaurants) = download_data(parsed_args.username, parsed_args.password)
    generate_csv(fleets_dict, restaurants)
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
