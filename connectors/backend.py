'''
Created on Jul 28, 2015

@author: nicolasguenon
'''


# Warning: deprecated. Use valkfleet model instead.

import argparse
import http.cookiejar
import json
import logging
import pprint
import sys
import urllib.request

from common import logging_configurer
from common.logging_configurer import LOG_DIR

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


class BackendConnector:

    def __init__(self):
        self.auth_data = None
        self.auth_token = None
        self.opener = None

    def authenticate(self, username, password):
        logger.info('Authenticating...')

        # In reality the backend doesn't use a cookie, but a JSON token
        cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
                            urllib.request.HTTPCookieProcessor(cookie_jar))

        login_params = {'username': username,
                        'password': password}
        post_data = json.dumps(login_params)
        post_data = post_data.encode('utf-8')

        request1 = urllib.request.Request(LOGIN_URL)
        request1.add_header('content-type', 'application/json')
        request1.add_header('accept', 'application/json')

        with self.opener.open(request1, post_data) as response1:

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

        self.auth_data = json.loads(auth_json)
        self.auth_token = self.auth_data['token']
        logger.debug('Got auth token: {}'.format(self.auth_token))
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

        return


def parse_args():

    parser = argparse.ArgumentParser(
            description='Connects to backend and outputs the auth token',
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


def main(parsed_args):
    backend = BackendConnector()
    backend.authenticate(parsed_args.username, parsed_args.password)
    msg = 'Token = {}'.format(backend.auth_token)
    logger.info(msg)
    print(msg)
    sys.stdout.flush()


if __name__ == '__main__':

    parsed_args = parse_args()
    configure_logs(parsed_args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(parsed_args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
