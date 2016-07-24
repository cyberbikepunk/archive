'''
Created on Jul 28, 2015

@author: nicolasguenon
'''
import argparse
import datetime
import logging
import pprint
import psycopg2
from common import logging_configurer
from common.logging_configurer import LOG_DIR

COUNT_EXISTING_SQL = """SELECT COUNT(*)
                        FROM tableau.restaurant_weekly_delivery
                        WHERE year = %s
                            AND week = %s
                            AND fleet = %s;"""

INSERT_SQL = """
INSERT INTO tableau.restaurant_weekly_delivery
(year,
  week,
  fleet_backend_name,
  restaurant_city,
  restaurant_name,
  restaurant_uuid,
  deliveries)
SELECT
    date_part('year', gastronomic_day) as year,
    date_part('week', gastronomic_day) as week,
    fleet,
    restaurant_city,
    restaurant_name,
    restaurant_uuid,
    count(delivery_uuid) as deliveries
FROM public.delivery
WHERE driver_username NOT IN ('%%demo%%', '%%test%%', '%%valk%%')
    AND (last_delivery_status LIKE ('done')
    OR cancellation_reason LIKE ('Order delivered'))
GROUP BY date_part('year', gastronomic_day),
    DATE_PART('week', gastronomic_day),
    fleet, restaurant_city, restaurant_name, restaurant_uuid
HAVING date_part('year', gastronomic_day) = %s
    AND date_part('week', gastronomic_day) = %s
;
"""

DELETE_SQL = """DELETE FROM tableau.restaurant_weekly_delivery
                    WHERE year = %s
                        AND week = %s;"""

UPDATE_FLEET_INFO_SQL = """
UPDATE tableau.restaurant_weekly_delivery
SET fleet_display_name = tableau.fleet.display_name,
    fleet_uuid = tableau.fleet.uuid,
    fleet_country_code = tableau.fleet.country_code,
    fleet_country_name = tableau.fleet.country_name
FROM tableau.fleet
WHERE tableau.restaurant_weekly_delivery.fleet_backend_name = tableau.fleet.backend_name;
"""

MODULE_NAME = 'populate_restaurant_weekly_delivery'

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


def update_db(username, password, year, week):

    #conn = psycopg2.connect(("host='localhost' "
    conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                             "dbname='valk_fleet' user='{}' "
                             "password='{}'").format(username, password))
    cur = conn.cursor()

    logger.debug('Deleting Week {}'.format(week))
    cur.execute(DELETE_SQL, (year, week))
    logger.debug('Inserting Week {}'.format(week))
    cur.execute(INSERT_SQL, (year, week))
    logger.debug('Updating fleet info')
    cur.execute(UPDATE_FLEET_INFO_SQL)

    conn.commit()
    cur.close()
    conn.close()


def main(parsed_args):
    # This ensures the week has already been completed
    yesterday = datetime.date.today() - 7 * datetime.timedelta(days=1)
    (year, week, _) = yesterday.isocalendar()
    logger.debug('Filling table for year = {}, week = {}'.
                 format(year, week))

    update_db(parsed_args.dbuser, parsed_args.dbpassword, year, week)


if __name__ == '__main__':

    parsed_args = parse_args()
    configure_logs(parsed_args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(parsed_args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
