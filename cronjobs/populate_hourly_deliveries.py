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

INSERT_SQL = """
insert into tableau.hourly_deliveries
(
fleet_backend_name,
gastronomic_day,
hour,
actual_datetime ,
deliveries_created ,
deliveries_done ,
deliveries_cancelled_as_done ,
total_deliveries_done
)


select row_generator.fleet_backend_name, row_generator.gastronomic_day, row_generator.hour, row_generator.actual_datetime,
  coalesce(agg_deliveries.num_deliveries, 0) as deliveries_created,
  coalesce(agg_deliveries.dones, 0) as deliveries_done,
  coalesce(agg_deliveries.canc_dones, 0) as deliveries_cancelled_as_done,
  coalesce(agg_deliveries.total_dones, 0) as total_deliveries_done

FROM

(

  SELECT date_trunc('day', dd)::date as gastronomic_day,
    hh as hour,
    CASE WHEN hh >= 8 THEN dd ELSE dd + '1 day'::interval END + cast(to_char(hh, '99')||':00' AS time without time zone) as actual_datetime,
    fleets_table.fleet_backend_name as fleet_backend_name
  FROM
    generate_series (%s::timestamp, %s::timestamp, '1 day'::interval) dd,
    generate_series(0, 23, 1) hh,
    (select distinct fleet_backend_name as fleet_backend_name, min(gastronomic_day) as first_day, max(gastronomic_day) as last_day from tableau.delivery_dwh group by fleet_backend_name) as fleets_table
  WHERE date_trunc('day', dd)::date >= fleets_table.first_day and date_trunc('day', dd)::date <= fleets_table.last_day

) AS row_generator

LEFT OUTER JOIN

(
  select date(gastronomic_day) as gastronomic_day,
  date_part('hour', created_at_timestamp) as hour,
  fleet_backend_name,
  date_trunc('hour', created_at_timestamp) as actual_datetime,
  count(*) as num_deliveries,
  count(CASE WHEN last_delivery_status = 'done' THEN 1 ELSE NULL END) as dones,
  count(CASE WHEN last_delivery_status = 'cancelled' and cancellation_reason = 'Order delivered' THEN 1 ELSE NULL END) as canc_dones,
  count(CASE WHEN last_delivery_status = 'done' OR (last_delivery_status = 'cancelled' and cancellation_reason = 'Order delivered') THEN 1 ELSE NULL END) as total_dones
  from tableau.delivery_dwh
  where gastronomic_day >= %s
  and gastronomic_day <= %s

  group by fleet_backend_name, date(gastronomic_day), date_part('hour', created_at_timestamp), date_trunc('hour', created_at_timestamp)
) AS agg_deliveries

ON (row_generator.gastronomic_day = agg_deliveries.gastronomic_day
  AND row_generator.hour = agg_deliveries.hour
  AND row_generator.fleet_backend_name = agg_deliveries.fleet_backend_name)
;
"""

DELETE_SQL = """
delete from tableau.hourly_deliveries
WHERE gastronomic_day >= %s
AND gastronomic_day <= %s;
"""

UPDATE_FLEET_INFO_SQL = """
UPDATE tableau.hourly_deliveries
SET fleet_display_name = tableau.fleet.display_name,
    fleet_uuid = tableau.fleet.uuid,
    fleet_country_code = tableau.fleet.country_code,
    fleet_country_name = tableau.fleet.country_name
FROM tableau.fleet
WHERE tableau.hourly_deliveries.fleet_backend_name = tableau.fleet.backend_name;
"""

MODULE_NAME = 'populate_hourly_deliveries'

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


def update_db(username, password, start_date, end_date):

    #conn = psycopg2.connect(("host='localhost' "
    conn = psycopg2.connect(("host='bi-live-mon.deliveryhero.com' "
                             "dbname='valk_fleet' user='{}' "
                             "password='{}'").format(username, password))
    cur = conn.cursor()

    logger.debug('Deleting between dates {} and {}'.format(start_date,
                                                           end_date))
    cur.execute(DELETE_SQL, (start_date, end_date))
    logger.debug('Inserting between dates {} and {}'.format(start_date,
                                                            end_date))
    cur.execute(INSERT_SQL, (start_date, end_date, start_date, end_date))
    conn.commit()

    logger.debug('Updating fleet info')
    cur.execute(UPDATE_FLEET_INFO_SQL)

    conn.commit()
    cur.close()
    conn.close()


def main(parsed_args):
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=2)
    logger.debug('Start date = {}, end date = {}'.format(start_date, end_date))

    update_db(parsed_args.dbuser, parsed_args.dbpassword, start_date, end_date)


if __name__ == '__main__':

    parsed_args = parse_args()
    configure_logs(parsed_args)

    logger.info(60 * '=')
    logger.info("Script execution started")

    main(parsed_args)

    logger.info("Script execution finished")
    logger.info(60 * '=')
