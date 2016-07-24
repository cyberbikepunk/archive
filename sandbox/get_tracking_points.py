""" Extract tracking points from the DataStore and load them into Postgres.

Usage:
    python -m sandbox.get_tracking_points --batch <batch>
                                          --start <start>
                                          --stop <stop>

Postgres:
    The relevant SQL statements are in get_tracking_points.sql. A table needs to be created
    before the script is run (the first statement).

"""


from datetime import datetime
from logging import getLogger
from pandas import DataFrame
from dateutil.parser import parse

from common.logger import configure_logger
from common.parser import InternalToolsParser
from common.sqlreader import sql
from connectors import ValkfleetConnector, WarehouseConnector


ENDPOINT = 'https://api.valkfleet.com/tracking_points/inrange'
SCHEMA = 'tableau'
TABLE = 'tracking_points'

DEFAULT_BATCH_SIZE = 500
DEFAULT_START = datetime(2016, 1, 1)
DEFAULT_STOP = datetime(2016, 1, 31)


POSTGRES = 'get_tracking_points'
LAST_RECORD = 1


configure_logger()
log = getLogger('get_tracking_points')


def fetch_time_interval(args):
    session = ValkfleetConnector().db
    engine = WarehouseConnector().db

    log.info('Starting to load tracking points for %s to %s (batches of %s)...',
             args.start,
             args.stop,
             args.batch_size)

    query = sql(POSTGRES)[LAST_RECORD]
    result = engine.execute(query)
    resume_from = args.start

    if result.rowcount:
        last_timestamp = list(result)[0][0]
        if last_timestamp > args.start:
            resume_from = last_timestamp
            log.info('Resuming at %s', resume_from)

    cursor = None
    more = True
    batch_counter = 0
    batch_max_timestamp = None

    while more:
        url = ENDPOINT + '?begin={start}&end={stop}&batchsize={batch_size}'.format(start=resume_from,
                                                                                   stop=args.stop,
                                                                                   batch_size=args.batch_size)
        if cursor:
            url += '&cursor=%s' % cursor

        log.info('Requesting %s', url)
        response = session.get(url)
        json = response.json()

        if 'error' not in json.keys():
            records = json['items']
            more = json['more']
            cursor = json['cursor']

            batch = DataFrame().from_records(records)
            # Postgres doesn't swallow dicts and the
            # information is already in lat/lng anyways.
            del batch['location']

            # Pandas will create a table if it doesn't
            # exist but column types won't be optimal.
            batch.to_sql(TABLE, engine,
                         schema=SCHEMA,
                         if_exists='append',
                         index=False)

            # The time columns are actually strings
            batch_min_timestamp = batch['datetime'].min()[:19]
            batch_max_timestamp = batch['datetime'].max()[:19]

            batch_counter += 1
            log.info("Loaded batch {index} into table {schema}.{table} | "
                     "{tmin} to {tmax} | "
                     "{nrecords} records | "
                     "{nfields} fields".format(schema=SCHEMA,
                                               table=TABLE,
                                               index=batch_counter,
                                               nrecords=batch.shape[0],
                                               nfields=batch.shape[1],
                                               tmin=batch_min_timestamp,
                                               tmax=batch_max_timestamp))

        else:
            message = 'Lost the cursor on batch {n} ({time}): aborting! {status} {error}'.format(
                n=batch_counter,
                time=batch_max_timestamp,
                status=response.status_code,
                error=response.json()
            )
            log.error(message, exc_info=True)
            raise ConnectionAbortedError(message)

    log.info('Finished loading tracks for %s to %s', args.start, args.stop)


if __name__ == '__main__':

    p = InternalToolsParser()

    p.add_argument(
        '--start',
        help='start datetime yyyy-mm-dd (default = %s)' % DEFAULT_START,
        type=parse,
        dest='start',
        default=DEFAULT_START
    )

    p.add_argument(
        '--stop',
        help='stop datetime yyyy-mm-dd (default = %s)' % DEFAULT_STOP,
        type=parse,
        dest='stop',
        default=DEFAULT_STOP
    )

    p.add_argument(
        '--batch',
        help='batch size (default = %s)' % DEFAULT_BATCH_SIZE,
        type=int,
        dest='batch_size',
        default=DEFAULT_BATCH_SIZE
    )

    args_ = p.parse_args()
    fetch_time_interval(args_)
