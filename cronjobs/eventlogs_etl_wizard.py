""" Extract audit_logs from the data store and load them into the postgres data warehouse.

Usage:
    python -m sandbox.get_audit_logs_demo --batch <batch> --start <start>

"""


from argparse import ArgumentParser
from dateutil.parser import parse
from sys import exit
from datetime import datetime, timedelta
from logging import getLogger
from pandas import DataFrame

from common.logger import configure_logger
from common.sqlreader import SQLReader
from connectors import ValkfleetConnector, WarehouseConnector


ENDPOINT = 'https://api.valkfleet.com/event_audit_logs/'
SCHEMA = 'tableau'
TABLE = 'audit_logs'


DEFAULT_BATCH_SIZE = 200
DEFAULT_START = datetime(2016, 2, 2)
NOW = datetime.now()


configure_logger()
log = getLogger('audit_logs')
sql = SQLReader('sql.audit_logs')


def load_audit_logs_into_postgres(options):
    session = ValkfleetConnector().db
    engine = WarehouseConnector().db

    log.info('Loading audit points for {start} to {stop} (batches of {n})'.format(
        start=options.start,
        stop=NOW,
        n=options.batch_size)
    )

    # Avoid loading data twice: grab the latest timestamp in the database
    result = engine.execute(sql.statements[1])
    resume_from = args.start

    if result.rowcount:
        last_timestamp = list(result)[0][0] + timedelta(milliseconds=1)
        if last_timestamp > args.start:
            resume_from = last_timestamp
            log.info('Resuming at %s', resume_from)

    cursor = None
    more = True
    batch_counter = 0
    batch_max_timestamp = None

    while more:
        url = ENDPOINT + '?filter=timestamp gt {start}&orderby=timestamp&batch_size={batch_size}'.format(
            start=resume_from.isoformat(),
            batch_size=options.batch_size
        )
        if cursor:
            url += '&cursor=%s' % cursor

        log.info('Requesting %s', url)
        response = session.get(url)
        json = response.json()

        if 'error' not in json.keys():
            records = json['items']
            more = json['more']
            cursor = json['cursor']

            def add_foreign_key(records_):
                for record in records_:
                    if record['event'] != 'acceptRoute-clicked':
                        record.update({'delivery_uuid': record['metadata']['delivery']})
                        del record['metadata']
                        yield record

            fc_records = list(add_foreign_key(records))
            batch = DataFrame().from_records(fc_records)

            if batch.empty:
                log.warning('Batch %s is empty', batch_counter)

            batch.to_sql(TABLE, engine,
                         schema=SCHEMA,
                         if_exists='append',
                         index=False)

            # The time columns are actually strings
            batch_min_timestamp = batch['timestamp'].min()[:19]
            batch_max_timestamp = batch['timestamp'].max()[:19]

            batch_counter += 1
            kwargs = dict(schema=SCHEMA,
                          table=TABLE,
                          n=batch_counter,
                          records=batch.shape[0],
                          fields=batch.shape[1],
                          min=batch_min_timestamp,
                          max=batch_max_timestamp)
            log.info('Loaded batch {n} into table {schema}.{table} '
                     '({records} records, {fields} fields):'
                     '{min} to {max}'.format(**kwargs))

        else:
            message = 'Lost the cursor on batch {n} {time}): {status} {error}'.format(
                n=batch_counter,
                time=batch_max_timestamp,
                status=response.status_code,
                error=response.json()
            )
            log.error(message, exc_info=True)
            exit(message)

    log.info('Finished loading tracks for %s to %s', options.start, NOW)


if __name__ == '__main__':
    p = ArgumentParser()

    p.add_argument(
        '--start',
        help='start datetime yyyy-mm-dd (default = %s)' % DEFAULT_START,
        type=parse,
        dest='start',
        default=DEFAULT_START
    )

    p.add_argument(
        '--batch',
        help='batch size (default = %s)' % DEFAULT_BATCH_SIZE,
        type=int,
        dest='batch_size',
        default=DEFAULT_BATCH_SIZE
    )

    args = p.parse_args()
    load_audit_logs_into_postgres(args)
