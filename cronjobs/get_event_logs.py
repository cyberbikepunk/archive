"""Extract, transform and load event audit logs into the data warehouse.

Usage:
    cronjob.events_etl_wizard SOURCE TARGET [--start=BEGIN] [--batch=SIZE]

Arguments:
    SOURCE                 Either "drivers" or "fleet-controllers"
    TARGET                 Target table in the data warehouse

Options:
    --help                  Show this message
    --start=DATE            Start date [default: 2016-01-01]
    --batch=SIZE            API batch size [default: 500]

"""


from cached_property import cached_property
from dateutil.parser import parse
from datetime import datetime, timedelta
from logging import getLogger
from docopt import docopt
from pandas import DataFrame, to_datetime

from common.logger import configure_logger
from common.sqlreader import sql
from connectors import ValkfleetConnector, WarehouseConnector
from connectors.valkfleet import ValkfleetAPIError


CREATE_TABLE_IF_NOT_EXISTS = sql('get_event_logs')[0]
GET_LAST_TIMESTAMP = sql('get_event_logs')[1]
GRANT_ACCESS = sql('get_event_logs')[2]

configure_logger()
log = getLogger('get_event_logs')


class EventsETL(object):
    api_endpoint = 'event_audit_logs'

    # The CRUD doesn't let me filter by event type
    api_query = [
        'filter={time_field} gt {start_time}',
        'batch_size={batch_size}',
        'orderby={time_field}'
    ]

    def __init__(self,
                 source,
                 target,
                 start='2016-01-01',
                 batch='200'):

        assert source in ('drivers', 'fleet-controllers')

        self.api = ValkfleetConnector()
        self.dwh = WarehouseConnector()

        self.api_source = source
        self.dwh_target = target

        self.etl_timestamp = datetime.now()
        self.start_time = parse(start)
        self.batch_size = int(batch)

        self.counter = 0
        self.cursor = None
        self.more = True

        self.batch_df = DataFrame()
        self.batch_records = []

        self.time_field = 'created_at' if self.api_source == 'drivers' else 'timestamp'
        self.metadata = 'route_uuid' if self.api_source == 'drivers' else 'delivery_uuid'
        # TODO: use python urllib or request module to buils the URL 
        self.api_payload = '?' + '&'.join(self.api_query).format(**self.api_params)

    @property
    def base_url(self):
        return self.api.url + '/' + self.api_endpoint + '/' + self.api_payload

    @property
    def url(self):
        url = self.base_url
        if self.cursor:
            url += '&cursor=%s' % self.cursor
        return url

    @property
    def api_params(self):
        return {'start_time': self.resume_time.isoformat(),
                'batch_size': self.batch_size,
                'time_field': self.time_field}

    @property
    def sql_params(self):
        return {'schema_': self.dwh.schema,
                'table_': self.dwh_target,
                'time_field': self.time_field,
                'metadata': self.metadata}

    @cached_property
    def resume_time(self):
        timestamps = self.dwh.execute(GET_LAST_TIMESTAMP, **self.sql_params)
        timestamp = parse(str(timestamps[self.time_field].iloc[0]))
        # TODO: rethink, as this might potentially skip events (rare, but possible)
        last_timestamp = timestamp + timedelta(microseconds=1)

        if last_timestamp > self.start_time:
            return last_timestamp
        else:
            return self.start_time

    def setup(self):
        create_table = CREATE_TABLE_IF_NOT_EXISTS.format(**self.sql_params)
        grant_access = GRANT_ACCESS.format(**self.sql_params)
        self.dwh.db.execute(create_table + grant_access + 'COMMIT;')
        log.info('Target table %s set up', self.dwh_target)

    def extract(self):
        log.debug('Request base URL: %s', self.base_url)
        log.debug('Cursor: %s', self.cursor)

        response = self.api.db.get(self.url)
        json = response.json()

        if 'error' not in json.keys():
            self.batch_records = json['items']
            self.more = json['more']
            self.cursor = json['cursor']

        else:
            parameters = {'count': self.counter, 'error': response.json()}
            message = 'API error on batch {count}: {error}'.format(**parameters)
            log.error(message, exc_info=True)
            raise ValkfleetAPIError(message)

    def transform(self):
        events = []

        for record in self.batch_records:
            if self.api_source == 'drivers':
                if record['event'] == 'acceptRoute-clicked':
                    route_uuid = record['metadata']['route']
                    metadata = {'route_uuid': route_uuid}
                else:
                    continue
            else:
                if record['event'] != 'acceptRoute-clicked':
                    delivery_uuid = record['metadata']['delivery']
                    metadata = {'delivery_uuid': delivery_uuid}
                else:
                    continue

            record.update(metadata)
            del record['metadata']
            events.append(record)

        self.batch_df = DataFrame().from_records(events)
        self.batch_df['etl_timestamp'] = self.etl_timestamp
        self.batch_df[self.time_field] = to_datetime(self.batch_df[self.time_field])

    def load(self):
        self.counter += 1

        if 'event' in self.batch_df.columns:
            self.batch_df.to_sql(self.dwh_target,
                                 self.dwh.db,
                                 schema=self.dwh.schema,
                                 if_exists='append',
                                 index=False)

            parameters = dict(schema=self.dwh.schema,
                              table=self.dwh_target,
                              count=self.counter,
                              records=self.batch_df.shape[0],
                              fields=self.batch_df.shape[1],
                              tmin=self.batch_df[self.time_field].min(),
                              tmax=self.batch_df[self.time_field].max())

            log.info('Loaded batch {count} into table {schema}.{table} '
                     '({records} records, {fields} fields): '
                     '{tmin} to {tmax}'.format(**parameters))

        else:
            log.info('Batch %s is empty: no events found', self.counter)


def run_etl_job(*io, **options):
    """ ETL job for driver or fleet controller events. """

    etl = EventsETL(*io, **options)
    etl.setup()

    log.info('Starting ETL job at %s', etl.etl_timestamp)
    log.info('Resuming extraction at %s', etl.resume_time)

    while etl.more:
        etl.extract()
        etl.transform()
        etl.load()

    log.info('Finished ETL job')


def parse_args():
    kwargs = {k.replace('--', ''): v for k, v in docopt(__doc__).items()}
    args = (kwargs.pop('SOURCE'), kwargs.pop('TARGET'))
    log.info('ETL job source and target: %s', args)
    log.info('ETL options: %s', kwargs)
    return args, kwargs


if __name__ == '__main__':
    args_, kwargs_ = parse_args()
    run_etl_job(*args_, **kwargs_)
