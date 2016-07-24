""" Grab audit logs from the data store. """


from datetime import datetime
from logging import getLogger
from os.path import join
from pandas import DataFrame, concat
from common.logger import configure_logger

from common.settings import CONFIG_DIR
from connectors import ValkfleetConnector


BUCKET = join(CONFIG_DIR, 'get_audit_logs_demo')
BASE_URL = 'https://api.valkfleet.com'
ENDPOINT_QUERY = BASE_URL + '/event_audit_logs/?filter=timestamp gt {start}&orderby=timestamp&cursor={cursor}'
BATCH_SIZE = 500


# This is the event audit-log json returned by the endpoint:
# .../event_audit_logs/?filter=timestamp gt 2016-01-21&orderby=timestamp
#
#     {
#       "more": true,
#       "items": [
#         {
#           "event": "assignDelivery-clicked",
#           "user_uuid": "db4cdb7d-02c5-489e-9a10-ae77b78db859",
#           "metadata": {
#             "delivery": "a80f53f1-401c-41d6-b9c5-594e110c03df"
#           },
#           "driver_uuid": null,
#           "modified_at": "2016-01-21T14:04:41.499301+00:00",
#           "deleted_at": null,
#           "uuid": "996fafc7-8c7a-4b46-883d-e12f27de4c90",
#           "fleet_controller_uuid": "6f24f5a6-9fb2-4bed-8de3-d890d156c0d5",
#           "timestamp": "2016-01-21T14:04:34.006000+00:00",
#           "username": null,
#           "created_at": "2016-01-21T14:04:41.499298+00:00"
#         }
#       ],
#       "cursor": "THE CURSOR"
#     }


def download_since(api, start):
    batches = []
    more = True
    batch_counter = 0
    cursor = None

    while more:
        if batch_counter == 0:
            url = ENDPOINT_QUERY.replace('&cursor={cursor}', '').format(
                start=start.isoformat(),
                batch_size=BATCH_SIZE
            )
        else:
            url = ENDPOINT_QUERY.format(
                cursor=cursor,
                start=start.isoformat(),
                batch_size=BATCH_SIZE
            )

        log.debug('Requesting %s', url)
        response = api.db.get(url)

        if response.status_code == 200:
            json = response.json()

            records = json['items']
            more = json['more']
            cursor = json['cursor']

            batch_df = DataFrame().from_records(records)
            batches.append(batch_df)
            batch_counter += 1

            # The time columns are actually strings
            batch_min_timestamp = batch_df['timestamp'].min()[:19].replace('T', ' ')
            batch_max_timestamp = batch_df['timestamp'].max()[:19].replace('T', ' ')

            log.debug('Batch %s has %s records with %s fields covering %s to %s',
                      batch_counter,
                      batch_df.shape[0],
                      batch_df.shape[1],
                      batch_min_timestamp,
                      batch_max_timestamp)

        else:
            log.error(
                'Failed to get batch %s (SKIPPED!): %s [%s] %s',
                batch_counter,
                response.status_code,
                response.reason,
                response.json(),
                exc_info=True
            )

    return concat(batches)


def main(start):
    log.info('Starting to collect audit logs since %s', start)

    api = ValkfleetConnector()
    filepath = join(BUCKET, start.strftime('%Y-%m-%d') + '.csv')
    df = download_since(api, start)
    df.to_csv(filepath)

    log.info('Finished to collect audit logs since %s', start)


if __name__ == '__main__':
    configure_logger()
    log = getLogger(__name__)
    main(start=datetime(2016, 2, 3, 18, 0, 0))

