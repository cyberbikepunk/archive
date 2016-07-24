""" The connectors for the valkfleet backend. """


from json import loads
from logging import getLogger
from pandas import DataFrame
from requests import Session

from connectors.base import BaseConnector


log = getLogger(__name__)

# Set the API cursor limit
BATCH_SIZE = None


class ValkfleetAPIError(Exception):
    pass


class ValkfleetConnector(BaseConnector):
    def _authenticate(self):
        session = Session()

        url = self.url + '/login'
        response = session.post(url, json={'username': self.username,
                                           'password': self.password})

        if response.status_code == 200:
            json = loads(response.text)

            # Add the authentication token to the session header
            token = {'authorization': json['token']}
            session.headers.update(token)

            return session

        else:
            kwargs = {'url': url,
                      'reason': response.reason,
                      'code': response.status_code}
            message = '{code} {url} {reason}'.format(**kwargs)

            raise ValkfleetAPIError(message)

    def _get_endpoint(self, endpoint):
        url = self.url + '/' + endpoint + '/'

        if BATCH_SIZE:
            url += '?batch_size=%s' % BATCH_SIZE

        more = True
        cursor = None
        uuids = []
        counter = 1

        while more:
            if cursor is not None:
                url += '?cursor=' + cursor

            log.debug('Request (cursor %s) = %s', counter, url)
            response = self.db.get(url)
            json = loads(response.text)

            if 'error' in json.keys():
                raise ValkfleetAPIError('%s' % json)

            items = json['items']
            uuids.extend(items)
            more = json['more']
            cursor = json['cursor']
            counter += 1

        log.debug('Found %s %s', len(uuids), endpoint)
        return uuids

    def _get_records(self, endpoint, uuids):
        for uuid in uuids:
            yield self.get_record(endpoint, uuid)

    def get_records(self, endpoint, uuids):
        records = self._get_records(endpoint, uuids)
        return DataFrame().from_records(records)

    def get_record(self, endpoint, uuid):
        url = self.url + '/' + endpoint + '/' + uuid
        record = self.db.get(url)
        json = loads(record.text)
        log.debug('Response = %s', json)
        return json

    def extract(self, endpoint):
        records = self._get_endpoint(endpoint)
        # The API is not consistent. Some endpoints
        # return a list of uuids (strings). Others
        # return directly a list of records (dicts).
        are_uuids = map(lambda x: isinstance(x, str), records)

        if all(are_uuids):
            return self.get_records(endpoint, records)
        else:
            return records

    def _execute_test(self):
        return self.extract('fleets')


if __name__ == '__main__':
    ValkfleetConnector().execute_test()
