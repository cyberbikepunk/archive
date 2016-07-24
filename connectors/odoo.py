""" The connector for the Odoo API. """


from logging import getLogger
from pprint import pprint
from xmlrpc.client import ServerProxy
from erppeek import Client, Error as ERPPeekError
from pandas import DataFrame

from connectors.base import BaseConnector

ALL = []
log = getLogger(__name__)


class OdooConnector(BaseConnector):
    def _authenticate(self):
        db = Client(
            self.url,
            db=self.database,
            user=self.username,
            password=self.password
        )
        return db

    def _test_xmlrpc_connection(self):
        # DEPRECATED: use the erppeek wrapper instead

        print('Testing %s' % self.name)
        pprint(self.config)

        try:
            base_url = self.url + '/xmlrpc/2/'

            common = ServerProxy(base_url + 'common')
            user_id = common.authenticate(
                self.database,
                self.username,
                self.password,
                {}
            )

            models = ServerProxy(base_url + 'object')
            access = models.execute_kw(
                self.database,
                user_id,
                self.password,
                'res.partner',
                'check_access_rights',
                ['read'],
                {'raise_exception': False}
            )

            customer_ids = models.execute_kw(
                self.database,
                user_id,
                self.password,
                'res.partner',
                'search',
                [[['is_company', '=', True], ['customer', '=', True]]]
            )

            customer_records = models.execute_kw(
                self.database,
                user_id,
                self.password,
                'res.partner',
                'read',
                [customer_ids]
            )

            print('Connection = %s', common.version())
            print('User ID = %s' % user_id)
            print('Access = %s' % access)
            print('Number of customers = %s' % len(customer_ids))
            print('Number of fields = %s' % len(customer_records[0]))
            print('Success!')

        except ERPPeekError as e:
            print(e.__class__, e)
            print('Failure!')

    def _execute_test(self):
        filters = [('is_company', '=', True),
                   ('customer', '=', True),
                   ('active', '=', True)]

        return self.extract('res.partner', filters)

    def extract(self, table, filters=ALL):
        records = self.db.read(table, filters)
        return DataFrame.from_records(records)

if __name__ == '__main__':
    OdooConnector().execute_test()
