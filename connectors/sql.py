""" Connectors for MySQL and Postgres databases. """


from pandas import DataFrame
from sqlalchemy import create_engine
from common.sqlreader import SQLReader
from connectors.base import BaseConnector


class BaseSQLConnector(BaseConnector):
    def _authenticate(self):
        return create_engine(self.url)

    @property
    def url(self):
        url_format = '{dialect}://{username}:{password}@{host}/{database}'
        return url_format.format(**self.config)

    @property
    def safe_url(self):
        safe_url_format = '{dialect}://{username}:***@{host}/{database}'
        return safe_url_format.format(**self.config)

    def execute(self, dynamic_sql, **parameters):
        sql = dynamic_sql.format(**parameters)

        with self.db.connect() as c:
            result = c.execute(sql)

        records = list(result)
        df = DataFrame.from_records(records)
        # noinspection PyProtectedMember
        df.columns = result._metadata.keys

        return df

    def _execute_test(self):
        module = '.'.join(['assets', 'sql', self.name])
        test_query = SQLReader(module=module).statements[0]
        return self.execute(test_query)


class WarehouseConnector(BaseSQLConnector):
    pass


class CloudSQLConnector(BaseSQLConnector):
    pass


if __name__ == '__main__':
    WarehouseConnector().execute_test()
    CloudSQLConnector().execute_test()
