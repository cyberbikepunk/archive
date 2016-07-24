""" Test data connectors.  """


from pandas import DataFrame

from common.sqlreader import SQLReader
from connectors import all_connectors, WarehouseConnector

import pytest


def test_login():
    for name, connector in all_connectors.items():
        db = connector()
        assert db

# TODO: Change the odoo and valkfleet API requests used in test_execute.

@pytest.mark.skipif(True, reason='Takes too long')
def test_execute():
    for name, connector in all_connectors.items():
        db = connector()
        df = db.execute_test()

        assert isinstance(db, DataFrame)
        assert all(df.shape)
        assert all(df.columns)