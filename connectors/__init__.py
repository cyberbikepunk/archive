""" This module pulls all the connectors together in one place. """


from connectors.valkfleet import ValkfleetConnector
from connectors.odoo import OdooConnector
from connectors.sql import CloudSQLConnector, WarehouseConnector


all_connectors = {
    'valkfleet': ValkfleetConnector,
    'odoo': OdooConnector,
    'tableau': WarehouseConnector,
    'cloudsql': CloudSQLConnector
}


def test_connections(connectors, test, **options):
    for connector in connectors:
        engine = connector(**options)
        if test == 'extract':
            engine.execute_test()
