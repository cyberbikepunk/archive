""" Produce a table with the monthly count of driver deliveries for Germany. """


from logging import getLogger
from petl import fromdataframe, leftjoin
from os.path import join

from common.logger import configure_logger, MultilineFilter
from common.sqlreader import SQLReader
from connectors import OdooConnector, WarehouseConnector
from common.settings import CACHE_DIR


configure_logger()
log = getLogger(__name__)


dwh_cache_file = join(CACHE_DIR, 'german_drivers_monthly_delivery_counts_from_delivery_dwh.xlsx')
odoo_cache_file = join(CACHE_DIR, 'german_drivers_from_odoo.xlsx')
output_file = join(CACHE_DIR, 'german_payroll_table.xlsx')


def produce_delivery_count_table():
    log.addFilter(MultilineFilter())  # useful for tables
    log.info('Starting to generate the monthly german payroll table')

    # ------------------------------
    # Extract driver names from Odoo
    # ------------------------------

    log.info('Extracting driver names from Odoo')
    odoo = OdooConnector()

    filters = [('supplier', '=', True),
               ('active', '=', True),
               ('company_id', '=', 5)]  # 5 is germany
    df = odoo.extract('res.partner', filters)
    odoo_drivers = fromdataframe(df)

    mappings = {
        'driver_app_username': 'x_driver_app_username',
        'planday_salary_id_in_odoo': 'x_salary_id',
        'odoo_id': 'id',
        'fullname_in_odoo': 'display_name'
    }
    odoo_drivers = odoo_drivers.fieldmap(mappings)

    # cache the results
    odoo_drivers.toxlsx(odoo_cache_file)
    log.info('%s drivers found in Odoo', odoo_drivers.nrows())
    log.debug(odoo_drivers.look())

    # ------------------------------------------
    # Extract delivery counts from the warehouse
    # ------------------------------------------

    log.info('Extracting delivery counts from the DWH')
    dwh = WarehouseConnector()

    query = SQLReader('sql.german_drivers_delivery_counts').statements[0]
    log.debug(query)
    df = dwh.execute(query)
    driver_counts = fromdataframe(df)

    # cache the results
    driver_counts.toxlsx(dwh_cache_file)
    log.info('%s drivers found in the DWH', driver_counts.nrows())
    log.info('Deliveries per driver %s', driver_counts.stats('number_of_deliveries'))
    log.debug(driver_counts.look())

    # ----------------------------
    # Join the two tables together
    # ----------------------------

    payroll = leftjoin(driver_counts, odoo_drivers, key='driver_app_username')
    # Some usernames appear multiple times in Odoo
    payroll = payroll.distinct('driver_app_username')
    log.debug(payroll.look())

    payroll.toxlsx(output_file)
    log.info('Payroll table saved to %s', output_file)
    log.removeFilter(MultilineFilter())


if __name__ == '__main__':
    produce_delivery_count_table()
