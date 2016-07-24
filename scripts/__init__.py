""" This module pulls all the ETL scripts together in one place. """


from scripts.uk_drivers_sync_police import load_into_warehouse
from scripts.german_payroll_wizard import produce_delivery_count_table


all_scripts = {
    'sync_police': load_into_warehouse,
    'payroll_germany': produce_delivery_count_table
}


def run_etl_scripts(scripts):
    for script in scripts:
        script()
