""" Restaurant monthly billing breakdown

This script produces a breakdown of all the orders for one restaurant for one month.
The idea is to be nice with our customers: they should be able to thoroughly check
what they are being billed for!

"""

from os import getenv
from pandas import read_sql_table
from sqlalchemy import create_engine


postgres_password = getenv('VALKFLEET_INTERNAL_TOOLS_POSTGRES_PASSWORD_READ_ONLY_USER')
postgres_user = 'valkfleet_ro'
postgres_database = 'valk_fleet'
postgrest_server = 'bi-live-mon.deliveryhero.com'

uri_format = 'postgresql://{user}:{password}@{server}/{database}'

postgres_uri = uri_format.format(user=postgres_user,
                                 password=postgres_password,
                                 server=postgrest_server,
                                 database=postgres_database)
print(postgres_uri)

engine = create_engine(postgres_uri)

with engine.connect() as connection, connection.begin():
    data = read_sql_table('delivery', connection, schema='tableau')

data.info()
