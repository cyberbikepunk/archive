""" Test the SQLReader class. """


from common.sqlreader import SQLReader


sql1 = [
    "SELECT *",
    "FROM tableau.planday_hourly_drivers LIMIT 10;"
]

sql2 = [
    "SELECT DISTINCT %(columns)s",
    "FROM delivery",
    "WHERE %(datestamp)s::date >= %(start)s::date",
    "  AND %(datestamp)s::date <= %(stop)s::date;"
]

sql3 = [
    "SELECT DISTINCT restaurant_uuid,",
    "                restaurant_name",
    "FROM delivery",
    "WHERE gastronomic_day::date >= '2015-12-01'::date",
    "  AND gastronomic_day::date <= '2015-12-31'::date;"
]

sql4 = [
    "SELECT DISTINCT restaurant_uuid, restaurant_name",
    "FROM delivery",
    "WHERE gastronomic_day::date >= '2015-12-01'::date::date",
    "  AND gastronomic_day::date <= '2015-12-31'::date::date;"
]

def test_sqlreader():
    sql = SQLReader('assets.sql.test_sqlreader')

    assert sql.statements[0] == '\n'.join(sql1)
    assert sql.statements[1] == '\n'.join(sql2)
    assert sql.statements[2] == '\n'.join(sql3)

