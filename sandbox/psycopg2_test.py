'''
Created on Jul 28, 2015

@author: nicolasguenon
'''
import argparse
import pprint

pp = pprint.PrettyPrinter(indent=4)


def parse_args():

    parser = argparse.ArgumentParser(
            description='Test connection to PostgreSQL DB',
            epilog='')

    parser.add_argument('-u', '--username', action='store',
                        dest='username', required=True,
                        help='user to log into the backend')
    parser.add_argument('-p', '--password', action='store',
                        dest='password', required=True,
                        help='password to log into the backend')
    parser.add_argument('--host', action='store',
                        dest='host', required=False,
                        help='host name or IP address')
    parser.add_argument('--db', '--database', action='store',
                        dest='database', required=False,
                        help='Database name')
    parsed_args = parser.parse_args()
    return parsed_args


def test_psycopg2(username, password, host, database):
    import psycopg2

    if host is None:
        host = 'bi-live-mon.deliveryhero.com'
    if database is None:
        database = 'valk_fleet'
    try:
        conn = psycopg2.connect(("dbname='{}' host='{}' "
                                 "user='{}' password='{}'").
                                format(database, host, username, password))
    except:
        print("I am unable to connect to the database")
    cur = conn.cursor()
    cur.execute("SELECT * FROM tableau.planday_hourly_drivers LIMIT 10;")
    result = cur.fetchone()
    pp.pprint(result)
    cur.close()
    conn.close()


def main(parsed_args):
    test_psycopg2(parsed_args.username, parsed_args.password,
                  parsed_args.host, parsed_args.database)


if __name__ == '__main__':

    parsed_args = parse_args()
    main(parsed_args)
