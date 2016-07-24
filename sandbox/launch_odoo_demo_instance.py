'''
Created on Jun 12, 2015

@author: nicolas

Simple script to launch a demo Odoo instance that will stay up for 4 hours.
The host, DB name, user and password can then be used to test other scripts.
The demo database is populated with example data.
'''

import xmlrpc.client as xc
import pprint

pp = pprint.PrettyPrinter(indent=4)


def main():

    info = xc.ServerProxy('https://demo.odoo.com/start').start()
    (url, db, username, password) = (info['host'], info['database'],
                                     info['user'], info['password'])

    pp.pprint(info)

    common = xc.ServerProxy('{}/xmlrpc/2/common'.format(url))
    pp.pprint(common.version())

    # Testing if everything works
    uid = common.authenticate(db, username, password, {})
    print(uid)

    models = xc.ServerProxy('{}/xmlrpc/2/object'.format(url))
    result = models.execute_kw(db, uid, password,
                               'res.partner', 'check_access_rights',
                               ['read'], {'raise_exception': False})
    print(result)

    result = models.execute_kw(db, uid, password,
                               'res.partner', 'search',
                               [[['is_company', '=', True],
                                 ['customer', '=', True]]])
    print(result)

    print(2 * '\n')
    print('If we reached this point, the demo instance works.')
    print('Connection details:')
    pp.pprint(info)


if __name__ == '__main__':
    main()
