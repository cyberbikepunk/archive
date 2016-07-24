'''
Created on Jun 12, 2015

@author: nicolas

This script shows how to connect and call XML RPC methods to retrieve objects
from an Odoo online instance
'''


import xmlrpc.client as xc
import pprint

pp = pprint.PrettyPrinter(indent=4)


# Hard-coded. Uses the toy testorg Odoo instance.
url = 'https://testorg.odoo.com'
db = 'testorg'

# The username MUST be the one listed in the "Login" column of the
# Settings/Users list
username = 'admin'

# The password MUST have been set first from the Settings/Users page + click
# on the user + "More" menu --> Change Password
password = 'thisisatest'  # We don't care about this password going to Github


def main():

    # The /xmlrpc/2/common endpoint is used only for authentication
    common = xc.ServerProxy('{}/xmlrpc/2/common'.format(url))
    pp.pprint(common.version())

    # pp.pprint(common.system.listMethods())  # Odoo does not support this

    # This is the only documented method
    uid = common.authenticate(db, username, password, {})
    print('UID = {}'.format(uid))

    # The "xmlrpc/2/object" endpoint is used to call methods of Odoo models
    models = xc.ServerProxy('{}/xmlrpc/2/object'.format(url))

    # Odoo uses only one XML RPC method (execute_kw). The final method that
    # will be executed on the object is called here "method" (to be consistent
    # with Odoo docs) and its name is passed as a parameter to the execute_kw
    # XML RPC method. The positional and keyword args (if present) will be
    # passed directly to the final method.
    model = "res.partner"
    method = "check_access_rights"
    method_args = ['read']  # The operation we want to check access to
    method_kwargs = {'raise_exception': False}

    accessible = models.execute_kw(db, uid, password,
                                   model, method, method_args, method_kwargs)
    print('Are res.partner objects accessible for reading? {}'.
          format(accessible))

    # Simple search example with AND filter
    method = "search"
    method_args = [[['is_company', '=', True], ['customer', '=', True],
                    ['name', 'like', '']]]
    method_kwargs = {'limit': 2}

    ids = models.execute_kw(db, uid, password,
                            model, method, method_args, method_kwargs)
    print('IDs found: {}'.format(ids))

    method = "read"
    method_args = [ids]

    records = models.execute_kw(db, uid, password, model, method, method_args)

    print('Found {} records with {} fields each'.format(len(records),
                                                        len(records[0])))
    print('First record:')
    pp.pprint(records[0])
    pp.pprint(records[1])

if __name__ == '__main__':
    main()
