""" Read SQL statements from file.

This is a utility class that helps to seperate SQL statements from python code.
To inject a dynamic field, use %(field)s. Check out '/assets/sql/sqlreader.sql'
or read the docs: http://initd.org/psycopg/docs/usage.html. Make sure that
PyCharm's dynamic field auto-detection is on: Settings > Tools > Database >
User parameters. Use '%\(\w+\)s' as regex. When you execute the SQL manually,
PyCharm will ask you for parameters.

"""


from os.path import join
from collections import UserList

from sqlparse import split

from common.settings import ROOT_DIR


class SQLReader(object):
    def __init__(self, module=None, filepath=None):
        assert module or filepath, 'Specify either a module or a filepath'

        if module:
            self.filepath = join(ROOT_DIR, *module.split('.')) + '.sql'
        elif filepath:
            self.filepath = filepath

        with open(self.filepath) as f:
            self.statements = split(f.read())

    def __repr__(self):
        return '<SQLReader (%s)>' % self.filepath


def sql(module):
    filepath = join(ROOT_DIR, 'sql', module) + '.sql'
    with open(filepath) as f:
        text = f.read()
    return split(text)


if __name__ == '__main__':
    reader = SQLReader(module='assets.sql.test_sqlreader')

    print(reader.statements[0])
    print(reader.statements[:])

    for i, sql in enumerate(reader.statements):
        print('Statement', i)
        print(sql)
