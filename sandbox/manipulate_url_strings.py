""" murls (Mutable URL Strings): an expressive way to manipulate URLs. """


from collections import UserString


class Url(UserString):
    """ The base class for Mutable URL Strings. """

    _specs = ('{schema}://{host}{host_slash}'
              '{path}{path_slash}'
              '{question_mark}{query}'
              '{hash}{fragment}')

    def __init__(self, *args, **kwargs):
        super(Url, self).__init__(*args, **kwargs)

        self._schema = str()
        self._host = str()
        self._path = list()
        self._path_slash = False
        self._query = dict()
        self._fragment = str()

    def host(self, host):
        self._host = host
        self.data = self._build()
        return self

    def schema(self, schema):
        self._schema = schema
        self.data = self._build()
        return self

    def fragment(self, fragment):
        self._fragment = fragment
        self.data = self._build()
        return self

    def path(self, *path, slash=False):
        self._path = path
        if slash is True:
            self._path_slash = True
        else:
            self._path_slash = False
        self.data = self._build()
        return self

    def query(self, **parameters):
        self._query = dict(**parameters)
        self.data = self._build()
        return self

    @property
    def _parts(self):
        return {
            'schema': self._schema,
            'host': self._host,
            'host_slash': self._host_slash,
            'path': '/'.join(self._path),
            'path_slash': '/' if self._path_slash else '',
            'question_mark': self._question_mark,
            'query': '&'.join(['{k}={v}'.format(k=k, v=v) for k, v in self._query.items()]) if self._query else '',
            'hash': self._hash,
            'fragment': self._fragment
        }

    def _build(self):
        return self._specs.format(**self._parts)

    @property
    def _hash(self):
        return '#' if self._fragment else ''

    @property
    def _question_mark(self):
        return '?' if self._query else ''

    @property
    def _host_slash(self):
        return '/' if self._path else ''


class Http(Url):
    def __init__(self, *args, **kwargs):
        super(Http, self).__init__(*args, **kwargs)
        self._schema = 'http'
        self._host = args[0]
        self.data = self._build()


class Https(Url):
    def __init__(self, *args, **kwargs):
        super(Https, self).__init__(*args, **kwargs)
        self._schema = 'https'
        self._host = args[0]
        self.data = self._build()


if __name__ == '__main__':
    url = Http('foo.bar').path('foo').query(foo='bar')
    print(url)
