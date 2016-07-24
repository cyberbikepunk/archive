""" Apply a decorator to mulitple class methods. """
from functools import wraps

from reports.factory import ReportError


def catch_query_mismatch(cls):
    def decorate(getter):
        @wraps(getter)
        def wrapper(self):
            try:
                return getter(self)
            except KeyError:
                raise ReportError('hello')
        return wrapper

    for method_name in cls.wanted:
            setattr(cls, method_name, decorate(cls.__dict__[method_name]))
    return cls


@catch_query_mismatch
class Spam(object):
    defined = {'foo': 'foo'}
    wanted = 'foo', 'bar'

    def foo(self):
        return self.defined['foo']

    def bar(self):
        return self.defined['bar']


if __name__ == '__main__':
    spam = Spam()
    spam.foo()
    spam.bar()


