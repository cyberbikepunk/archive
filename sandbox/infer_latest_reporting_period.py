from calendar import monthrange
from arrow import utcnow, get
from unittest import TestCase


# For clarity
END = 1
NOW = utcnow()


def latest_time_window(date=NOW):
    """ Returns a tuple of strings in the format 'dd-mm-yyyy' for the SQL query.
    :param date: an Arrow object
    """

    if date.month != 1:
        month = - 1
        year = date.year
    else:
        month = date.month
        year = date.year - 1

    if 1 <= date.day <= 15:
        start_day = dict(month=month, year=year, day=monthrange(2002, 1)[END])
        stop_day = dict(month=month, year=year, day=16)
    else:
        start_day = dict(month=month, year=year, day=1)
        stop_day = dict(month=month, year=year, day=15)

    return '{day}-{month}-{year‚}'.format(start_day), '{day}-{month}-{year‚}'.format(stop_day)


class TestBillingReport(TestCase):
    def test_latest_time_window(self):
        self.assertEquals(latest_time_window(get('01-01-2016')), ('16-12-2015', '31-12-2015'))
        self.assertEquals(latest_time_window(get('01-03-2016')), ('16-12-2015', '29-02-2016'))
        self.assertEquals(latest_time_window(get('24-03-1975')), ('01-03-1975', '15-03-1975'))

