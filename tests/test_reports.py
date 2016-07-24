""" Tests for the billing_reports module. """


from datetime import date
from reports.__main__ import get_defaut_time_interval


def test_latest_time_period(self):
    assert get_defaut_time_interval(today=date(2016, 1, 1)) == [date(2015, 12, 16), date(2015, 12, 31)]
    assert get_defaut_time_interval(today=date(1975, 3, 25)) == [date(1975, 3, 1), date(1975, 3, 15)]
    assert get_defaut_time_interval(today=date(2012, 3, 3)) == [date(2012, 2, 16), date(2012, 2, 29)]  # Leap year
