""" You can compose columns using Pandas. See output.pdf for the result. """

from pandas.tslib import Timestamp

from scripts.pdf_report_factory import BaseCustomizer, catch_query_mismatch


def pretty_date(x):
    return str(x) if isinstance(x, Timestamp) else ''

def pretty_time(x):
    return x.strftime('%H:%M') if isinstance(x, Timestamp) else ''

def pretty_km(x):
    return '{:3.1f}'.format(x / 1000)

def is_done(x):
    return True if x == 'done' else False

def is_delivered(x):
    return True if x == 'Order delivered' else False

def is_cancelled(x):
    return True if x == 'cancelled' else False


@catch_query_mismatch
class Customizer(BaseCustomizer):
    ordered_columns = (
        'gastronomic_day',
        'delivered',
        'pickup_day',
        'pickup_time',
        'customer_address',
        'air_distance'
    )

    def gastronomic_day(self):
        return self.data.gastronomic_day

    def customer_address(self):
        return self.data.customer_raw_address

    def air_distance(self):
        return self.data.air_distance_to_customer.map(pretty_km) + ' km'

    def pickup_time(self):
        return self.data.start_route_timestamp.map(pretty_time)

    def pickup_day(self):
        return self.data.gastronomic_day.map(pretty_date)

    @property
    def _status(self):
        done = self.data.last_delivery_status.map(is_done)
        corrected = self.data.cancellation_reason.map(is_delivered)
        cancelled = self.data.last_delivery_status.map(is_cancelled)
        return done | (cancelled & corrected)

    def delivered(self):
        return self._status.map(lambda x: 'Delivered' if x else 'CANCELLED')

