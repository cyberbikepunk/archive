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

def status(x):
    return 'Delivered' if x else 'CANCELLED'


@catch_query_mismatch
class Customizer(BaseCustomizer):
    ordered_columns = (
        'gastronomic_date',
        'requested_pickup_time',
        'pickup_time',
        'delivery_time',
        'delivered',
        'customer_address',
    )

    def gastronomic_date(self):
        return self.data.gastronomic_day

    def customer_address(self):
        return self.data.customer_raw_address

    def requested_pickup_time(self):
        return self.data.requested_pickup_timestamp.map(pretty_time)

    def pickup_time(self):
        return self.data.start_route_timestamp.map(pretty_time)

    def delivery_time(self):
        return self.data.last_delivery_status_timestamp.map(pretty_time)

    @property
    def _status(self):
        done = self.data.last_delivery_status.map(is_done)
        corrected = self.data.cancellation_reason.map(is_delivered)
        cancelled = self.data.last_delivery_status.map(is_cancelled)
        return done | (cancelled & corrected)

    def delivered(self):
        return self._status.map(status)

