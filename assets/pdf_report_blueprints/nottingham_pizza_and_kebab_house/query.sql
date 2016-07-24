SELECT gastronomic_day,
  last_delivery_status_timestamp,
  restaurant_name,
  fleet_city,
  requested_pickup_timestamp,
  cancellation_reason,
  customer_raw_address,
  confirmed_pickup_timestamp,
  last_delivery_status,
  start_route_timestamp
FROM tableau.delivery_dwh
WHERE gastronomic_day::date >= %(start_date)s::date
  AND gastronomic_day::date <= %(stop_date)s::date
  AND restaurant_name='Notts Pizza & Kebabs';
