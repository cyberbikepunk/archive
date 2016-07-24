SELECT gastronomic_day,
  last_delivery_status_timestamp,
  restaurant_name,
  fleet_city,
  cancellation_reason,
  customer_raw_address,
  air_distance_to_customer,
  last_delivery_status,
  start_route_timestamp
FROM tableau.delivery_dwh
WHERE gastronomic_day::date >= %(start_date)s::date
  AND gastronomic_day::date <= %(stop_date)s::date
  and fleet_country_code='UK';