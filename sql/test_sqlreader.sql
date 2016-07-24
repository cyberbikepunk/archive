SELECT *
FROM tableau.planday_hourly_drivers LIMIT 10;
SELECT DISTINCT %(columns)s
FROM delivery
WHERE %(datestamp)s::date >= %(start)s::date
  AND %(datestamp)s::date <= %(stop)s::date;
SELECT DISTINCT restaurant_uuid,
                restaurant_name
FROM delivery
WHERE gastronomic_day::date >= '2015-12-01'::date
  AND gastronomic_day::date <= '2015-12-31'::date;