
delete from tableau.hourly_deliveries;
-- Left outer join of:
-- 1.- Add the rows for the period (needed because next query does not return rows for hours when there are 0 orders)
-- 2.- The aggregated data from public.delivery
-- This is surprisingly fast (and without indexes!!!)
-- Do all at once
-- Important! Update the dates in these lines:
--     generate_series ('2015-06-01'::timestamp, '2015-10-31'::timestamp, '1 day'::interval) dd,
--  where gastronomic_day >= '2015-06-01'
--  and gastronomic_day < '2015-10-31'
insert into tableau.hourly_deliveries
(
fleet_backend_name,
gastronomic_day,
hour,
actual_datetime ,
deliveries_created ,
deliveries_done ,
deliveries_cancelled_as_done ,
total_deliveries_done
)


select row_generator.fleet, row_generator.gastronomic_day, row_generator.hour, row_generator.actual_datetime,
  coalesce(agg_deliveries.num_deliveries, 0) as deliveries_created,
  coalesce(agg_deliveries.dones, 0) as deliveries_done,
  coalesce(agg_deliveries.canc_dones, 0) as deliveries_cancelled_as_done,
  coalesce(agg_deliveries.total_dones, 0) as total_deliveries_done

FROM

(

  SELECT date_trunc('day', dd)::date as gastronomic_day,
    hh as hour,
    CASE WHEN hh >= 8 THEN dd ELSE dd + '1 day'::interval END + cast(to_char(hh, '99')||':00' AS time without time zone) as actual_datetime,
    fleets_table.fleet as fleet
  FROM
    generate_series ('2015-06-01'::timestamp, '2015-10-31'::timestamp, '1 day'::interval) dd,
    generate_series(0, 23, 1) hh,
    (select distinct fleet as fleet, min(gastronomic_day) as first_day, max(gastronomic_day) as last_day from public.delivery group by fleet) as fleets_table
  WHERE date_trunc('day', dd)::date >= fleets_table.first_day and date_trunc('day', dd)::date <= fleets_table.last_day

) AS row_generator

LEFT OUTER JOIN

(
  select date(gastronomic_day) as gastronomic_day,
  date_part('hour', created_at_timestamp) as hour,
  fleet,
  date_trunc('hour', created_at_timestamp) as actual_datetime,
  count(*) as num_deliveries,
  count(CASE WHEN last_delivery_status = 'done' THEN 1 ELSE NULL END) as dones,
  count(CASE WHEN last_delivery_status = 'cancelled' and cancellation_reason = 'Order delivered' THEN 1 ELSE NULL END) as canc_dones,
  count(CASE WHEN last_delivery_status = 'done' OR (last_delivery_status = 'cancelled' and cancellation_reason = 'Order delivered') THEN 1 ELSE NULL END) as total_dones
  from public.delivery
  where gastronomic_day >= '2015-06-01'
  and gastronomic_day < '2015-10-31'
  and driver_username NOT LIKE '%demo%'
  and driver_username NOT LIKE '%test%'
  and driver_username NOT LIKE '%valk%'
  and cancellation_reason <> 'Test Order'
  group by fleet, date(gastronomic_day), date_part('hour', created_at_timestamp), date_trunc('hour', created_at_timestamp)
) AS agg_deliveries

ON (row_generator.gastronomic_day = agg_deliveries.gastronomic_day
  AND row_generator.hour = agg_deliveries.hour
  AND row_generator.fleet = agg_deliveries.fleet)
;


UPDATE tableau.hourly_deliveries SET fleet_uuid =
    (SELECT uuid FROM tableau.fleet
     WHERE tableau.fleet.backend_name = tableau.hourly_deliveries.fleet_backend_name);

UPDATE tableau.hourly_deliveries SET fleet_display_name =
    (SELECT display_name FROM tableau.fleet
     WHERE tableau.fleet.backend_name = tableau.hourly_deliveries.fleet_backend_name);

UPDATE tableau.hourly_deliveries SET fleet_country_code =
    (SELECT country_code FROM tableau.fleet
     WHERE tableau.fleet.backend_name = tableau.hourly_deliveries.fleet_backend_name);

UPDATE tableau.hourly_deliveries SET fleet_country_name =
    (SELECT country_name FROM tableau.fleet
     WHERE tableau.fleet.backend_name = tableau.hourly_deliveries.fleet_backend_name);

