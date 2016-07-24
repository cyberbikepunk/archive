-- for christian for the german payroll
SELECT
    driver_username AS driver_app_username,
    fleet_city,
    round(sum(air_distance_to_customer) / 1000) AS air_distance_km,
    round(sum(distance_to_customer) / 1000) AS distance_km,
    count(*) as number_of_deliveries
FROM tableau.delivery_dwh
WHERE fleet_country_code = 'DE'
    AND gastronomic_day > '2015-12-31'
    AND gastronomic_day < '2016-02-01'
    AND (
        delivery_dwh.last_delivery_status = 'done'
        OR (
            cancellation_reason = 'Order delivered'
            AND last_delivery_status = 'cancelled'
        )
    )
GROUP BY
    fleet_city,
    driver_username
ORDER BY
    COUNT (*)
DESC;
