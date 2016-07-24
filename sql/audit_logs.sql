CREATE TABLE tableau.audit_logs (
  id BIGSERIAL PRIMARY KEY,
  event TEXT,
  username TEXT,
  timestamp TIMESTAMP,
  created_at TIMESTAMP,
  modified_at TIMESTAMP,
  deleted_at TIMESTAMP,
  uuid TEXT,
  driver TEXT,
  fleet_controller_uuid TEXT,
  user_uuid TEXT,
  delivery TEXT
);

SELECT "timestamp" FROM tableau.audit_logs ORDER BY "timestamp" DESC LIMIT 1;
SELECT "timestamp" FROM {myschema}.{mytable} ORDER BY "timestamp" DESC LIMIT 1;

select
    LOWER(CONCAT(LEFT(HEX(driver.uuid), 8), '-', MID(HEX(driver.uuid), 9,4), '-', MID(HEX(driver.uuid), 13,4), '-', MID(HEX(driver.uuid), 17,4), '-', RIGHT(HEX(driver.uuid), 12))) as driver_uuid,
    concat(driver.first_name, ' ', driver.last_name) as driver_name,
    fleet.name
from core.driver, core.fleet
where core.fleet.uuid = core.driver.fleet_uuid;

DELETE from tableau.audit_logs;

SELECT *
from tableau.audit_logs as assignDelivery,
    tableau.audit_logs as assignToDriver,
    tableau.audit_logs as confirmPickup,
    tableau.audit_logs as cancelDelivery,
    tableau.audit_logs as reassignDelivery,
    tableau.audit_logs as unassignDelivery,
    tableau.delivery_dwh as dwh
WHERE assignDelivery.delivery_uuid = dwh.delivery_uuid
and assignToDriver.delivery_uuid = dwh.delivery_uuid
and confirmPickup.delivery_uuid = dwh.delivery_uuid
and cancelDelivery.delivery_uuid = dwh.delivery_uuid
and reassignDelivery.delivery_uuid = dwh.delivery_uuid
and unassignDelivery.delivery_uuid = dwh.delivery_uuid
and assignDelivery.event = 'assignDelivery-clicked'
and assignToDriver.event = 'assignToDriver-clicked'
and confirmPickup.event = 'confirmPickupTime-clicked'
and cancelDelivery.event = 'cancelDelivery-clicked'
and reassignDelivery.event = 'reassignDelivery-clicked'
and unassignDelivery.event = 'unassignDelivery-clicked'
and assignDelivery.delivery_uuid = 'cf0c84cb-4cc9-49ec-bcce-6db983045660';

SELECT *
from tableau.delivery_dwh as dwh LEFT OUTER JOIN tableau.audit_logs as assignDelivery ON (dwh.delivery_uuid = assignDelivery.delivery_uuid)
     LEFT OUTER JOIN tableau.audit_logs as assignToDriver ON (dwh.delivery_uuid = assignToDriver.delivery_uuid)
and assignDelivery.event =  'assignDelivery-clicked'
and assignToDriver.event =  'assignToDriver-clicked';

-- this query is used in tableau as the fleet control datasource
SELECT delivery.delivery_uuid,
    delivery.order_uuid,
    event.username,
    event.event,
    event.timestamp,
    delivery.fleet_backend_name,
    delivery.source_name,
    delivery.restaurant_name,
    delivery.restaurant_city,
    delivery.driver_username,
    delivery.customer_zipcode,
    delivery.customer_raw_address,
    delivery.distance_to_customer,
    delivery.last_delivery_status,
    delivery.last_delivery_status_timestamp,
    delivery.created_at_timestamp,
    delivery.accepted_timestamp,
    delivery.at_restaurant_timestamp,
    delivery.start_route_timestamp,
    delivery.requested_pickup_timestamp,
    delivery.confirmed_pickup_timestamp,
    delivery.pick_up_eta,
    delivery.delivery_at_eta,
    delivery.cancellation_reason,
    delivery.gastronomic_day,
    delivery.air_distance_to_customer,
    delivery.customer_lat,
    delivery.customer_lng,
    delivery.created_at_hour,
    delivery.fleet_display_name,
    delivery.fleet_country_code,
    delivery.fleet_country_name
from tableau.audit_logs as event,
    tableau.delivery_dwh as delivery
WHERE event.delivery_uuid = delivery.delivery_uuid;


GRANT SELECT ON tableau.audit_logs TO valkfleet_ro;
GRANT SELECT ON tableau.delivery_dwh TO valkfleet_ro