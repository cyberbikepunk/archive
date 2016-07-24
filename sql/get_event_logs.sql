CREATE TABLE IF NOT EXISTS {schema_}.{table_} (
  id BIGSERIAL PRIMARY KEY,
  event CHAR(36),
  username TEXT,
  fleet_controller CHAR(36),
  "timestamp" TIMESTAMP,
  created_at TIMESTAMP,
  modified_at TIMESTAMP,
  deleted_at TIMESTAMP,
  uuid CHAR(36),
  driver CHAR(36),
  {metadata} CHAR(36),
  user_uuid CHAR(36),
  delivery CHAR(36),
  etl_timestamp TIMESTAMP
);


SELECT {time_field}
FROM {schema_}.{table_}
ORDER BY {time_field}
DESC LIMIT 1;

GRANT SELECT ON {schema_}.{table_} TO valkfleet_ro;