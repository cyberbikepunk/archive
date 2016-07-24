CREATE TABLE tableau.tracking_points (
  accuracy DOUBLE PRECISION,
  altitude DOUBLE PRECISION,
  battery DOUBLE PRECISION,
  bearing DOUBLE PRECISION,
  created_at TIMESTAMP,
  datetime TIMESTAMP,
  deleted_at TIMESTAMP,
  device TEXT,
  driver CHAR(36),
  gsm_signal DOUBLE PRECISION,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  location_provider TEXT,
  modified_at TEXT,
  network_type TEXT,
  num_satelites TEXT,
  route TEXT,
  shift TEXT,
  speed DOUBLE PRECISION,
  ts_milisecond DOUBLE PRECISION,
  uuid CHAR(36)
);

SELECT datetime from tableau.tracking_points ORDER BY datetime DESC LIMIT 1;
