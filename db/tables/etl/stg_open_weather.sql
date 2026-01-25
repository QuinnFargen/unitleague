CREATE TABLE IF NOT EXISTS etl.stg_open_weather (
  lat decimal(8,6) not NULL,
  lon decimal(9,6) not NULL,
  current_epoch bigint not NULL,
  current_json json,
  daily_json json
);