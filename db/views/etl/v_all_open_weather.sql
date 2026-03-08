CREATE OR REPLACE VIEW etl.v_all_open_weather
AS SELECT a.lat,
    a.lon,
    a.current_epoch,
    to_timestamp(a.current_epoch::double precision)::date AS current_dt,
    a.current_json,
    a.daily_json
   FROM etl.stg_open_weather a
     LEFT JOIN etl.all_open_weather b ON a.lat = b.lat AND a.lon = b.lon AND to_timestamp(a.current_epoch::double precision)::date = b.current_dt
  WHERE b.openweather_api_id IS NULL;