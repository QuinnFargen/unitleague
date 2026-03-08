CREATE OR REPLACE VIEW src.v_weather_forecast AS
SELECT 
    A.lat,
    A.lon,
    A.daily_epoch,
    to_timestamp(A.daily_epoch)::date AS daily_dt,
    to_timestamp(A.daily_epoch)::date - A.current_dt AS num_days_ahead,
    (A.weather->>'id')::smallint AS weather_id,
    A.temp_min::decimal(6,3),
    A.temp_max::decimal(6,3),
    A.temp_morn::decimal(6,3),
    A.temp_eve::decimal(6,3),
    A.wind_speed::decimal(6,3),
    A.snow_vol_mm::decimal(6,3),
    A.rain_vol_mm::decimal(6,3)
FROM (
    SELECT 
        O.lat,
        O.lon,
        O.current_epoch,
        O.current_dt,
        (d->>'dt')::bigint AS daily_epoch,
        d#>>'{temp,min}' AS temp_min,
        d#>>'{temp,max}' AS temp_max,
        d#>>'{temp,morn}' AS temp_morn,
        d#>>'{temp,eve}' AS temp_eve,
        d->>'wind_speed' AS wind_speed,
        d->>'snow' AS snow_vol_mm,
        d->>'rain' AS rain_vol_mm,
        trim(d->>'weather','[]')::json AS weather
        -- select *
    FROM etl.all_open_weather O
    CROSS JOIN LATERAL json_array_elements(O.daily_json::json) AS d
    WHERE O.imported = B'0'
) A;