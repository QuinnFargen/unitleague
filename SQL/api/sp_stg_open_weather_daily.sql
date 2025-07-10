CREATE OR REPLACE PROCEDURE api.sp_stg_open_weather_daily()
 LANGUAGE plpgsql
AS $procedure$
begin


	INSERT INTO "SPORT"."api"."open_weather" ("LAT", "LON", "CURRENT_EPOCH", "CURRENT_DT", "CURRENT_JSON", "DAILY_JSON")
		SELECT 
			A."LAT", A."LON", A."CURRENT_EPOCH"
			,to_timestamp(A."CURRENT_EPOCH")::date AS "CURRENT_DT"
			,A."CURRENT_JSON"
			,A."DAILY_JSON"
		FROM "SPORT"."api"."stg_open_weather" A
		LEFT JOIN "api"."open_weather" B ON A."LAT" = B."LAT" AND A."LON" = B."LON" AND to_timestamp(A."CURRENT_EPOCH")::date  = B."CURRENT_DT"
		WHERE B."OPENWEATHER_API_ID" IS NULL;


	CREATE TEMP TABLE DAILY AS
		SELECT 
			A."LAT", A."LON",	-- A."CURRENT_EPOCH", A."CURRENT_DT", 
			A."DAILY_EPOCH",
			to_timestamp(A."DAILY_EPOCH")::date as "DAILY_DT",
			to_timestamp(A."DAILY_EPOCH")::date - A."CURRENT_DT" as "NUM_DAYS_AHEAD",
			(A.weather->>'id')::smallint as "WEATHER_ID",
			A."TEMP_MIN"::decimal(6,3),A."TEMP_MAX"::decimal(6,3),A."TEMP_MORN"::decimal(6,3),A."TEMP_EVE"::decimal(6,3),
			A."WIND_SPEED"::decimal(6,3),
			A."SNOW_VOL_MM"::decimal(6,3), A."RAIN_VOL_MM"::decimal(6,3)
		FROM (
			SELECT 
				O."LAT",
          		O."LON",
                O."CURRENT_EPOCH",
                O."CURRENT_DT",
				(json_array_elements_text(O."DAILY_JSON")::json->>'dt')::bigint as "DAILY_EPOCH",
				json_array_elements_text(O."DAILY_JSON")::json#>>'{temp,min}' as "TEMP_MIN",
				json_array_elements_text(O."DAILY_JSON")::json#>>'{temp,max}' as "TEMP_MAX",
				json_array_elements_text(O."DAILY_JSON")::json#>>'{temp,morn}' as "TEMP_MORN",
				json_array_elements_text(O."DAILY_JSON")::json#>>'{temp,eve}' as "TEMP_EVE",
				json_array_elements_text(O."DAILY_JSON")::json->>'wind_speed' as "WIND_SPEED",
				json_array_elements_text(O."DAILY_JSON")::json->>'snow' as "SNOW_VOL_MM",
				json_array_elements_text(O."DAILY_JSON")::json->>'rain' as "RAIN_VOL_MM",
				trim(json_array_elements_text(O."DAILY_JSON")::json->>'weather','[]')::json as weather
	       	FROM "api"."open_weather" O
            WHERE O."IMPORTED" = '0'::"bit"
			) A;


	INSERT INTO "BALL"."WEATHER_DAILY"( "LAT", "LON", "DAILY_EPOCH", "DAILY_DT", "NUM_DAYS_AHEAD", "WEATHER_ID"
									, "TEMP_MIN", "TEMP_MAX" , "TEMP_MORN" , "TEMP_EVE" 
									, "WIND_SPEED", "SNOW_VOL_MM", "RAIN_VOL_MM"
									  )
		SELECT 	
			A."LAT", A."LON", A."DAILY_EPOCH", A."DAILY_DT", A."NUM_DAYS_AHEAD", A."WEATHER_ID"
			, A."TEMP_MIN", A."TEMP_MAX" , A."TEMP_MORN" , A."TEMP_EVE" 
			, A."WIND_SPEED", A."SNOW_VOL_MM", A."RAIN_VOL_MM"
		FROM DAILY A
		LEFT JOIN "BALL"."WEATHER_DAILY" B ON A."LAT" = B."LAT" AND A."LON" = B."LON" AND A."DAILY_DT" = B."DAILY_DT"
		WHERE B."WEATHER_DAILY_ID" IS NULL;


	UPDATE "SPORT"."api"."open_weather" A
	SET "IMPORTED" = '1'
	FROM ( SELECT DISTINCT A."LAT" AS "IMP_LAT", A."LON" AS "IMP_LON", A."DAILY_DT" AS "IMP_DAILY_DT" FROM DAILY A WHERE A."NUM_DAYS_AHEAD" = 0 )  
	WHERE "LAT" = "IMP_LAT" AND "LON" = "IMP_LON" AND "CURRENT_DT" = "IMP_DAILY_DT";

	TRUNCATE TABLE "SPORT"."api"."stg_open_weather";


end; $procedure$
;
