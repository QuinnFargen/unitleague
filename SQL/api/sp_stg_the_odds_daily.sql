CREATE OR REPLACE PROCEDURE api.sp_stg_the_odds_daily()
 LANGUAGE plpgsql
AS $procedure$
begin


	CREATE TEMP TABLE ODDS_DAILY AS
		select 
			1::bigint as "THE_ODDs_API_ID",
			a."id" as "id_temp",
			a."sport_key",
			a."sport_title",
			a."commence_time",
			a."home_team",
			a."away_team",
			a."full_json",
  			now() as "INSERT_DT_temp"
		from "SPORT"."api"."stg_the_odds_api" a;

--	SELECT * FROM ODDS_DAILY;

	INSERT INTO "SPORT"."api"."the_odds_api" ( "id", "sport_key", "sport_title", "commence_time", "home_team", "away_team", "full_json", "INSERT_DT" )
		SELECT 
			a."id_temp",
			a."sport_key",
			a."sport_title",
			a."commence_time",
			a."home_team",
			a."away_team",
			a."full_json",
  			a."INSERT_DT_temp"
		FROM ODDS_DAILY a;

	
	UPDATE ODDS_DAILY 
	SET "THE_ODDs_API_ID" = b."THE_ODDs_API_ID"
	FROM "SPORT"."api"."the_odds_api" b
	WHERE "id_temp" = b."id" and "INSERT_DT_temp" = b."INSERT_DT";
	

	CREATE TEMP TABLE ODDS_DAILY_BETS AS
		SELECT o."THE_ODDs_API_ID",
	    o.id,
	    o.bookmakers_key,
	    o.bookmakers_title,
	    o.markets_key,
	    o.markets_last_update,
	    b.value::json ->> 'name'::text AS name,
	    b.value::json ->> 'price'::text AS price,
	    b.value::json ->> 'point'::text AS point
	   FROM ( SELECT m."THE_ODDs_API_ID",
	            m.id,
	            m.home_team,
	            m.away_team,
	            m.commence_time,
	            m.bookmakers_key,
	            m.bookmakers_title,
	            m.markets,
	            m.markets ->> 'key'::text AS markets_key,
	            m.markets ->> 'last_update'::text AS markets_last_update,
	            m.markets ->> 'outcomes'::text AS markets_outcomes
	           FROM ( SELECT b_1."THE_ODDs_API_ID",
	                    b_1.id,
	                    b_1.home_team,
	                    b_1.away_team,
	                    b_1.commence_time,
	                    b_1.bookmakers_key,
	                    b_1.bookmakers_title,
	                    m_1.value::json AS markets
	                   FROM ( SELECT b_2."THE_ODDs_API_ID",
	                            b_2.id,
	                            b_2.home_team,
	                            b_2.away_team,
	                            b_2.commence_time,
	                            b_2.bookmaker,
	                            b_2.bookmaker ->> 'key'::text AS bookmakers_key,
	                            b_2.bookmaker ->> 'title'::text AS bookmakers_title,
	                            b_2.bookmaker ->> 'markets'::text AS bookmakers_markets
	                           FROM ( SELECT oa."THE_ODDs_API_ID",
	                                    oa.id_temp as "id",
	                                    oa.home_team,
	                                    oa.away_team,
	                                    oa.commence_time,
	                                    b_3.value::json AS bookmaker
	                                   FROM ODDS_DAILY oa,
	                                    LATERAL json_array_elements_text(oa.full_json) b_3(value)
	                                  ) b_2) b_1,
	                    LATERAL json_array_elements_text(b_1.bookmakers_markets::json) m_1(value)) m) o,
	    LATERAL json_array_elements_text(o.markets_outcomes::json) b(value);

	
	INSERT INTO "SPORT"."api"."the_odds_api_bet" ("THE_ODDs_API_ID", "id", "bookmakers_key", "bookmakers_title", "markets_key", "markets_last_update", "name", "price", "point")
		SELECT 
			a."THE_ODDs_API_ID",
		    a.id,
		    a.bookmakers_key,
		    a.bookmakers_title,
		    a.markets_key,
		    a.markets_last_update,
		    a."name",
		    a."price",
		    a."point"
		FROM ODDS_DAILY_BETS a;

	UPDATE "SPORT"."api"."the_odds_api" A
	SET "IMPORTED" = '1'::bit
	FROM ( SELECT DISTINCT A."THE_ODDs_API_ID" AS "IMP_THE_ODDs_API_ID" FROM ODDS_DAILY_BETS A )  
	WHERE "THE_ODDs_API_ID" = "IMP_THE_ODDs_API_ID";

	TRUNCATE TABLE "SPORT"."api"."stg_the_odds_api";


end; $procedure$
;
