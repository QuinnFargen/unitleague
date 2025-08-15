CREATE OR REPLACE PROCEDURE api.sp_stg_the_odds_daily()
 LANGUAGE plpgsql
AS $procedure$
begin


	CREATE TEMP TABLE ODDS_DAILY AS
		select 
			1::bigint as the_odds_api_id,
			a.id as id_temp,
			a.sport_key,
			a.sport_title,
			a.commence_time,
			a.home_team,
			a.away_team,
			a.full_json,
  			now() as insert_dt_temp
		from api.stg_the_odds_api a;

--	SELECT * FROM ODDS_DAILY;

	INSERT INTO api.the_odds_api ( id, sport_key, sport_title, commence_time, home_team, away_team, full_json, insert_dt )
		SELECT 
			a.id_temp,
			a.sport_key,
			a.sport_title,
			a.commence_time,
			a.home_team,
			a.away_team,
			a.full_json,
  			a.insert_dt_temp
		FROM ODDS_DAILY a;

	
	UPDATE ODDS_DAILY 
	SET the_odds_api_id = b.the_odds_api_id
	FROM api.the_odds_api b
	WHERE id_temp = b.id and insert_dt_temp = b.insert_dt;
	

	CREATE TEMP TABLE ODDS_DAILY_BETS AS
		SELECT o.the_odds_api_id,
	    o.id,
	    o.bookmakers_key,
	    o.bookmakers_title,
	    o.markets_key,
	    o.markets_last_update,
	    b.value::json ->> 'name'::text AS name,
	    b.value::json ->> 'price'::text AS price,
	    b.value::json ->> 'point'::text AS point
	   FROM ( SELECT m.the_odds_api_id,
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
	           FROM ( SELECT b_1.THE_ODDs_API_ID,
	                    b_1.id,
	                    b_1.home_team,
	                    b_1.away_team,
	                    b_1.commence_time,
	                    b_1.bookmakers_key,
	                    b_1.bookmakers_title,
	                    m_1.value::json AS markets
	                   FROM ( SELECT b_2.THE_ODDs_API_ID,
	                            b_2.id,
	                            b_2.home_team,
	                            b_2.away_team,
	                            b_2.commence_time,
	                            b_2.bookmaker,
	                            b_2.bookmaker ->> 'key'::text AS bookmakers_key,
	                            b_2.bookmaker ->> 'title'::text AS bookmakers_title,
	                            b_2.bookmaker ->> 'markets'::text AS bookmakers_markets
	                           FROM ( SELECT oa.THE_ODDs_API_ID,
	                                    oa.id_temp as id,
	                                    oa.home_team,
	                                    oa.away_team,
	                                    oa.commence_time,
	                                    b_3.value::json AS bookmaker
	                                   FROM ODDS_DAILY oa,
	                                    LATERAL json_array_elements_text(oa.full_json) b_3(value)
	                                  ) b_2) b_1,
	                    LATERAL json_array_elements_text(b_1.bookmakers_markets::json) m_1(value)) m) o,
	    LATERAL json_array_elements_text(o.markets_outcomes::json) b(value);

	
	INSERT INTO api.the_odds_api_bet (the_odds_api_id, id, bookmakers_key, bookmakers_title, markets_key, markets_last_update, name, price, point)
		SELECT 
			a.the_odds_api_id,
		    a.id,
		    a.bookmakers_key,
		    a.bookmakers_title,
		    a.markets_key,
		    a.markets_last_update,
		    a.name,
		    a.price,
		    a.point
		FROM ODDS_DAILY_BETS a;

	UPDATE api.the_odds_api A
	SET imported = false
	FROM ( SELECT DISTINCT A.the_odds_api_id AS imp_the_odds_api_id FROM ODDS_DAILY_BETS A )  
	WHERE the_odds_api_id = imp_the_odds_api_id;

	TRUNCATE TABLE api.stg_the_odds_api;


	CREATE TEMP TABLE ODDS_GAMES_NEEDED AS
		select distinct
			a.sport_key,
			a.commence_time,
			a.commence_time::date as game_dt,
			a.home_team,
			a.away_team
		from api.the_odds_api a
		where a.game_id is null;
	
	
	CREATE TEMP TABLE ODDS_TEAM AS
	select o.*
		, coalesce(tn.league_id,tn_a.league_id) as league_id
		, coalesce(tn.team_id,tn_a.league_id * 10000) as home_team_id
		, coalesce(tn_a.team_id,tn.league_id * 10000) as away_team_id
	from ODDS_GAMES_NEEDED o
	left join ball.team_name tn on tn."source"  = 'the_odds_api'
									and tn.league_id = case when o.sport_key = 'americanfootball_ncaaf' then 5 
														when o.sport_key = 'americanfootball_nfl' then 2 else null end
									and o.home_team = tn.name
	left join ball.team_name tn_a on tn_a."source"  = 'the_odds_api'
									and tn_a.league_id = case when o.sport_key = 'americanfootball_ncaaf' then 5 
														when o.sport_key = 'americanfootball_nfl' then 2 else null end
									and o.away_team = tn_a.name
	where tn.team_id is not null or tn_a.team_id is not null;

	CREATE TEMP TABLE ODDS_WITH_GAME AS
	select o.*, g.game_id 
	from ODDS_TEAM o
	join ball.game g on o.league_id = g.league_id and o.game_dt = g.game_dt 
						and o.away_team_id = g.away_team_id and o.home_team_id = g.home_team_id;
	

	UPDATE api.the_odds_api A
	SET game_id = o.game_id
		, home_team_id = o.home_team_id
		, away_team_id = o.away_team_id
	FROM ODDS_WITH_GAME o  
	WHERE 1=1
		and a.game_id is null
		AND a.sport_key = o.sport_key
		AND a.commence_time = o.commence_time
		AND a.home_team = o.home_team
		AND a.away_team = o.away_team
	;



end; $procedure$
;
