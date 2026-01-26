CREATE OR REPLACE VIEW etl.v_all_the_odds_api
AS 
	SELECT 
		a.id,
		a.sport_key,
		a.sport_title,
		a.commence_time,
		a.home_team,
		a.away_team,
		a.full_json
		-- SELECT *
	from etl.stg_the_odds_api a;