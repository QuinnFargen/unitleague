CREATE OR REPLACE VIEW etl.v_all_the_odds_api_bet
AS 
	SELECT
		oa.the_odds_api_id,
		oa.id AS id,
		oa.sport_key,
		oa.sport_title,
		oa.commence_time,
		oa.home_team,
		oa.away_team,
		bm ->> 'key'          AS bookmakers_key,
		bm ->> 'title'        AS bookmakers_title,
		mk ->> 'key'          AS markets_key,
		mk ->> 'last_update'  AS markets_last_update,
		oc ->> 'name'         AS name,
		oc ->> 'price'        AS price,
		oc ->> 'point'        AS point
	FROM etl.all_the_odds_api oa
	CROSS JOIN LATERAL jsonb_array_elements(oa.full_json::jsonb) AS bm
	CROSS JOIN LATERAL jsonb_array_elements(bm -> 'markets') AS mk
	CROSS JOIN LATERAL jsonb_array_elements(mk -> 'outcomes') AS oc
	where oa.imported = false ;