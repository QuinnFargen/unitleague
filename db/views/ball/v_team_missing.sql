CREATE OR REPLACE VIEW ball.v_team_missing
AS 
	SELECT 
		o.meta_keyid::int as team_id
		,o.league_id
		,null::varchar(10) as abbr
		,null::varchar(20) as team_concat
		,o.meta_key::varchar(50) as name
		,null::varchar(50) as location
		,null::varchar(20) as conf
		,null::varchar(20) as div
		,null::numeric(8,6) as lat
		,null::numeric(9,6) as lon
		,null as weather
		,null::numeric(6,3) as region_lat
		,null::numeric(6,3) as region_lon
		,o.meta_value::int as espn_team_id
		,null as cfbd_team_id
		,null as odds_team_name
		-- select *
	from ball.meta o
	where o.meta_source = 'espn' and o.meta_type = 'source_team_id'
		and o.meta_keyid % 10000 = 0 -- not in ball.team
;