CREATE OR REPLACE VIEW ball.v_team
AS 
	SELECT 
		a.team_id
		,a.league_id
		,a.abbr
		,a.team_concat
		,a.name
		,a.location
		,a.conf
		,a.div
		,a.lat
		,a.lon
		,a.weather
		,w.region_lat
		,w.region_lon
		,e.meta_value::int as espn_team_id
		,c.meta_value::int as cfbd_team_id
		,p.meta_value as odds_team_name
		,k.meta_value as kalshi_team_name
		-- SELECT *
	from ball.team a
	join (
			select w.location
				,avg(w.lat)::numeric(6,3) as region_lat
				,avg(w.lon)::numeric(6,3) as region_lon
			from ball.team w
			group by w.location
		) w on a.location = w.location
	left join ball.meta e on a.team_id = e.meta_keyid and e.meta_type = 'source_team_id' and e.meta_source = 'espn'
	left join ball.meta c on a.team_id = c.meta_keyid and c.meta_type = 'source_team_id' and c.meta_source = 'cfbd'
	left join ball.meta p on a.team_id = p.meta_keyid and p.meta_type = 'team_name' and p.meta_source = 'the_odds_api'
	left join ball.meta k on a.team_id = k.meta_keyid and k.meta_type = 'team_name' and k.meta_source = 'kalshi'	
	;