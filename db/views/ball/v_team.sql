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
		,e.meta_value as espn_team_id
		,c.meta_value as cfbd_team_id
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
	;