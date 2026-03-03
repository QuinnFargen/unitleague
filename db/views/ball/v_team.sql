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
	union
		-- brings in the rest of the teams that aren't in ball.team yet, but duplicates the tbd team_id
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
	