CREATE OR REPLACE VIEW etl.v_fetch_game_latlon
as	select 
		distinct 
		t.region_lat
		, t.region_lon
	from ball.game g 
	join ball.v_team t on g.home_team_id = t.team_id
	where g.game_dt between CURRENT_DATE and CURRENT_DATE + (case when t.weather = 1 then 1 else 7 end)
  ;