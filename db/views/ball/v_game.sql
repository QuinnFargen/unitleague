CREATE OR REPLACE VIEW ball.v_game
AS 
	SELECT 
		g.game_id
		,h.abbr as home
		,a.abbr as away
		,g.game_dt
		,g.game_time
		,g.game_concat
		,g.h
		,g.a
		,case when g.home_team_id = g.won_team_id then h.abbr
				when g.away_team_id = g.won_team_id then a.abbr else null end as winner
		,g.league_id
		,g.home_team_id
		,g.away_team_id
		,g.won_team_id
		
		-- SELECT *
	from ball.game g
	join ball.team h on g.home_team_id = h.team_id 
	join ball.team a on g.away_team_id = a.team_id 
	;