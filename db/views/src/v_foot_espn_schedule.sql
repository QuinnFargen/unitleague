CREATE OR REPLACE VIEW src.v_foot_espn_schedule
AS
	select distinct 
		 case when t.team_id is not null then 2 else 5 end as league_id
		,a.game_id
		,a.gamedate
		,a."name"
		,a.shortname
		,a.week_number
		,a.season_year
		,a.season_type
		,a.season_slug
		,case when a.gamedate < CURRENT_DATE and a.status_period = 0 then -1 else a.status_period end as status_period
		,a.home_score
		,a.home_team
		,a.home_abbr
		,a.home_short
		,a.home_team_id
		,a.seasontype
		,a.seasontypelabel
		,a.gameweek
		,a.weeklabel
		,a.q1_home
		,a.q2_home
		,a.q3_home
		,a.q4_home
		,a.q5_home
		,a.away_score
		,a.away_team
		,a.away_abbr
		,a.away_short
		,a.away_team_id
		,a.q1_away
		,a.q2_away
		,a.q3_away
		,a.q4_away
		,a.q5_away
		-- SELECT *
	from etl.stg_foot_espn_schedule a
	join ball.game g on a.game_id = g.source_game_id
	left join src.foot_espn_schedule s on a.game_id = s.game_id
	left join ball.v_team t on a.home_team_id = t.espn_team_id and t.league_id = 2 -- is nfl
	where (
			s.game_id is null						-- not yet in src or
		 	or a.status_period <> s.status_period 	-- game has completed
		 	or a.gamedate < CURRENT_DATE - 3
		)
		and a.status_period <> -1
	;