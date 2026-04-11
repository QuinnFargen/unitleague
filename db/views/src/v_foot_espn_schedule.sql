CREATE OR REPLACE VIEW src.v_foot_espn_schedule
AS
	select distinct
		 a.league_id
		,a.game_id
		,a.gamedate
		,a."name"
		,a.shortname
		,a.season_year
		,a.season_type
		,a.season_slug
		,case when a.gamedate < CURRENT_DATE - 3 and a.status_period = 0 then -1 else a.status_period end as status_period
		,a.home_score
		,a.home_team
		,a.home_abbr
		,a.home_short
		,a.home_team_id
		,a.h1
		,a.h2
		,a.h3
		,a.h4
		,a.h5
		,a.h6
		,a.h7
		,a.h8
		,a.h9
		,a.h10
		,a.away_score
		,a.away_team
		,a.away_abbr
		,a.away_short
		,a.away_team_id
		,a.a1
		,a.a2
		,a.a3
		,a.a4
		,a.a5
		,a.a6
		,a.a7
		,a.a8
		,a.a9
		,a.a10
		-- SELECT *
	from etl.stg_foot_espn_schedule a
	left join src.espn_schedule s on a.game_id = s.game_id and s.league_id = a.league_id
	where (
			s.game_id is null						-- not yet in src or
		 	or a.status_period <> s.status_period 	-- game has completed
		 	or a.gamedate < CURRENT_DATE - 3
		)
		and a.status_period <> -1
	;
