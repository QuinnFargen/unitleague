
	
	-- src.v_foot_espn_schedule source

CREATE OR REPLACE VIEW src.v_foot_espn_schedule
AS SELECT DISTINCT
        CASE
            WHEN t.team_id IS NOT NULL THEN 2
            ELSE 5
        END AS league_id,
    a.game_id,
    a.gamedate,
    a.name,
    a.shortname,
    a.week_number,
    a.season_year,
    a.season_type,
    a.season_slug,
        CASE
            WHEN a.gamedate < CURRENT_DATE AND a.status_period = 0 THEN '-1'::integer
            ELSE a.status_period
        END AS status_period,
    a.home_score,
    a.home_team,
    a.home_abbr,
    a.home_short,
    a.home_team_id,
    a.seasontype,
    a.seasontypelabel,
    a.gameweek,
    a.weeklabel,
    a.q1_home as h1,
    a.q2_home as h2,
    a.q3_home as h3,
    a.q4_home as h4,
    a.q5_home as h5,
    a.away_score,
    a.away_team,
    a.away_abbr,
    a.away_short,
    a.away_team_id,
    a.q1_away as a1,
    a.q2_away as a2,
    a.q3_away as a3,
    a.q4_away as a4,
    a.q5_away as a5
   FROM etl.stg_foot_espn_schedule a
     JOIN ball.game g ON a.game_id = g.source_game_id
     LEFT JOIN src.espn_schedule s ON a.game_id = s.game_id
     LEFT JOIN ball.v_team t ON a.home_team_id = t.espn_team_id AND t.league_id = 2
  WHERE (s.game_id IS NULL OR a.status_period <> s.status_period OR a.gamedate < (CURRENT_DATE - 3)) AND a.status_period <> '-1'::integer;
	