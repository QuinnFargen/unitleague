CREATE OR REPLACE VIEW etl.v_fetch_espn_sched_update
AS SELECT DISTINCT date_part('YEAR'::text, gamedate::date) AS year,
    season_type AS seasontype,
    gameweek AS weekno,
    league_id
   FROM src.espn_schedule fs2
  WHERE gamedate < CURRENT_DATE AND status_period = 0;