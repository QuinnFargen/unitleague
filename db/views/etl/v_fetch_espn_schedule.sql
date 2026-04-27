CREATE OR REPLACE VIEW etl.v_fetch_espn_sched_update
AS 
    SELECT DISTINCT 
        TO_CHAR(gamedate::date, 'YYYYMMDD') AS game_dt
        ,league_id
    FROM src.espn_schedule fs2
    WHERE 
        gamedate < CURRENT_DATE 
        AND status_period = 0;