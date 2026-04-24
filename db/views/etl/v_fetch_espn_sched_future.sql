CREATE OR REPLACE VIEW etl.v_fetch_espn_sched_future
AS
SELECT
     s.league_id
    ,s.season_concat
    ,s.yr_var
    ,s.reg_start_dt
    ,s.reg_end_dt
    ,s.post_start_dt
    ,s.champ_dt
    ,COALESCE(bool_or(d.gamedate::date BETWEEN s.reg_start_dt AND s.reg_end_dt), FALSE)        AS has_reg
    ,COALESCE(bool_or(
        s.post_start_dt IS NOT NULL
        AND d.gamedate::date BETWEEN s.post_start_dt AND s.champ_dt
    ), FALSE)                                                                                   AS has_post
FROM ball.season s
LEFT JOIN src.espn_schedule d
    ON  s.league_id = d.league_id
    AND d.gamedate::date BETWEEN s.reg_start_dt AND s.champ_dt
WHERE s.champ_dt > CURRENT_DATE
GROUP BY
     s.league_id, s.season_concat, s.yr_var
    ,s.reg_start_dt, s.reg_end_dt, s.post_start_dt, s.champ_dt
;