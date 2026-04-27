{{
    config(
        materialized = 'view',
        schema       = 'etl',
        alias        = 'v_fetch_espn_sched_future'
    )
}}

select
     s.league_id
    ,s.season_concat
    ,s.yr_var
    ,s.reg_start_dt
    ,s.reg_end_dt
    ,s.post_start_dt
    ,s.champ_dt
    ,coalesce(bool_or(d.gamedate::date between s.reg_start_dt and s.reg_end_dt), false)        as has_reg
    ,coalesce(bool_or(
        s.post_start_dt is not null
        and d.gamedate::date between s.post_start_dt and s.champ_dt
    ), false)                                                                                   as has_post
from {{ ref('season') }} s
left join {{ source('src', 'espn_schedule') }} d
    on  s.league_id = d.league_id
    and d.gamedate::date between s.reg_start_dt and s.champ_dt
where s.champ_dt > current_date
group by
     s.league_id, s.season_concat, s.yr_var
    ,s.reg_start_dt, s.reg_end_dt, s.post_start_dt, s.champ_dt
