{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'league_game_dates'
    )
}}

with distinct_dates as (
    select distinct league_id, game_dt
    from {{ ref('mart_game') }}
)

select
    d.league_id,
    d.game_dt,
    w.week_num,
    s.yr

from distinct_dates d
left join {{ ref('ball_week') }} w
    on  w.league_id = d.league_id
    and d.game_dt between w.week_start_dt and w.week_end_dt
left join {{ ref('season') }} s
    on  s.league_id = d.league_id
    and d.game_dt between coalesce(s.pre_dt, s.reg_start_dt) and s.champ_dt

order by d.league_id, d.game_dt
