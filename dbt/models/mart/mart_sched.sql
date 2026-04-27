{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'sched'
    )
}}

select
    s.sched_id,
    t.abbr                                                          as team_abbr,
    o.abbr                                                          as opp_abbr,
    s.game_dt,
    s.game_num,
    n.yr,
    s.sched_concat,
    s.team,
    s.opp,
    s.home,
    s.won,
    s.source_game_id,
    s.league_id,
    s.team_id,
    s.opp_team_id

from {{ ref('ball_sched') }} s
join {{ ref('team') }} t  on s.team_id     = t.team_id
join {{ ref('team') }} o  on s.opp_team_id = o.team_id
join {{ ref('season') }} n on s.season_id  = n.season_id
