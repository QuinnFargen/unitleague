{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'season'
    )
}}

with champ_games as (
    select
        s.season_id,
        g.won_team_id                                                   as champ_team_id,
        t.abbr                                                          as champ_abbr
    from {{ ref('season') }} s
    join {{ ref('ball_game') }} g
        on  g.league_id  = s.league_id
        and g.game_dt    = s.champ_dt
        and g.won_team_id is not null
    join {{ ref('team') }} t on t.team_id = g.won_team_id
)

select
    s.season_id,
    s.league_id,
    s.season_concat,
    s.yr,
    s.yr_var,
    s.pre_dt,
    s.reg_start_dt,
    s.reg_end_dt,
    s.post_start_dt,
    s.champ_series_start_dt,
    s.champ_dt,
    (current_date between s.reg_start_dt and s.champ_dt)               as active,
    cg.champ_team_id,
    cg.champ_abbr

from {{ ref('season') }} s
left join champ_games cg on cg.season_id = s.season_id
