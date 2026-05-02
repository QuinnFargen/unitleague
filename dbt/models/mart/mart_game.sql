{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'game'
    )
}}

select
    g.source_game_id                                                as game_id,
    h.abbr                                                          as home,
    a.abbr                                                          as away,
    g.game_dt,
    g.game_time,
    g.game_concat,
    g.h,
    g.a,
    case
        when g.home_team_id = g.won_team_id then h.abbr
        when g.away_team_id = g.won_team_id then a.abbr
        else null
    end                                                             as winner,
    case
        when g.won_team_id is not null then abs(g.h - g.a)
    end                                                             as margin,
    case
        when g.h is not null and g.a is not null then g.h + g.a
    end                                                             as total,
    g.league_id,
    g.home_team_id,
    g.away_team_id,
    g.won_team_id

from {{ ref('ball_game') }} g
join {{ ref('team') }} h on g.home_team_id = h.team_id
join {{ ref('team') }} a on g.away_team_id = a.team_id
