{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'team_season'
    )
}}

with completed as (
    select
        *,
        row_number() over (
            partition by team_id, season_id
            order by game_dt desc, source_game_id desc
        )                                                               as recency_rank
    from {{ ref('ball_sched') }}
    where team is not null
)

select
    t.team_id,
    t.abbr,
    t.league_id,
    n.season_id,
    n.season_concat,
    n.yr,
    count(*)                                                            as games_played,
    sum(case when c.won then 1 else 0 end)                             as wins,
    sum(case when not c.won then 1 else 0 end)                         as losses,
    string_agg(
        case when c.won then 'W' else 'L' end, ''
        order by c.recency_rank
    ) filter (where c.recency_rank <= 5)                               as last_5_str,
    sum(case when c.recency_rank <= 5  and c.won then 1 else 0 end)   as last_5_wins,
    sum(case when c.recency_rank <= 10 and c.won then 1 else 0 end)   as last_10_wins,
    sum(case when c.recency_rank <= 15 and c.won then 1 else 0 end)   as last_15_wins

from completed c
join {{ ref('team') }}   t on t.team_id   = c.team_id
join {{ ref('season') }} n on n.season_id = c.season_id

group by t.team_id, t.abbr, t.league_id, n.season_id, n.season_concat, n.yr
