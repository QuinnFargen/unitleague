{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'team_odds_recent'
    )
}}

with unpivoted as (

    select
        game_id, league_id, game_dt,
        home_team_id   as team_id,
        away_team_id   as opp_team_id,
        spr_home_won   as covered,
        spr_home_push  as ats_push,
        over_won,
        under_won,
        total_push
    from {{ ref('mart_game_oddbest') }}

    union all

    select
        game_id, league_id, game_dt,
        away_team_id   as team_id,
        home_team_id   as opp_team_id,
        spr_away_won   as covered,
        spr_away_push  as ats_push,
        over_won,
        under_won,
        total_push
    from {{ ref('mart_game_oddbest') }}

),

-- scope each game to the season it belongs to for this team, using the same
-- team/game -> season assignment ball_sched (and mart_team_season) already use
scoped as (
    select
        u.*,
        s.season_id
    from unpivoted u
    join {{ ref('ball_sched') }} s
        on  s.game_id = u.game_id
        and s.team_id = u.team_id
),

ats_ranked as (
    select
        *,
        row_number() over (
            partition by team_id, season_id
            order by game_dt desc, game_id desc
        ) as ats_recency_rank
    from scoped
    where covered is not null or ats_push
),

ou_ranked as (
    select
        *,
        row_number() over (
            partition by team_id, season_id
            order by game_dt desc, game_id desc
        ) as ou_recency_rank
    from scoped
    where over_won is not null or under_won is not null or total_push
),

-- aggregated independently (not joined pre-aggregation) so the two trailing
-- windows don't fan out against each other when a team's settled-spread and
-- settled-total game counts differ
ats_agg as (
    select
        team_id,
        season_id,
        sum(case when covered         then 1 else 0 end) as ats_wins,
        sum(case when covered = false then 1 else 0 end) as ats_losses,
        sum(case when ats_push        then 1 else 0 end) as ats_pushes,
        string_agg(
            case when ats_push then '=' when covered then 'W' else 'L' end, ''
            order by ats_recency_rank desc
        ) filter (where ats_recency_rank <= 10)           as ats_last10_str
    from ats_ranked
    group by team_id, season_id
),

ou_agg as (
    select
        team_id,
        season_id,
        sum(case when over_won   then 1 else 0 end) as over_count,
        sum(case when under_won  then 1 else 0 end) as under_count,
        sum(case when total_push then 1 else 0 end) as ou_pushes,
        string_agg(
            case when total_push then '=' when over_won then 'O' else 'U' end, ''
            order by ou_recency_rank desc
        ) filter (where ou_recency_rank <= 10)      as ou_last10_str
    from ou_ranked
    group by team_id, season_id
),

team_seasons as (
    select distinct team_id, season_id from scoped
)

select
    t.team_id,
    t.league_id,
    t.abbr,
    t.conf,
    t.color,
    t.region,
    t.category,
    ts.season_id,
    n.season_concat,
    n.yr,
    coalesce(ats.ats_wins, 0)    as ats_wins,
    coalesce(ats.ats_losses, 0) as ats_losses,
    coalesce(ats.ats_pushes, 0) as ats_pushes,
    ats.ats_last10_str,
    ats.ats_wins::numeric / nullif(ats.ats_wins + ats.ats_losses, 0) as ats_cover_pct,
    coalesce(ou.over_count, 0)  as over_count,
    coalesce(ou.under_count, 0) as under_count,
    coalesce(ou.ou_pushes, 0)   as ou_pushes,
    ou.ou_last10_str,
    ou.over_count::numeric / nullif(ou.over_count + ou.under_count, 0) as over_pct

from team_seasons ts
join {{ ref('team') }}   t on t.team_id   = ts.team_id
join {{ ref('season') }} n on n.season_id = ts.season_id
left join ats_agg ats on ats.team_id = ts.team_id and ats.season_id = ts.season_id
left join ou_agg  ou  on ou.team_id  = ts.team_id and ou.season_id  = ts.season_id
