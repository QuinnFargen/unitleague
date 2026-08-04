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
        home_team_id as team_id,
        away_team_id as opp_team_id,
        spr_home_won as covered,
        over_won,
        under_won
    from {{ ref('mart_game_oddbest') }}

    union all

    select
        game_id, league_id, game_dt,
        away_team_id as team_id,
        home_team_id as opp_team_id,
        spr_away_won as covered,
        over_won,
        under_won
    from {{ ref('mart_game_oddbest') }}

),

ats_ranked as (
    select
        *,
        row_number() over (
            partition by team_id
            order by game_dt desc, game_id desc
        ) as ats_recency_rank
    from unpivoted
    where covered is not null
),

ou_ranked as (
    select
        *,
        row_number() over (
            partition by team_id
            order by game_dt desc, game_id desc
        ) as ou_recency_rank
    from unpivoted
    where over_won is not null
       or under_won is not null
),

-- aggregated independently (not joined pre-aggregation) so the two trailing
-- windows don't fan out against each other when a team's settled-spread and
-- settled-total game counts differ
ats_agg as (
    select
        team_id,
        sum(case when covered then 1 else 0 end)     as ats_wins,
        sum(case when not covered then 1 else 0 end) as ats_losses,
        string_agg(
            case when covered then 'W' else 'L' end, ''
            order by ats_recency_rank
        )                                             as ats_last10_str
    from ats_ranked
    where ats_recency_rank <= 10
    group by team_id
),

ou_agg as (
    select
        team_id,
        sum(case when over_won  then 1 else 0 end) as over_count,
        sum(case when under_won then 1 else 0 end) as under_count,
        string_agg(
            case when over_won then 'O' else 'U' end, ''
            order by ou_recency_rank
        )                                           as ou_last10_str
    from ou_ranked
    where ou_recency_rank <= 10
    group by team_id
)

select
    t.team_id,
    t.league_id,
    t.abbr,
    t.conf,
    t.color,
    t.region,
    t.category,
    coalesce(ats.ats_wins, 0)    as ats_wins,
    coalesce(ats.ats_losses, 0)  as ats_losses,
    ats.ats_last10_str,
    coalesce(ou.over_count, 0)   as over_count,
    coalesce(ou.under_count, 0)  as under_count,
    ou.ou_last10_str

from {{ ref('team') }} t
left join ats_agg ats on ats.team_id = t.team_id
left join ou_agg  ou  on ou.team_id  = t.team_id
