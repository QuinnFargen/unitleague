{{
    config(
        materialized = 'table',
        schema       = 'ball',
        alias        = 'sched'
    )
}}

/*
  Produces two rows per game: one from the home team's perspective, one from the away team's.
  Merges the original stored procedure's two-step INSERT + UPDATE into a single pass:
    1. unpivoted_games  — UNION ALL of home + away rows, both joined to ball.season
    2. with_week        — join to ball_week applied once after the UNION
                          (fixes the bug in the original procedure where away rows got NULL week_id)
    3. with_game_num    — ROW_NUMBER() replaces the post-insert UPDATE for game_num
    4. Final SELECT     — builds sched_concat using team abbrs and computed game_num
*/

with unpivoted_games as (

    -- Home team perspective
    select
        g.league_id,
        s.season_id,
        g.home_team_id                                              as team_id,
        g.away_team_id                                              as opp_team_id,
        g.game_dt,
        true                                                        as home,
        case
            when g.won_team_id = g.home_team_id then true
            else false
        end                                                         as won,
        g.h                                                         as team_score,
        g.a                                                         as opp_score,
        g.game_id,
        s.season_concat
    from {{ ref('ball_game') }} g
    join {{ ref('season') }} s
        on  g.league_id = s.league_id
        and g.game_dt between s.reg_start_dt - interval '2 months'
                          and s.champ_dt     + interval '2 months'
    where g.home_team_id % 10000 <> 0   -- exclude TBD placeholder teams

    union all

    -- Away team perspective
    select
        g.league_id,
        s.season_id,
        g.away_team_id                                              as team_id,
        g.home_team_id                                              as opp_team_id,
        g.game_dt,
        false                                                       as home,
        case
            when g.won_team_id = g.away_team_id then true
            else false
        end                                                         as won,
        g.a                                                         as team_score,
        g.h                                                         as opp_score,
        g.game_id,
        s.season_concat
    from {{ ref('ball_game') }} g
    join {{ ref('season') }} s
        on  g.league_id = s.league_id
        and g.game_dt between s.reg_start_dt - interval '2 months'
                          and s.champ_dt     + interval '2 months'
    where g.away_team_id % 10000 <> 0   -- exclude TBD placeholder teams

),

with_week as (

    -- Single week join applied after the UNION so both home and away rows get week_id
    select
        u.*,
        w.week_id
    from unpivoted_games u
    join {{ ref('ball_week') }} w
        on  u.league_id = w.league_id
        and u.game_dt between w.week_start_dt and w.week_end_dt

),

with_game_num as (

    -- Compute game_num per team per season; replaces the post-insert UPDATE step
    select
        ww.*,
        row_number() over (
            partition by ww.season_id, ww.team_id
            order by ww.game_dt
        )                                                           as game_num
    from with_week ww

)

select
    g.league_id,
    g.season_id,
    g.week_id,
    g.team_id,
    g.game_num::smallint                                            as game_num,
    g.opp_team_id,
    -- sched_concat: e.g. "NBA_2024_BOS_1_MIA"
    g.season_concat
        || '_' || t.abbr
        || '_' || g.game_num::varchar
        || '_' || o.abbr                                            as sched_concat,
    g.game_dt,
    g.home,
    g.won,
    g.team_score                                                    as team,
    g.opp_score                                                     as opp,
    g.game_id

from with_game_num g
join {{ ref('team') }} t on g.team_id     = t.team_id
join {{ ref('team') }} o on g.opp_team_id = o.team_id
