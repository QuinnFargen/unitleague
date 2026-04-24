{{
    config(
        materialized  = 'incremental',
        unique_key    = ['league_id', 'season_id', 'team_id', 'game_id'],
        schema        = 'ball',
        alias         = 'sched'
    )
}}

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
            when g.won_team_id is null then null
            when g.won_team_id = g.home_team_id then true
            else false
        end                                                         as won,
        g.h                                                         as team_score,
        g.a                                                         as opp_score,
        g.source_game_id,
        s.season_concat
    from {{ ref('ball_game') }} g
    join {{ ref('season') }} s
        on  g.league_id = s.league_id
        and g.game_dt between s.reg_start_dt - interval '2 months'
                          and s.champ_dt     + interval '2 months'
    where g.home_team_id % 10000 <> 0

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
            when g.won_team_id is null then null
            when g.won_team_id = g.away_team_id then true
            else false
        end                                                         as won,
        g.a                                                         as team_score,
        g.h                                                         as opp_score,
        g.source_game_id,
        s.season_concat
    from {{ ref('ball_game') }} g
    join {{ ref('season') }} s
        on  g.league_id = s.league_id
        and g.game_dt between s.reg_start_dt - interval '2 months'
                          and s.champ_dt     + interval '2 months'
    where g.away_team_id % 10000 <> 0

),

with_week as (

    select
        u.*,
        w.week_id
    from unpivoted_games u
    join {{ ref('ball_week') }} w
        on  u.league_id = w.league_id
        and u.game_dt between w.week_start_dt and w.week_end_dt

),

with_game_num as (

    -- row_number over ALL games so game_num is correct across the full season.
    -- source_game_id as tiebreaker makes double-header ordering deterministic.
    select
        ww.*,
        row_number() over (
            partition by ww.season_id, ww.team_id
            order by ww.game_dt, ww.source_game_id
        )                                                           as game_num
    from with_week ww

)

select
    {{ dbt_utils.generate_surrogate_key(['g.league_id', 'g.season_id', 'g.team_id', 'g.source_game_id']) }} as sched_id,
    g.league_id,
    g.season_id,
    g.week_id,
    g.team_id,
    g.game_num::smallint                                            as game_num,
    g.opp_team_id,
    g.season_concat
        || '_' || t.abbr
        || '_' || g.game_num::varchar
        || '_' || o.abbr                                            as sched_concat,
    g.game_dt,
    g.home,
    g.won,
    g.team_score                                                    as team,
    g.opp_score                                                     as opp,
    g.source_game_id                                                as game_id

from with_game_num g
join {{ ref('team') }} t on g.team_id     = t.team_id
join {{ ref('team') }} o on g.opp_team_id = o.team_id

{% if is_incremental() %}
where g.game_dt >= (select max(game_dt) - interval '7 days' from {{ this }})
{% endif %}
