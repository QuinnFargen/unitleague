{{
    config(
        materialized  = 'incremental',
        unique_key    = ['league_id', 'source_game_id'],
        schema        = 'ball',
        alias         = 'game'
    )
}}

with mapped as (

    select
        fs.league_id,
        coalesce(t.team_id, (fs.league_id * 10000))                as home_team_id,
        coalesce(a.team_id, (fs.league_id * 10000))                as away_team_id,
        fs.gamedate::date                                           as game_dt,
        fs.gamedate::time                                           as game_time,
        fs.home_score                                               as h,
        fs.away_score                                               as a,
        case
            when fs.home_score = 0 and fs.away_score = 0 then null
            when fs.home_score > fs.away_score           then coalesce(t.team_id, (fs.league_id * 10000))
            else                                              coalesce(a.team_id, (fs.league_id * 10000))
        end                                                         as won_team_id,
        fs.game_id                                                  as source_game_id,
        coalesce(t.team_concat, l.abbr || '_TBD')                  as home_concat,
        coalesce(a.abbr, 'TBD')                                    as away_abbr,
        fs.a1,  fs.h1,
        fs.a2,  fs.h2,
        fs.a3,  fs.h3,
        fs.a4,  fs.h4,
        fs.a5,  fs.h5,
        fs.a6,  fs.h6,
        fs.a7,  fs.h7,
        fs.a8,  fs.h8,
        fs.a9,  fs.h9,
        fs.a10, fs.h10

    from {{ source('src', 'espn_schedule') }} fs
    join {{ ref('league') }} l
        on fs.league_id = l.league_id
    left join {{ ref('ball_v_team') }} t
        on  fs.league_id    = t.league_id
        and fs.home_team_id = t.espn_team_id
    left join {{ ref('ball_v_team') }} a
        on  fs.league_id    = a.league_id
        and fs.away_team_id = a.espn_team_id
    where t.team_id is not null
       or a.team_id is not null

    {% if is_incremental() %}
        and fs.gamedate >= (select max(game_dt) - interval '7 days' from {{ this }})
    {% endif %}

),

with_dh as (

    -- Detect double headers: two teams playing each other twice on the same day
    select
        m.*,
        row_number() over (
            partition by m.league_id, m.home_team_id, m.away_team_id, m.game_dt
            order by m.source_game_id
        )                                                           as dh_num,
        count(*) over (
            partition by m.league_id, m.home_team_id, m.away_team_id, m.game_dt
        )                                                           as dh_count
    from mapped m

)

select
    league_id,
    home_team_id,
    away_team_id,
    game_dt,
    game_time,
    h,
    a,
    won_team_id,
    source_game_id          as game_id,
    home_concat
        || '_' || away_abbr
        || '_' || to_char(game_dt, 'YYYYMMDD')
        || case when dh_count > 1 then '_' || dh_num::varchar else '' end  as game_concat,
    a1,  h1,
    a2,  h2,
    a3,  h3,
    a4,  h4,
    a5,  h5,
    a6,  h6,
    a7,  h7,
    a8,  h8,
    a9,  h9,
    a10, h10

from with_dh
