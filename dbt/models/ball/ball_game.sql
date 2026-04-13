{{
    config(
        materialized = 'table',
        schema       = 'ball',
        alias        = 'game'
    )
}}

select
    fs.league_id,

    -- Map ESPN team IDs to internal team_ids; fall back to league_id * 10000 (TBD placeholder)
    coalesce(t.team_id, (fs.league_id * 10000))                     as home_team_id,
    coalesce(a.team_id, (fs.league_id * 10000))                     as away_team_id,

    fs.gamedate::date                                                as game_dt,
    fs.gamedate::time                                                as game_time,

    fs.home_score                                                    as h,
    fs.away_score                                                    as a,

    case
        when fs.home_score = 0 and fs.away_score = 0 then null
        when fs.home_score > fs.away_score           then coalesce(t.team_id, (fs.league_id * 10000))
        else                                              coalesce(a.team_id, (fs.league_id * 10000))
    end                                                              as won_team_id,

    fs.game_id                                                       as source_game_id,

    -- game_concat: e.g. "NBA_BOS_MIA_20241015"
    coalesce(t.team_concat, l.name || '_TBD')
        || '_' || coalesce(a.abbr, 'TBD')
        || '_' || to_char(fs.gamedate, 'YYYYMMDD')                  as game_concat,

    -- Period scores — null when 0 (game not yet played)
    nullif(fs.a1,  0) as a1,   nullif(fs.h1,  0) as h1,
    nullif(fs.a2,  0) as a2,   nullif(fs.h2,  0) as h2,
    nullif(fs.a3,  0) as a3,   nullif(fs.h3,  0) as h3,
    nullif(fs.a4,  0) as a4,   nullif(fs.h4,  0) as h4,
    nullif(fs.a5,  0) as a5,   nullif(fs.h5,  0) as h5,
    nullif(fs.a6,  0) as a6,   nullif(fs.h6,  0) as h6,
    nullif(fs.a7,  0) as a7,   nullif(fs.h7,  0) as h7,
    nullif(fs.a8,  0) as a8,   nullif(fs.h8,  0) as h8,
    nullif(fs.a9,  0) as a9,   nullif(fs.h9,  0) as h9,
    nullif(fs.a10, 0) as a10,  nullif(fs.h10, 0) as h10

from {{ source('src', 'espn_schedule') }} fs
join {{ ref('league') }} l
    on fs.league_id = l.league_id
left join {{ ref('ball_v_team') }} t
    on  fs.league_id    = t.league_id
    and fs.home_team_id = t.espn_team_id
left join {{ ref('ball_v_team') }} a
    on  fs.league_id    = a.league_id
    and fs.away_team_id = a.espn_team_id

-- Keep only games with at least one mapped team (drops Pro All-Stars, TBD match-ups)
where t.team_id is not null
   or a.team_id is not null
