{{
    config(
        materialized = 'table',
        schema       = 'ball',
        alias        = 'week'
    )
}}

select
    -- week_id: season_id * 1000 + offset + week_num
    -- offsets: 100 = pre-season, 200 = regular, 300 = post-season
    (s.season_id * 1000)
        + w.week_num
        + case
            when w.is_pre  then 100
            when w.is_post then 300
            else                200
          end                                                        as week_id,

    s.season_id,
    w.league_id,
    w.week_start_dt,
    w.week_end_dt,
    w.week_num,

    -- e.g. "NFL_2024_PR1", "NBA_2024_1", "MLB_2024_PO1"
    s.season_concat
        || case
               when w.is_pre  then '_PR'
               when w.is_post then '_PO'
               else                '_'
           end
        || w.week_num::varchar                                       as week_concat,

    w.week_name,
    w.is_pre,
    w.is_post

from {{ source('src', 'espn_week') }} w
join {{ ref('season') }} s
    on  w.league_id = s.league_id
    and w.yr        = s.yr
