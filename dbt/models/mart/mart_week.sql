{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'week'
    )
}}

select
    week_id,
    season_id,
    league_id,
    week_start_dt,
    week_end_dt,
    week_num,
    week_concat,
    week_name,
    is_pre,
    is_post

from {{ ref('ball_week') }}
