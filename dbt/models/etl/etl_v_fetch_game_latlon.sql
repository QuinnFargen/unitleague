{{
    config(
        materialized = 'view',
        schema       = 'etl',
        alias        = 'v_fetch_game_latlon'
    )
}}

select distinct
    t.region_lat,
    t.region_lon
from {{ ref('ball_game') }} g
join {{ ref('ball_v_team') }} t on g.home_team_id = t.team_id
where g.game_dt between current_date
    and current_date + (case when t.weather = 1 then 1 else 7 end)
