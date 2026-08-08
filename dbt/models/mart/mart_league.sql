{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'league'
    )
}}

with season_status as (
    select distinct on (league_id)
        league_id,
        case
            when current_date between pre_dt and reg_start_dt - interval '1 day'  then 'preseason'
            when current_date between reg_start_dt and reg_end_dt                 then 'regular season'
            when current_date between post_start_dt and champ_dt                  then 'playoffs'
            else                                                                       'offseason'
        end as status
    from {{ ref('season') }}
    where current_date >= coalesce(pre_dt, reg_start_dt)
    order by league_id, champ_dt desc
),

season_max_yr as (
    select
        league_id,
        max(yr) as yr_data
    from {{ ref('mart_season') }}
    group by league_id
),

season_start as (
    select
        league_id,
        max(reg_start_dt) as season_start_dt
    from {{ ref('season') }}
    group by league_id
)

select
    l.league_id,
    l.abbr,
    l.name,
    l.sport,
    l.yr_orig,
    my.yr_data,
    l.weather,
    coalesce(s.status, 'offseason')                                 as status,
    ss.season_start_dt

from {{ ref('league') }} l
left join season_status s on s.league_id = l.league_id
left join season_max_yr my on my.league_id = l.league_id
left join season_start ss on ss.league_id = l.league_id
