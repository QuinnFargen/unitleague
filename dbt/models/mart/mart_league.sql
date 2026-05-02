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
)

select
    l.league_id,
    l.abbr,
    l.name,
    l.sport,
    l.yr_orig,
    l.yr_data,
    l.weather,
    coalesce(s.status, 'offseason')                                 as status

from {{ ref('league') }} l
left join season_status s on s.league_id = l.league_id
