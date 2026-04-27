{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'league'
    )
}}

with active_seasons as (
    select distinct league_id
    from {{ ref('season') }}
    where current_date between reg_start_dt and champ_dt
)

select
    l.league_id,
    l.abbr,
    l.name,
    l.sport,
    l.yr_orig,
    l.yr_data,
    l.weather,
    (a.league_id is not null)                                           as active_season

from {{ ref('league') }} l
left join active_seasons a on a.league_id = l.league_id
