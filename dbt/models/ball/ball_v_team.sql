{{
    config(
        materialized = 'view',
        schema       = 'ball',
        alias        = 'v_team'
    )
}}

with region_avg as (
    select
        loc,
        avg(lat)::numeric(6, 3) as region_lat,
        avg(lon)::numeric(6, 3) as region_lon
    from {{ ref('team') }}
    group by loc
)

select
    t.team_id,
    t.league_id,
    t.abbr,
    t.team_concat,
    t.name,
    t.loc,
    t.conf,
    t.div,
    t.lat,
    t.lon,
    t.weather,
    r.region_lat,
    r.region_lon,
    e.meta_value::int  as espn_team_id,
    c.meta_value::int  as cfbd_team_id,
    p.meta_value       as odds_team_name,
    k.meta_value       as kalshi_team_name
from {{ ref('team') }} t
join region_avg r
    on t.loc = r.loc
left join {{ ref('meta') }} e
    on t.team_id = e.meta_keyid
    and e.meta_type   = 'source_team_id'
    and e.meta_source = 'espn'
left join {{ ref('meta') }} c
    on t.team_id = c.meta_keyid
    and c.meta_type   = 'source_team_id'
    and c.meta_source = 'cfbd'
left join {{ ref('meta') }} p
    on t.team_id = p.meta_keyid
    and p.meta_type   = 'team_name'
    and p.meta_source = 'the_odds_api'
left join {{ ref('meta') }} k
    on t.team_id = k.meta_keyid
    and k.meta_type   = 'team_name'
    and k.meta_source = 'kalshi'
