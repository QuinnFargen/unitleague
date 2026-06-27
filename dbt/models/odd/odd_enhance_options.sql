{{
    config(
        materialized = 'table',
        schema       = 'odd',
        alias        = 'enhance_options'
    )
}}

-- syndicate.config stores league_ids as a JSON array: {"league_ids": [1, 2]}
with syndicate_leagues as (
    select
        syndicate_id,
        (json_array_elements_text(config -> 'league_ids'))::int as league_id
    from {{ source('odd', 'syndicate') }}
    where config -> 'league_ids' is not null
),

-- Distinct attribute values available per league from the team table
team_attr_values as (
    select distinct league_id, 'Region'   as attribute, region   as value from {{ ref('mart_team') }} where region   is not null
    union all
    select distinct league_id, 'Conference'     as attribute, conf     as value from {{ ref('mart_team') }} where conf     is not null
    union all
    select distinct league_id, 'Mascot' as attribute, category as value from {{ ref('mart_team') }} where category is not null
    union all
    select distinct league_id, 'Color'    as attribute, color    as value from {{ ref('mart_team') }} where color    is not null
),

-- For team-type enhancements, randomly pick one available attribute value
-- from the syndicate's leagues. enhancement.config format: {"attribute": "region"}
team_picks as (
    select distinct on (r.runner_id, en.enhancement_id)
        r.runner_id,
        en.enhancement_id,
        tav.value as selected_value
    from {{ source('odd', 'runner') }} r
    join syndicate_leagues sl  on sl.syndicate_id = r.syndicate_id
    cross join {{ source('odd', 'enhancement') }} en
    join team_attr_values tav
        on  tav.league_id = sl.league_id
        and tav.attribute = en.name
    where r.active              = true
      and en.active             = true
      and en.enhancement_type   = 'team'
    order by r.runner_id, en.enhancement_id, random()
)

select
    r.runner_id,
    r.bettor_id,
    r.syndicate_id,
    en.enhancement_id,
    en.enhancement_type,
    en.name,
    en.description,
    en.bet_type,
    en.config,
    tp.selected_value,
    md5(r.runner_id::text || en.enhancement_id::text) as option_hash

from {{ source('odd', 'runner') }} r
cross join {{ source('odd', 'enhancement') }} en
left join team_picks tp
    on  tp.runner_id      = r.runner_id
    and tp.enhancement_id = en.enhancement_id

where r.active  = true
  and en.active = true
  and (
      en.enhancement_type <> 'team'
      or tp.runner_id is not null
  )
