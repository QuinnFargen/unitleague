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
        (jsonb_array_elements_text(config -> 'league_ids'))::int as league_id
    from {{ source('odd', 'syndicate') }}
    where config ? 'league_ids'
),

-- For team-type enhancements, randomly pick one distinct attribute value
-- (e.g. "red", "south") present in the syndicate's leagues that falls within valid_values.
-- enhancement.config format: {"attribute": "region", "valid_values": ["south", "north"]}
-- Supported attributes: region, conf, category, color
team_picks as (
    select distinct on (r.runner_id, en.enhancement_id)
        r.runner_id,
        en.enhancement_id,
        case en.config ->> 'attribute'
            when 'region'   then t.region
            when 'conf'     then t.conf
            when 'category' then t.category
            when 'color'    then t.color
        end as selected_value
    from {{ source('odd', 'runner') }} r
    join syndicate_leagues sl on sl.syndicate_id = r.syndicate_id
    cross join {{ source('odd', 'enhancement') }} en
    join {{ ref('team') }} t on t.league_id = sl.league_id
        and (
            (en.config ->> 'attribute' = 'region'   and t.region   = any(array(select jsonb_array_elements_text(en.config -> 'valid_values'))))
         or (en.config ->> 'attribute' = 'conf'     and t.conf     = any(array(select jsonb_array_elements_text(en.config -> 'valid_values'))))
         or (en.config ->> 'attribute' = 'category' and t.category = any(array(select jsonb_array_elements_text(en.config -> 'valid_values'))))
         or (en.config ->> 'attribute' = 'color'    and t.color    = any(array(select jsonb_array_elements_text(en.config -> 'valid_values'))))
        )
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
