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
-- from the syndicate's leagues. The attribute is matched against en.name
-- (e.g. "Region", "Conference", "Mascot", "Color"). league_id and the
-- enhancement columns are carried through so the final select doesn't need
-- a second join back to the enhancement table.
team_picks as (
    select distinct on (r.runner_id, en.enhancement_id)
        r.runner_id,
        r.bettor_id,
        r.syndicate_id,
        sl.league_id,
        en.enhancement_id,
        en.enhancement_type,
        en.edge_type,
        en.name,
        en.description,
        en.bet_type,
        en.rarity,
        en.cost,
        en.effect,
        en.value,
        en.config,
        en.symbol,
        tav.value as available_attr_value
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
),

-- For non-team-type enhancements there's no attribute value to pick, but we
-- still need a league_id — randomly pick one from the syndicate's leagues.
non_team_picks as (
    select distinct on (r.runner_id, en.enhancement_id)
        r.runner_id,
        r.bettor_id,
        r.syndicate_id,
        sl.league_id,
        en.enhancement_id,
        en.enhancement_type,
        en.edge_type,
        en.name,
        en.description,
        en.bet_type,
        en.rarity,
        en.cost,
        en.effect,
        en.value,
        en.config,
        en.symbol
    from {{ source('odd', 'runner') }} r
    join syndicate_leagues sl on sl.syndicate_id = r.syndicate_id
    cross join {{ source('odd', 'enhancement') }} en
    where r.active            = true
      and en.active            = true
      and en.enhancement_type <> 'team'
    order by r.runner_id, en.enhancement_id, random()
)

select
    runner_id,
    bettor_id,
    syndicate_id,
    league_id,
    enhancement_id,
    enhancement_type,
    edge_type,
    name,
    description,
    bet_type,
    rarity,
    cost,
    effect,
    value,
    config,
    symbol,
    available_attr_value,
    md5(
        runner_id::text
        || enhancement_id::text
        || coalesce(league_id::text, '')
        || coalesce(available_attr_value, '')
    ) as option_hash
from team_picks

union all

select
    runner_id,
    bettor_id,
    syndicate_id,
    league_id,
    enhancement_id,
    enhancement_type,
    edge_type,
    name,
    description,
    bet_type,
    rarity,
    cost,
    effect,
    value,
    config,
    symbol,
    null::text as available_attr_value,
    md5(
        runner_id::text
        || enhancement_id::text
        || coalesce(league_id::text, '')
    ) as option_hash
from non_team_picks
