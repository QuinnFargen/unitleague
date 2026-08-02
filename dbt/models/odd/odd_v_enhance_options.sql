{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'v_enhance_options'
    )
}}

select
    o.runner_id,
    o.bettor_id,
    o.syndicate_id,
    o.league_id,
    o.enhancement_id,
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
    o.available_attr_value,
    o.option_hash

from {{ ref('odd_enhance_options') }} o
join {{ source('odd', 'enhancement') }} en
    on en.enhancement_id = o.enhancement_id
left join {{ source('odd', 'enhanced') }} ed
    on  ed.option_hash = o.option_hash
    and ed.active      = true

where ed.option_hash is null
