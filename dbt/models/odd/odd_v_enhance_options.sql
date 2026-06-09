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
    o.enhancement_id,
    o.enhancement_type,
    o.name,
    o.description,
    o.bet_type,
    o.config,
    o.selected_value,
    o.option_hash

from {{ ref('odd_enhance_options') }} o
left join {{ source('odd', 'enhanced') }} ed
    on  ed.option_hash = o.option_hash
    and ed.active      = true

where ed.option_hash is null
