{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'enhance_options'
    )
}}

with current_week as (
    select distinct week_id, league_id
    from {{ ref('ball_week') }}
    where current_date between week_start_dt and week_end_dt
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
    cw.week_id,
    cw.league_id,
    md5(r.runner_id::text || en.enhancement_id::text) as option_hash

from {{ source('odd', 'runner') }} r
cross join {{ source('odd', 'enhancement') }} en
join current_week cw on true

where r.active  = true
  and en.active = true
