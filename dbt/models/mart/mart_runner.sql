{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'runner'
    )
}}

select
    r.runner_id,
    r.bettor_id,
    r.syndicate_id,
    r.role,
    r.active,
    r.profile_name,
    r.symbol,
    r.color,

    coalesce(sum(
        case
            when t.txn_type = 'unit' then t.unit
            when t.won               then t.unit  * t.price
            else                         -t.unit
        end
    ), 0) as balance,

    coalesce(sum(
        case
            when t.txn_type = 'unit' then t.unit
            when t.won               then t.unit_enhanced  * t.price_enhanced
            else                         -t.unit_enhanced
        end
    ), 0) as balance_enhanced

from {{ source('odd', 'runner') }} r
left join {{ source('odd', 'syndicate') }} s on s.syndicate_id = r.syndicate_id
left join {{ ref('mart_txn') }} t
    on  t.bettor_id    = r.bettor_id
    and t.syndicate_id = case when s.is_started then r.syndicate_id else 0 end

where r.active = true

group by
    r.runner_id,
    r.bettor_id,
    r.syndicate_id,
    r.role,
    r.active,
    r.profile_name,
    r.symbol,
    r.color
