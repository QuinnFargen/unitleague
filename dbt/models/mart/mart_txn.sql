{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'txn'
    )
}}

with enhancement_agg as (
    select
        txn_id,
        exp(sum(ln(price_mult))) as combined_price_mult,
        exp(sum(ln(unit_mult)))  as combined_unit_mult
    from {{ ref('mart_txn_enhanced') }}
    group by txn_id
)

select
    t.txn_id,
    t.insert_dt,
    t.bettor_id,
    t.syndicate_id,
    t.bet_hash,
    t.parlay_id,
    t.unit,
    t.price,
    t.unit  * coalesce(e.combined_unit_mult,  1)  as unit_enhanced,
    t.price * coalesce(e.combined_price_mult, 1)  as price_enhanced,
    false   as won,
    t.canceled,
    t.cancel_ts

from {{ source('odd', 'txn') }} t
left join enhancement_agg e on e.txn_id = t.txn_id
