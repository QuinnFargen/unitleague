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
),

parlay_cte as (
    select
        vp.parlay_id,
        max(g.game_dt) as game_dt
    from {{ ref('v_parlay') }} vp
    left join {{ ref('mart_game') }} g on g.game_id = vp.game_id
    group by vp.parlay_id
)

select
    t.txn_id,
    t.insert_dt,
    t.bettor_id,
    t.syndicate_id,
    case when t.parlay_id is not null then 'parlay' else 'straight' end  as txn_type,
    t.bet_hash,
    t.parlay_id,
    t.price,
    t.unit,
    t.unit  * coalesce(e.combined_unit_mult,  1)                         as unit_enhanced,
    t.price * coalesce(e.combined_price_mult, 1)                         as price_enhanced,
    b.game_id,
    coalesce(g.game_dt, pc.game_dt)                                      as game_dt,
    g.game_time,
    g.home,
    g.away,
    b.bookmaker,
    b.bet_type,
    b.bet_concat,
    b.points,
    false                                                                 as won

from {{ source('odd', 'txn') }} t
left join {{ ref('odd_bet') }}   b  on  b.bet_hash   = t.bet_hash and b.active
left join {{ ref('mart_game') }} g  on  g.game_id    = b.game_id
left join parlay_cte             pc on  pc.parlay_id = t.parlay_id
left join enhancement_agg        e  on  e.txn_id     = t.txn_id

where t.canceled = false
