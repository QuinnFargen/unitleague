{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'v_parlay'
    )
}}

select
    p.parlay_id,
    p.insert_ts,
    p.price_mult,
    l.bet_hash,
    l.price         as leg_price,
    b.game_id,
    b.bookmaker,
    b.bet_type,
    b.bet_concat,
    b.points,
    g.home,
    g.away,
    g.game_time

from {{ source('odd', 'parlay') }} p
join {{ source('odd', 'leg') }}    l  on  l.parlay_id = p.parlay_id
left join {{ ref('odd_bet') }}     b  on  b.bet_hash  = l.bet_hash  and b.active
left join {{ ref('mart_game') }}   g  on  g.game_id   = b.game_id
