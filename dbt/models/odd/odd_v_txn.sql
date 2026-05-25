{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'odd_v_txn'
    )
}}

select
    t.txn_id,
    t.insert_dt,
    t.bettor_id,
    t.syndicate_id,
    t.unit,
    tt.name                                                              as txn_type,
    coalesce(t.bet_hash,   vp.bet_hash)    as bet_hash,
    coalesce(b.price,      vp.leg_price)   as price,
    t.game_dt,
    t.canceled,
    t.cancel_ts,
    coalesce(b.bookmaker,  vp.bookmaker)   as bookmaker,
    coalesce(b.bet_type,   vp.bet_type)    as bet_type,
    coalesce(b.bet_concat, vp.bet_concat)  as bet_concat,
    coalesce(b.points,     vp.points)      as points,
    mt.abbr                                as team,
    t.parlay_id,
    vp.price_mult                          as parlay_price_mult,
    coalesce(b.game_id,    vp.game_id)     as game_id,
    coalesce(b.league_id,  vp.league_id)   as league_id,
    coalesce(g.home,       vp.home)        as home,
    coalesce(g.away,       vp.away)        as away,
    coalesce(g.game_time,  vp.game_time)   as game_time

from {{ source('odd', 'txn') }} t
left join {{ source('odd', 'txn_type') }}   tt on  tt.txn_type_id = t.txn_type_id
left join {{ ref('odd_bet') }}      b  on  b.bet_hash   = t.bet_hash    and b.active
left join {{ ref('odd_v_parlay') }} vp on  vp.parlay_id = t.parlay_id
left join {{ ref('mart_game') }}    g  on  g.game_id    = b.game_id
left join {{ ref('mart_team') }}    mt on  mt.team_id   = b.team_id
