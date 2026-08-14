{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'txn'
    )
}}

with parlay_cte as (
    select
        vp.parlay_id,
        max(g.game_dt) as game_dt
    from {{ ref('odd_v_parlay') }} vp
    left join {{ ref('mart_game') }} g on g.game_id = vp.game_id
    group by vp.parlay_id
),

parlay_leg_grade as (
    select
        vp.parlay_id,
        {{ grade_bet('vp.bet_type', 'vp.points', 'vp.team_id', 'g') }} as leg_won
    from {{ ref('odd_v_parlay') }} vp
    left join {{ ref('mart_game') }} g on g.game_id = vp.game_id
),

parlay_won as (
    select
        parlay_id,
        case
            when bool_or(leg_won is false) then false
            when bool_or(leg_won is null)  then null
            else true
        end as won
    from parlay_leg_grade
    group by parlay_id
)

select
    t.txn_id,
    t.insert_dt,
    t.bettor_id,
    t.syndicate_id,
    tt.name                                                              as txn_type,
    t.bet_hash,
    t.parlay_id,
    t.price,
    t.unit,
    t.unit_enhanced,
    t.price_enhanced,
    b.game_id,
    coalesce(g.game_dt, pc.game_dt)                                      as game_dt,
    g.game_ts,
    g.home,
    g.away,
    g.h                                                                   as home_score,
    g.a                                                                   as away_score,
    b.league_id,
    b.bookmaker,
    b.bet_type,
    b.bet_concat,
    b.points,
    mt.abbr                                                               as team,
    case
        when t.parlay_id is not null then pw.won
        else {{ grade_bet('b.bet_type', 'b.points', 'b.team_id', 'g') }}
    end                                                                   as won

from {{ source('odd', 'txn') }} t
left join {{ source('odd', 'txn_type') }} tt on  tt.txn_type_id = t.txn_type_id
left join {{ ref('odd_bet') }}   b  on  b.bet_hash   = t.bet_hash and b.active
left join {{ ref('mart_game') }} g  on  g.game_id    = b.game_id
left join parlay_cte              pc on  pc.parlay_id = t.parlay_id
left join parlay_won              pw on  pw.parlay_id = t.parlay_id
left join {{ ref('mart_team') }} mt on  mt.team_id   = b.team_id

where t.canceled = false
