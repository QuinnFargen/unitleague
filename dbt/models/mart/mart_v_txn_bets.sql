{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'v_txn_bets'
    )
}}

with leg as (
    select
        t.txn_id,
        t.insert_dt,
        t.bettor_id,
        t.syndicate_id,
        t.txn_type_id,
        t.parlay_id,
        t.unit,
        case when t.parlay_id is not null then vp.bet_hash        else t.bet_hash        end as bet_hash,
        case when t.parlay_id is not null then vp.leg_price       else t.price           end as price,
        case when t.parlay_id is not null then vp.unit_enhanced   else t.unit_enhanced   end as unit_enhanced,
        case when t.parlay_id is not null then vp.price_enhanced  else t.price_enhanced  end as price_enhanced,
        case when t.parlay_id is not null then vp.game_id         else b.game_id         end as game_id,
        case when t.parlay_id is not null then vp.league_id       else b.league_id       end as league_id,
        case when t.parlay_id is not null then vp.bookmaker       else b.bookmaker       end as bookmaker,
        case when t.parlay_id is not null then vp.bet_type        else b.bet_type        end as bet_type,
        case when t.parlay_id is not null then vp.bet_concat      else b.bet_concat      end as bet_concat,
        case when t.parlay_id is not null then vp.points          else b.points          end as points,
        case when t.parlay_id is not null then vp.team_id         else b.team_id         end as team_id
    from {{ source('odd', 'txn') }} t
    left join {{ ref('odd_bet') }}      b  on b.bet_hash   = t.bet_hash  and b.active
    left join {{ ref('odd_v_parlay') }} vp on vp.parlay_id = t.parlay_id
    where t.canceled = false
)

select
    l.txn_id,
    l.insert_dt,
    l.bettor_id,
    l.syndicate_id,
    tt.name           as txn_type,
    l.bet_hash,
    l.parlay_id,
    l.price,
    l.unit,
    l.unit_enhanced,
    l.price_enhanced,
    l.game_id,
    g.game_dt,
    g.game_ts,
    g.home,
    g.away,
    g.h                                                                   as home_score,
    g.a                                                                   as away_score,
    l.league_id,
    l.bookmaker,
    l.bet_type,
    l.bet_concat,
    l.points,
    mt.abbr as team,
    {{ grade_bet('l.bet_type', 'l.points', 'l.team_id', 'g') }} as won

from leg l
left join {{ source('odd', 'txn_type') }} tt on tt.txn_type_id = l.txn_type_id
left join {{ ref('mart_game') }}          g  on g.game_id      = l.game_id
left join {{ ref('mart_team') }}          mt on mt.team_id     = l.team_id
where tt.name != 'unit'
