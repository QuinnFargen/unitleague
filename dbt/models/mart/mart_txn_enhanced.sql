{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'txn_enhanced'
    )
}}

with single_bets as (
    select
        txn_id,
        bettor_id,
        syndicate_id,
        bet_hash
    from {{ source('odd', 'txn') }}
    where bet_hash is not null
),

bet_info as (
    select distinct on (bet_hash)
        bet_hash,
        game_id,
        team_id,
        bet_type
    from {{ ref('odd_bet') }}
    order by bet_hash
),

bet_weeks as (
    select
        bi.bet_hash,
        bi.game_id,
        bi.team_id,
        bi.bet_type,
        bw.week_id
    from bet_info bi
    join {{ ref('ball_game') }} bg on bg.game_id   = bi.game_id
    join {{ ref('ball_week') }} bw on bw.league_id = bg.league_id
                                  and bg.game_dt between bw.week_start_dt and bw.week_end_dt
),

txn_with_week as (
    select
        sb.txn_id,
        sb.bettor_id,
        sb.syndicate_id,
        bw.bet_hash,
        bw.game_id,
        bw.team_id,
        bw.bet_type,
        bw.week_id
    from single_bets sb
    join bet_weeks bw on bw.bet_hash = sb.bet_hash
)

select
    tw.txn_id,
    ed.enhanced_id,
    ed.enhancement_id,
    en.enhancement_type,
    en.name                                                             as enhancement_name,
    ed.level,
    1 + ((en.config->>'price_mult')::float - 1) * ed.level            as price_mult,
    1 + ((en.config->>'unit_mult')::float  - 1) * ed.level            as unit_mult

from txn_with_week tw
join {{ source('odd', 'enhanced') }} ed
    on  ed.bettor_id    = tw.bettor_id
    and ed.syndicate_id = tw.syndicate_id
    and ed.week_id      = tw.week_id
    and ed.active       = true
join {{ source('odd', 'enhancement') }} en
    on  en.enhancement_id = ed.enhancement_id
    and en.active         = true
where
    (en.enhancement_type = 'clv' and ed.team_id = 0 and (
        (en.bet_type = 'h2h'    and tw.bet_type = 'ML') or
        (en.bet_type = 'spread' and tw.bet_type = 'SPR') or
        (en.bet_type = 'total'  and tw.bet_type in ('OVER', 'UNDER'))
    ))
    or (en.enhancement_type = 'edge' and ed.team_id = 0)
    or (en.enhancement_type = 'team' and ed.team_id = tw.team_id)
