{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'game_oddall'
    )
}}

select distinct on (b.game_id, b.bet_concat, b.price, b.points)
    b.bet_hash,
    b.bookmaker,
    b.game_id,
    b.league_id,
    g.game_dt,
    g.game_time,
    g.home          as home_abbr,
    g.away          as away_abbr,
    b.team_id,
    b.bet_type,
    b.bet_concat,
    b.price,
    b.points,
    b.start_ts,
    mt.abbr         as team_abbr

from {{ ref('odd_bet') }}    b
left join {{ ref('mart_game') }} g  on  g.game_id  = b.game_id
left join {{ ref('mart_team') }} mt on  mt.team_id = b.team_id

where b.active = true

order by b.game_id, b.bet_concat, b.price, b.points
