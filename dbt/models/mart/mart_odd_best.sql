{{
    config(
        materialized = 'incremental',
        unique_key   = 'game_id',
        schema       = 'mart',
        alias        = 'odd_best'
    )
}}

with active_bets as (
    select distinct on (bet_concat)
        game_id,
        team_id,
        bet_type,
        bet_hash,
        bookmaker,
        price,
        points,
        bet_concat
    from {{ ref('odd_bet') }}
    where active = true
    order by bet_concat, price desc
),

games as (
    select
        game_id,
        league_id,
        game_concat,
        game_dt,
        game_time,
        home            as home_abbr,
        away            as away_abbr,
        home_team_id,
        away_team_id
    from {{ ref('mart_game') }}
    {% if is_incremental() %}
    where game_id in (
        select distinct game_id
        from {{ ref('odd_bet') }}
        where start_ts > (
            select coalesce(max(last_updated_ts), '1900-01-01'::timestamp)
            from {{ this }}
        )
    )
    {% endif %}
)

select
    g.game_id,
    g.league_id,
    g.game_concat,
    g.game_dt,
    g.game_time,
    g.home_abbr,
    g.away_abbr,
    g.home_team_id,
    g.away_team_id,
    case when ml_h.bet_hash is null and ml_a.bet_hash is null
            and spr_h.bet_hash is null and spr_a.bet_hash is null
            and ov.bet_hash is null and un.bet_hash is null
            then false else true end    as has_active_bets,

    ml_h.bet_hash        as ml_home_bet_hash,
    ml_h.bookmaker       as ml_home_bookmaker,
    ml_h.price           as ml_home_price,
    -- ml_h.bet_concat      as ml_home_bet_concat,

    ml_a.bet_hash        as ml_away_bet_hash,
    ml_a.bookmaker       as ml_away_bookmaker,
    ml_a.price           as ml_away_price,
    -- ml_a.bet_concat      as ml_away_bet_concat,

    spr_h.bet_hash       as spr_home_bet_hash,
    spr_h.bookmaker      as spr_home_bookmaker,
    spr_h.price          as spr_home_price,
    spr_h.points         as spr_home_points,
    -- spr_h.bet_concat     as spr_home_bet_concat,

    spr_a.bet_hash       as spr_away_bet_hash,
    spr_a.bookmaker      as spr_away_bookmaker,
    spr_a.price          as spr_away_price,
    spr_a.points         as spr_away_points,
    -- spr_a.bet_concat     as spr_away_bet_concat,

    ov.bet_hash          as over_bet_hash,
    ov.bookmaker         as over_bookmaker,
    ov.price             as over_price,
    ov.points            as over_points,
    -- ov.bet_concat        as over_bet_concat,

    un.bet_hash          as under_bet_hash,
    un.bookmaker         as under_bookmaker,
    un.price             as under_price,
    un.points            as under_points,
    -- un.bet_concat        as under_bet_concat,

    current_timestamp    as last_updated_ts

from games g
left join active_bets ml_h  on ml_h.game_id  = g.game_id and ml_h.bet_type  = 'ML'    and ml_h.team_id  = g.home_team_id
left join active_bets ml_a  on ml_a.game_id  = g.game_id and ml_a.bet_type  = 'ML'    and ml_a.team_id  = g.away_team_id
left join active_bets spr_h on spr_h.game_id = g.game_id and spr_h.bet_type = 'SPR'   and spr_h.team_id = g.home_team_id
left join active_bets spr_a on spr_a.game_id = g.game_id and spr_a.bet_type = 'SPR'   and spr_a.team_id = g.away_team_id
left join active_bets ov    on ov.game_id    = g.game_id and ov.bet_type    = 'OVER'  and ov.team_id    is null
left join active_bets un    on un.game_id    = g.game_id and un.bet_type    = 'UNDER' and un.team_id    is null
