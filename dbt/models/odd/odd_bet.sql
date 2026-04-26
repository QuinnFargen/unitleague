{{
    config(
        materialized = 'table',
        schema       = 'odd',
        alias        = 'bet'
    )
}}

with odds_api as (
    select
        o.odd_hash                                                          as bet_source_id,
        o.bookmakers_title                                                  as bookmaker,
        g.source_game_id,
        case
            when o.market_name = o.home_team then ht.team_id
            when o.market_name = o.away_team then at.team_id
            else null
        end                                                                 as team_id,
        o.markets_key                                                       as bet_type,
        g.game_concat || '_' || upper(o.markets_key)                       as bet_concat,
        o.price,
        o.point                                                             as points,
        o.markets_last_update_ts                                            as start_ts,
        null::timestamp                                                     as end_ts

    from {{ source('src', 'the_odds_api') }} o
    join {{ ref('ball_v_team') }} ht
        on ht.odds_team_name = o.home_team
    join {{ ref('ball_v_team') }} at
        on at.odds_team_name = o.away_team
    join {{ ref('ball_game') }} g
        on  g.home_team_id = ht.team_id
        and g.away_team_id = at.team_id
        and g.game_dt      = o.commence_ts::date
),

kalshi_home as (
    select
        kg.kalshi_hash                                          as bet_source_id,
        'Kalshi'                                                            as bookmaker,
        kg.game_id,
        kg.home_team_id                                                     as team_id,
        'h2h'                                                               as bet_type,
        g.game_concat || '_H2H'                                            as bet_concat,
        100.0 / kg.home_yes                                                as price,
        null::float                                                         as points,
        kg.insert_ts                                                        as start_ts,
        null::timestamp                                                     as end_ts

    from {{ source('src', 'kalshi_game') }} kg
    join {{ ref('ball_game') }} g
        on g.source_game_id = kg.game_id
    where kg.is_latest = true
),

kalshi_away as (
    select
        kg.kalshi_game_id::varchar                                          as bet_source_id,
        'Kalshi'                                                            as bookmaker,
        kg.game_id,
        kg.away_team_id                                                     as team_id,
        'h2h'                                                               as bet_type,
        g.game_concat || '_H2H'                                            as bet_concat,
        100.0 / kg.away_yes                                                as price,
        null::float                                                         as points,
        kg.insert_ts                                                        as start_ts,
        null::timestamp                                                     as end_ts

    from {{ source('src', 'kalshi_game') }} kg
    join {{ ref('ball_game') }} g
        on g.game_id = kg.game_id
    where kg.is_latest = true
),

combined as (
    select * from odds_api
    union all
    select * from kalshi_home
    union all
    select * from kalshi_away
)

select
    md5(bookmaker || game_id::varchar || coalesce(team_id::varchar, 'total') || bet_type) as bet_hash,
    bet_source_id,
    bookmaker,
    game_id,
    team_id,
    bet_type,
    bet_concat,
    price,
    points,
    true                                                                    as active,
    start_ts,
    end_ts

from combined