{{
    config(
        materialized  = 'incremental',
        unique_key    = ['bet_source_id', 'team_id'],
        schema        = 'odd',
        alias         = 'bet'
    )
}}

with odds_api as (
    select
        o.odd_hash                                                          as bet_source_id,
        o.bookmakers_title                                                  as bookmaker,
        g.source_game_id                                                    as game_id,
        case
            when o.market_name = o.home_team then ht.team_id
            when o.market_name = o.away_team then at.team_id
            else null
        end                                                                 as team_id,
        case
            when o.markets_key = 'h2h'     then 'ML'
            when o.markets_key = 'spreads' then 'SPR'
            when o.markets_key = 'totals'  then upper(split_part(o.market_name, ' ', 1))
            else upper(o.markets_key)
        end                                                                 as bet_type,
        case
            when o.markets_key = 'totals'
                then g.game_concat || '_' || upper(split_part(o.market_name, ' ', 1))
            when o.markets_key = 'h2h'
                then g.game_concat || '_ML_'
                    || case when o.market_name = o.home_team then ht.abbr else at.abbr end
            when o.markets_key = 'spreads'
                then g.game_concat || '_SPR_'
                    || case when o.market_name = o.home_team then ht.abbr else at.abbr end
            else g.game_concat || '_' || upper(o.markets_key)
        end                                                                 as bet_concat,
        o.price::numeric(10, 4)                                             as price,
        o.point                                                             as points,
        o.markets_last_update_ts                                            as start_ts,
        lead(o.markets_last_update_ts) over (
            partition by o.id, o.bookmakers_key, o.markets_key, o.market_name
            order by o.markets_last_update_ts
        )                                                                   as end_ts

    from {{ source('src', 'the_odds_api') }} o
    join {{ ref('ball_v_team') }} ht
        on ht.odds_team_name = o.home_team
    join {{ ref('ball_v_team') }} at
        on at.odds_team_name = o.away_team
    join {{ ref('ball_game') }} g
        on  g.home_team_id = ht.team_id
        and g.away_team_id = at.team_id
        and g.game_dt      = o.commence_ts::date

    {% if is_incremental() %}
    where o.id in (
        select distinct id
        from {{ source('src', 'the_odds_api') }}
        where markets_last_update_ts > (
            select coalesce(max(start_ts), '1900-01-01'::timestamp)
            from {{ this }}
            where bookmaker != 'Kalshi'
        )
    )
    {% endif %}
),

kalshi_home as (
    select
        kg.kalshi_hash                                                      as bet_source_id,
        'Kalshi'                                                            as bookmaker,
        g.source_game_id                                                    as game_id,
        ht.team_id                                                          as team_id,
        'ML'                                                                as bet_type,
        g.game_concat || '_ML_' || ht.abbr                                  as bet_concat,
        round((1.0 / kg.home_yes)::numeric, 4)                              as price,
        null::float                                                         as points,
        kg.insert_ts                                                        as start_ts,
        lead(kg.insert_ts) over (
            partition by kg.event_ticker
            order by kg.insert_ts
        )                                                                   as end_ts

    from {{ source('src', 'kalshi_game') }} kg
    join {{ ref('ball_v_team') }} ht
        on ht.kalshi_team_name = kg.home_team
    join {{ ref('ball_v_team') }} at
        on at.kalshi_team_name = kg.away_team
    join {{ ref('ball_game') }} g
        on  g.home_team_id = ht.team_id
        and g.away_team_id = at.team_id
        and g.game_dt      = kg.game_dt

    {% if is_incremental() %}
    where kg.event_ticker in (
        select distinct event_ticker
        from {{ source('src', 'kalshi_game') }}
        where insert_ts > (
            select coalesce(max(start_ts), '1900-01-01'::timestamp)
            from {{ this }}
            where bookmaker = 'Kalshi'
        )
    )
    {% endif %}
),

kalshi_away as (
    select
        kg.kalshi_hash                                                      as bet_source_id,
        'Kalshi'                                                            as bookmaker,
        g.source_game_id                                                    as game_id,
        at.team_id                                                          as team_id,
        'ML'                                                                as bet_type,
        g.game_concat || '_ML_' || at.abbr                                  as bet_concat,
        round((1.0 / kg.away_yes)::numeric, 4)                              as price,
        null::float                                                         as points,
        kg.insert_ts                                                        as start_ts,
        lead(kg.insert_ts) over (
            partition by kg.event_ticker
            order by kg.insert_ts
        )                                                                   as end_ts

    from {{ source('src', 'kalshi_game') }} kg
    join {{ ref('ball_v_team') }} ht
        on ht.kalshi_team_name = kg.home_team
    join {{ ref('ball_v_team') }} at
        on at.kalshi_team_name = kg.away_team
    join {{ ref('ball_game') }} g
        on  g.home_team_id = ht.team_id
        and g.away_team_id = at.team_id
        and g.game_dt      = kg.game_dt

    {% if is_incremental() %}
    where kg.event_ticker in (
        select distinct event_ticker
        from {{ source('src', 'kalshi_game') }}
        where insert_ts > (
            select coalesce(max(start_ts), '1900-01-01'::timestamp)
            from {{ this }}
            where bookmaker = 'Kalshi'
        )
    )
    {% endif %}
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
    end_ts is null                                                          as active,
    start_ts,
    end_ts

from combined
