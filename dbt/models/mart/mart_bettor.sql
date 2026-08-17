{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'bettor'
    )
}}

select
    b.bettor_id,
    b.profile_name,
    b.symbol,
    b.color,
    b.favorite_team_id,
    t.abbr       as favorite_team_abbr,
    t.league_id  as favorite_league_id,

    coalesce(sum(
        case
            when tx.txn_type = 'unit' then tx.unit
            when tx.won               then tx.unit * tx.price
            else                          -tx.unit
        end
    ), 0) as career_balance

from {{ source('odd', 'bettor') }} b
left join {{ ref('mart_team') }} t  on t.team_id    = b.favorite_team_id
left join {{ ref('mart_txn') }}  tx on tx.bettor_id = b.bettor_id

group by
    b.bettor_id,
    b.profile_name,
    b.symbol,
    b.color,
    b.favorite_team_id,
    t.abbr,
    t.league_id
