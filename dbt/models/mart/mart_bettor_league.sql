{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'bettor_league'
    )
}}

select
    t.bettor_id,
    t.league_id,

    coalesce(sum(
        case
            when t.won then t.unit * t.price
            else            -t.unit
        end
    ), 0) as balance

from {{ ref('mart_txn') }} t
where t.league_id is not null
group by t.bettor_id, t.league_id
