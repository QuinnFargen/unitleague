{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'syndicate'
    )
}}

select
    s.syndicate_id,
    s.name,
    s.description,
    s.fantasy,
    s.max_runner,
    s.created_by_bettor_id

from {{ source('odd', 'syndicate') }} s