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
    s.is_public,
    s.max_runner,
    s.created_by_bettor_id

from {{ source('odd', 'syndicate') }} s