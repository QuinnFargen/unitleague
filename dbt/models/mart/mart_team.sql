{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'team'
    )
}}

select * from {{ ref('ball_v_team') }}
