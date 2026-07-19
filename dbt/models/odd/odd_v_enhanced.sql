{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'v_enhanced'
    )
}}

select
    ed.*,
    t.abbr as team_abbr

from {{ source('odd', 'enhanced') }} ed
left join {{ ref('mart_team') }} t
    on  t.team_id = ed.team_id
    and ed.team_id != 0
