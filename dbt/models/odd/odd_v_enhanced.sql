{{
    config(
        materialized = 'view',
        schema       = 'odd',
        alias        = 'v_enhanced'
    )
}}

with active_enhanced as (

    select
        ed.bettor_id,
        ed.syndicate_id,
        ed.runner_id,
        ed.team_id,
        en.enhancement_type,
        coalesce(t.abbr, en.name) as name,
        t.league_id,
        ed.level

    from {{ source('odd', 'enhanced') }} ed
    join {{ source('odd', 'enhancement') }} en
        on en.enhancement_id = ed.enhancement_id
    left join {{ ref('mart_team') }} t
        on  t.team_id = ed.team_id
        and ed.team_id != 0
    where ed.active = true

)

select
    bettor_id,
    syndicate_id,
    runner_id,
    team_id,
    enhancement_type,
    name,
    league_id,
    sum(level) as level
from active_enhanced
group by bettor_id, syndicate_id, runner_id, team_id, enhancement_type, name, league_id
