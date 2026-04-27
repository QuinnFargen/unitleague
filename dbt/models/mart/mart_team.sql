{{
    config(
        materialized = 'table',
        schema       = 'mart',
        alias        = 'team'
    )
}}

with active_season as (
    select league_id, season_id
    from {{ ref('season') }}
    where current_date between reg_start_dt and champ_dt
),

current_record as (
    select
        s.team_id,
        sum(case when s.won then 1 else 0 end)                         as current_wins,
        sum(case when not s.won then 1 else 0 end)                     as current_losses
    from {{ ref('ball_sched') }} s
    join active_season a on a.season_id = s.season_id
    where s.team is not null
    group by s.team_id
)

select
    t.*,
    coalesce(r.current_wins,   0)                                      as current_wins,
    coalesce(r.current_losses, 0)                                      as current_losses

from {{ ref('ball_v_team') }} t
left join current_record r on r.team_id = t.team_id
