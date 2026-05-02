{{
    config(
        materialized = 'view',
        schema       = 'mart',
        alias        = 'runner'
    )
}}

select
    runner_id,
    bettor_id,
    syndicate_id,
    role,
    active,
    balance,
    profile_name,
    symbol,
    color
from (
    values
        (1, 101, 1, 'admin',  true, 142, 'SharpMike',    'figure.american.football.circle.fill',  '#E63946'),
        (2, 102, 1, 'member', true,  87, 'LineBuster',   'figure.basketball.circle.fill',          '#457B9D'),
        (3, 103, 1, 'member', true, 195, 'GrindQueen',   'figure.baseball.circle.fill',            '#2A9D8F'),
        (4, 104, 2, 'admin',  true,  63, 'ValueHunter',  'figure.hockey.circle.fill',              '#F4A261'),
        (5, 105, 2, 'member', true, 118, 'SlateBlue',    'figure.pickleball.circle.fill',          '#6A4C93')
) as t(runner_id, bettor_id, syndicate_id, role, active, balance, profile_name, symbol, color)
