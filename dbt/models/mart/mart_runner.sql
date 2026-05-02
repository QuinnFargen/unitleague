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
        (1, 1, 1, 'admin',  true, 142, 'SharpMike',    'figure.american.football.circle.fill',  'red'),
        (2, 102, 1, 'member', true,  87, 'LineBuster',   'figure.basketball.circle.fill',          'teal'),
        (3, 103, 1, 'member', true, 195, 'GrindQueen',   'figure.baseball.circle.fill',            'green'),
        (4, 104, 2, 'admin',  true,  63, 'ValueHunter',  'figure.hockey.circle.fill',              'yellow'),
        (5, 105, 2, 'member', true, 118, 'SlateBlue',    'figure.pickleball.circle.fill',          'stadium')
) as t(runner_id, bettor_id, syndicate_id, role, active, balance, profile_name, symbol, color)
