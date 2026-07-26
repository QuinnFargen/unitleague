{% macro grade_bet(bet_type, points, team_id, game_alias) -%}
case
    when {{ bet_type }} = 'ML'  and {{ game_alias }}.won_team_id is not null
        then {{ team_id }} = {{ game_alias }}.won_team_id
    when {{ bet_type }} = 'SPR' and {{ game_alias }}.h is not null and {{ points }} is not null
        then case
                when {{ team_id }} = {{ game_alias }}.home_team_id then ({{ game_alias }}.h + {{ points }}) > {{ game_alias }}.a
                when {{ team_id }} = {{ game_alias }}.away_team_id then ({{ game_alias }}.a + {{ points }}) > {{ game_alias }}.h
             end
    when {{ bet_type }} = 'OVER'  and {{ game_alias }}.h is not null and {{ points }} is not null
        then ({{ game_alias }}.h + {{ game_alias }}.a) > {{ points }}
    when {{ bet_type }} = 'UNDER' and {{ game_alias }}.h is not null and {{ points }} is not null
        then ({{ game_alias }}.h + {{ game_alias }}.a) < {{ points }}
end
{%- endmacro %}
