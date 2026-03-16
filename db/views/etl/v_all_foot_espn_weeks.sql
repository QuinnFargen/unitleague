CREATE OR REPLACE VIEW etl.v_all_foot_espn_weeks
AS 
	SELECT distinct
		case when a.league = 'CFB' then 5 when a.league = 'NFL' then 2 else null end as league_id,
		a.yr,
		a.season_label,
		a.season_value::smallint as season_value,
		a.week_label,
		a.week_value::smallint as week_value,
		a.week_start_date,
		a.week_end_date
		-- SELECT *
	from etl.stg_foot_espn_weeks a
	left join etl.all_foot_espn_weeks b on a.yr = b.yr and b.league_id = case when a.league = 'CFB' then 5 when a.league = 'NFL' then 2 else null end
										and a.season_value::smallint = b.season_value and a.week_value::smallint = b.week_value 
										and a.week_start_date = b.week_start_date
	where b.week_id is null
	;