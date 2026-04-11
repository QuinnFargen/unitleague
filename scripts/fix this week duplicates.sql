

-- Might need to modify with min/max of dates in the week to account for pre/reg week date overlap
-- might be a src schedule issue.

-- then finish seed_ball_game_sched with adding in week_id to ball.sched


CREATE OR REPLACE VIEW src.v_espn_week
AS 
	with cte_src_week as (
		select 
			es.league_id,
			es.season_year as yr,
			c.start_of_week as week_start_dt,
			c.end_of_week + 1 as week_end_dt,
			min(es.gamedate::date) as week_min_dt,
			max(es.gamedate::date) as week_max_dt,
			case when es.season_slug = 'preseason' then true else false end as is_pre,
			case when es.season_slug in ('post-season','play-in-season') then true else false end as is_post
		from src.espn_schedule es 
		join utility.calendar c on es.gamedate::date = c.thedate
		where es.league_id not in (2,5) -- foot is above
		group by es.league_id,
			c.start_of_week,
			c.end_of_week,
			case when es.season_slug = 'preseason' then true else false end,
			case when es.season_slug in ('post-season','play-in-season') then true else false end,
			es.season_year
		)
	select 
		c.league_id,
		c.yr,
		c.week_start_dt,
		c.week_end_dt,
		c.week_min_dt,
		c.week_max_dt,
		row_number() over(partition by c.league_id, c.yr, c.is_pre, c.is_post order by c.week_start_dt ) as week_num,
		case when c.is_pre then 'Preseason Week ' || (row_number() over(partition by c.league_id, c.yr, c.is_pre, c.is_post order by c.week_start_dt ) )::varchar 
			 when c.is_post then 'Postseason Week ' || (row_number() over(partition by c.league_id, c.yr, c.is_pre, c.is_post order by c.week_start_dt ) )::varchar 
				else 'Week ' || (row_number() over(partition by c.league_id, c.yr, c.is_pre, c.is_post order by c.week_start_dt ) )::varchar 
			 end as week_name,
		c.is_pre,
		c.is_post	
	from cte_src_week c
	where c.league_id = 4 and c.week_start_dt between '2024-03-10' and '2024-03-31'
	
	union all

	select 
		f.league_id,
		f.yr,
		f.week_start_date as week_start_dt,
		f.week_end_date as week_end_dt,
		f.week_value as week_num,
		f.week_label as week_name,
		case when f.season_value = 1 then true else false end as is_pre,
		case when f.season_value in (3,5) then true else false end as is_post
	from etl.all_foot_espn_weeks f
	where f.season_value <> 4 -- off season
		and case when f.season_value = 3 and f.week_label like 'Week%' then 0 else 1 end = 1
	;
	