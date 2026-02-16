CREATE OR REPLACE VIEW etl.v_fetch_espn_sched_update
AS
	select distinct    
	    date_part('YEAR',fs2.gamedate::date) as year
	    ,fs2.season_type as seasontype
	    ,fs2.gameweek as weekno
		,fs2.league_id
	from src.foot_espn_schedule fs2 
	where fs2.gamedate < CURRENT_DATE
	    and fs2.gamedate  > '2025-07-01'
	    and fs2.status_period  = 0	-- Not complete yet	  
	;