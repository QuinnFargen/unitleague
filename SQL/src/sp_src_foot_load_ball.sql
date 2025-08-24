CREATE OR REPLACE PROCEDURE api.sp_src_foot_load_ball()
 LANGUAGE plpgsql
AS $procedure$
begin


	CREATE TEMP TABLE both_game_team_summary AS
		select 			
			5 as league_id 
			,a.*
		from api.stg_cfb_game_team_summary a
		union
		select 
			2 as league_id 
			,a.*
		from api.stg_nfl_game_team_summary a			
		;

		insert into src.foot_game_team_summary
		select distinct a.*
		from both_game_team_summary a
		left join src.foot_game_team_summary s on a.league_id = s.league_id and a.event_id = s.event_id and a.team_id = s.team_id
		where s.foot_game_team_summary_id is null;

		truncate table api.stg_cfb_game_team_summary;
		truncate table api.stg_nfl_game_team_summary;

		
	CREATE TEMP TABLE both_defensive_box AS
		select 		
			5 as league_id 			 
			,a.event_id
			,a.team_id
			,a.team_abbr
			,a.team_name
			,a.athlete_id
			,a.athlete_name
			,a.athlete_jersey
			,a.totaltackles
			,a.solotackles
			,a.sacks
			,a.tacklesforloss
			,a.passesdefended
			,a.qbhits
			,a.defensivetouchdowns
			,a.interceptions
			,a.interceptionyards
			,a.interceptiontouchdowns
			,a.hurries
		from api.stg_cfb_defensive_box a
		union
		select 
			2 as league_id 			 
			,a.event_id
			,a.team_id
			,a.team_abbr
			,a.team_name
			,a.athlete_id
			,a.athlete_name
			,a.athlete_jersey
			,a.totaltackles
			,a.solotackles
			,a.sacks
			,a.tacklesforloss
			,a.passesdefended
			,a.qbhits
			,a.defensivetouchdowns
			,a.interceptions
			,a.interceptionyards
			,a.interceptiontouchdowns
			,null as hurries
		from api.stg_nfl_defensive_box a			
		;

		insert into src.foot_defensive_box
		select distinct a.*
		from both_defensive_box a
		left join src.foot_defensive_box s on a.league_id = s.league_id and a.event_id = s.event_id and a.team_id = s.team_id and a.athlete_id = s.athlete_id
		where s.foot_defensive_box_id is null;

		truncate table api.stg_cfb_defensive_box;
		truncate table api.stg_nfl_defensive_box;


	CREATE TEMP TABLE both_offensive_box AS
		select 
			5 as league_id 	
			,a.*	
		from api.stg_cfb_offensive_box a
		union
		select 
			2 as league_id 	
			,a.*
		from api.stg_nfl_offensive_box a			
		;

		insert into src.foot_offensive_box
		select distinct a.*
		from both_offensive_box a
		left join src.foot_offensive_box s on a.league_id = s.league_id and a.event_id = s.event_id and a.team_id = s.team_id and a.athlete_id = s.athlete_id
		where s.foot_offensive_box_id is null;

		truncate table api.stg_cfb_offensive_box;
		truncate table api.stg_nfl_offensive_box;


	CREATE TEMP TABLE both_special_teams_box AS
		select 
			5 as league_id 	
			,a.*	
		from api.stg_cfb_special_teams_box a
		union
		select 
			2 as league_id 	
			,a.*
		from api.stg_nfl_special_teams_box a			
		;
	
		insert into src.foot_special_teams_box
		select distinct a.*
		from both_offensive_box a
		left join src.foot_special_teams_box s on a.league_id = s.league_id and a.event_id = s.event_id and a.team_id = s.team_id and a.athlete_id = s.athlete_id
		where s.foot_special_teams_box_id is null;

		truncate table api.stg_cfb_special_teams_box;
		truncate table api.stg_nfl_special_teams_box;


	CREATE TEMP TABLE both_schedule AS
		select 
			5 as league_id 	
			,a.*	
		from api.stg_cfb_schedule a
		union
		select 
			2 as league_id 	
			,a.*
		from api.stg_nfl_schedule a			
		;
	

		delete
		from src.foot_schedule s 
		using both_schedule a 
		where a.league_id = s.league_id and a.game_id = s.game_id
			and a.status_period <> 0		-- is complete in api 
			and s.status_period = 0 ;	-- is not complete in src


		insert into src.foot_schedule(league_id,game_id,gamedate,name,shortname,week_number,season_year,season_type,season_slug,status_period
									,home_score,home_team,home_abbr,home_short,home_team_id,seasontype,seasontypelabel,gameweek,weeklabel
									,q1_home,q2_home,q3_home,q4_home,q5_home
									,away_score,away_team,away_abbr,away_short,away_team_id
									,q1_away,q2_away,q3_away,q4_away,q5_away)
		select distinct 
			 a.league_id
			,a.game_id
			,a.gamedate
			,a."name"
			,a.shortname
			,a.week_number
			,a.season_year
			,a.season_type
			,a.season_slug
			,a.status_period
			,a.home_score
			,a.home_team
			,a.home_abbr
			,a.home_short
			,a.home_team_id
			,a.seasontype
			,a.seasontypelabel
			,a.gameweek
			,a.weeklabel
			,a.q1_home
			,a.q2_home
			,a.q3_home
			,a.q4_home
			,a.q5_home
			,a.away_score
			,a.away_team
			,a.away_abbr
			,a.away_short
			,a.away_team_id
			,a.q1_away
			,a.q2_away
			,a.q3_away
			,a.q4_away
			,a.q5_away
		from both_schedule a
		left join src.foot_schedule s on a.league_id = s.league_id and a.game_id = s.game_id
		where s.game_id is null;


		truncate table api.stg_cfb_schedule;
		truncate table api.stg_nfl_schedule;


-------------------------------------------


	UPDATE ball.game g
	set h = s.home_score, a = s.away_score
		,won_team_id = case when s.home_score >= s.away_score then g.home_team_id when s.home_score < s.away_score then g.away_team_id else null end
		,h1 = s.q1_home ,h2 = s.q2_home ,h3 = s.q3_home ,h4 = s.q4_home ,h5 = s.q5_home 
		,a1 = s.q1_away ,a2 = s.q2_away ,a3 = s.q3_away ,a4 = s.q4_away ,a5 = s.q5_away 
		,is_pre = case when s.season_type = 1 then true else false end
		,update_dt = now()
	from src.foot_schedule s 
	where g.league_id = s.league_id and g.source_game_id = s.game_id
		and g.game_dt < CURRENT_DATE
		and g.game_dt > '2025-07-01'
		and g.won_team_id is null
		and s.status_period <> 0 -- src game complete
	;


	update ball.sched a
	set team = case when a.team_id = g.home_team_id then g.h else g.a end
		,opp = case when a.team_id = g.away_team_id then g.a else g.h end
		,won = case when a.team_id = g.won_team_id then true else false end
	from ball.game g 
	where a.game_id = g.game_id
		and g.game_dt > '2025-07-01'
		and a.team = 0 and a.opp = 0 and g.won_team_id is not null
	;




end; $procedure$
;
