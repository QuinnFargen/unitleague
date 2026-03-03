CREATE OR REPLACE PROCEDURE utility.seed_ball_game_sched()
LANGUAGE plpgsql
AS $$
BEGIN

-----------------------
-- Load ball.game from src.foot_schedule

	truncate table ball.game;

	insert into ball.game (league_id, home_team_id, away_team_id, game_dt, game_time, h, a, won_team_id, source_game_id, game_concat, 
							a1, a2, a3, a4, a5, a6, a7, a8, a9, a10,
							h1, h2, h3, h4, h5, h6, h7, h8, h9, h10	)					
	select 
		fs2.league_id, coalesce(t.team_id,(fs2.league_id * 10000)), coalesce(a.team_id,(fs2.league_id * 10000)), fs2.gamedate::date, fs2.gamedate::time, fs2.home_score, fs2.away_score
		,case 	when fs2.home_score = 0 and fs2.away_score = 0 then null
				when fs2.home_score > fs2.away_score then coalesce(t.team_id,(fs2.league_id * 10000)) 
				else coalesce(a.team_id,(fs2.league_id * 10000)) end
		,(fs2.game_id) as source_game_id
		,coalesce(t.team_concat, l.name || '_TBD') || '_' || coalesce(a.abbr,'TBD') || '_' || TO_CHAR(fs2.gamedate, 'YYYYMMDD')
		,case when fs2.a1 = 0 and fs2.a1 = 0 then null else fs2.a1 end as a1
		,case when fs2.a2 = 0 and fs2.a2 = 0 then null else fs2.a2 end as a2
		,case when fs2.a3 = 0 and fs2.a3 = 0 then null else fs2.a3 end as a3
		,case when fs2.a4 = 0 and fs2.a4 = 0 then null else fs2.a4 end as a4
		,case when fs2.a5 = 0 and fs2.a5 = 0 then null else fs2.a5 end as a5
		,case when fs2.a6 = 0 and fs2.a6 = 0 then null else fs2.a6 end as a6
		,case when fs2.a7 = 0 and fs2.a7 = 0 then null else fs2.a7 end as a7
		,case when fs2.a8 = 0 and fs2.a8 = 0 then null else fs2.a8 end as a8
		,case when fs2.a9 = 0 and fs2.a9 = 0 then null else fs2.a9 end as a9
		,case when fs2.a10 = 0 and fs2.a10 = 0 then null else fs2.a10 end as a10

		,case when fs2.h1 = 0 and fs2.h1 = 0 then null else fs2.h1 end as h1
		,case when fs2.h2 = 0 and fs2.h2 = 0 then null else fs2.h2 end as h2
		,case when fs2.h3 = 0 and fs2.h3 = 0 then null else fs2.h3 end as h3
		,case when fs2.h4 = 0 and fs2.h4 = 0 then null else fs2.h4 end as h4
		,case when fs2.h5 = 0 and fs2.h5 = 0 then null else fs2.h5 end as h5
		,case when fs2.h6 = 0 and fs2.h6 = 0 then null else fs2.h6 end as h6
		,case when fs2.h7 = 0 and fs2.h7 = 0 then null else fs2.h7 end as h7
		,case when fs2.h8 = 0 and fs2.h8 = 0 then null else fs2.h8 end as h8
		,case when fs2.h9 = 0 and fs2.h9 = 0 then null else fs2.h9 end as h9
		,case when fs2.h10 = 0 and fs2.h10 = 0 then null else fs2.h10 end as h10

	--	,t.abbr , a.abbr , fs2.shortname, fs2.*
	-- SELECT COUNT(*)	-- select fs2.*
	from src.espn_schedule fs2 	-- 94065
	join ball.league l on fs2.league_id = l.league_id
	left join ball.v_team t on fs2.league_id = t.league_id and fs2.home_team_id = t.espn_team_id
	left join ball.v_team a on fs2.league_id = a.league_id and fs2.away_team_id = a.espn_team_id
	where t.team_id  is not null or a.team_id is not null	-- 14923
			-- lose some to pro all stars, tbd or weird college basketball that we don't have the team yet for
--	order by fs2.game_id desc
	;	-- 92789

-----------------------
-- Load ball.sched from ball.game
	
	truncate table ball.sched;

	insert into ball.sched (league_id,season_id,team_id,game_num,opp_team_id,sched_concat,game_dt,home, won,team, opp, game_id)
	select 
		g.league_id, s.season_id
		, g.home_team_id 
		,1 as game_num -- will need to update after
		, g.away_team_id 
		,s.season_concat -- will need to update affter
		,g.game_dt 
		,true as home
		,case when g.won_team_id = g.home_team_id then true else false end as won
		,g.h as team
		,g.a as opp
		,g.game_id 
	--	,g.*
		-- SELECT COUNT(*)
	from ball.game g 	-- 14923
	join ball.season s on g.league_id = s.league_id and g.game_dt between s.reg_start_dt - interval '2 month' and s.champ_dt + interval '2 month'
	where 1=1
		and g.home_team_id % 10000 <> 0	-- Not unknown team

	union
	
	select 
		g.league_id, s.season_id
		, g.away_team_id 
		,1 as game_num -- will need to update after
		, g.home_team_id 
		,s.season_concat -- will need to update affter
		,g.game_dt 
		,false as home
		,case when g.won_team_id = g.away_team_id then true else false end as won
		,g.a as team
		,g.h as opp
		,g.game_id 
	--	,g.*
		-- SELECT COUNT(*)
	from ball.game g 	-- 14923
	join ball.season s on g.league_id = s.league_id and g.game_dt between s.reg_start_dt - interval '2 month' and s.champ_dt + interval '2 month'
	where 1=1
		and g.away_team_id % 10000 <> 0	-- Not unknown team		
	;-- 27962


	UPDATE ball.sched
	set sched_concat = a.sched_concat
		,game_num = a.game_num  
	from (
		select s.sched_id
			,s.sched_concat || '_' || t.abbr || '_' || (row_number() OVER(partition by s.season_id, s.team_id	--, s.is_pre, s.is_post 
								order by s.GAME_DT ))::varchar 
							|| '_' || o.abbr --|| case when is_post then '_POST' when is_pre then '_PRE' else '' end
								 as SCHED_CONCAT
			,row_number() OVER(partition by s.SEASON_ID, s.team_id	--, s.is_pre, s.is_post 
									order by s.GAME_DT ) as GAME_NUM
		from ball.sched s
		join ball.team t on s.team_id  = t.team_id 
		join ball.team o on s.opp_team_id  = o.team_id 
	--	where s.season_id   = 202024 and s.team_id   = 22503
		) a
	where ball.sched.sched_id  = a.sched_id;

END;
$$;