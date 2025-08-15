



select * from src.foot_schedule fs2 ;

select * from ball.game g ;

-- Change the 50000 to 60000 for nfl/cfb!!!!!!!

insert into ball.game (league_id, home_team_id, away_team_id, game_dt, game_time, h, a, won_team_id, source_game_id, game_concat, 
						a1, a2, a3, a4, a5, h1, h2, h3, h4, h5	)					
select 
	fs2.league_id, coalesce(t.team_id,(fs2.league_id * 10000)), coalesce(a.team_id,(fs2.league_id * 10000)), fs2.gamedate::date, fs2.gamedate::time, fs2.home_score, fs2.away_score
	,case when fs2.home_score > fs2.away_score then coalesce(t.team_id,(fs2.league_id * 10000)) else coalesce(a.team_id,(fs2.league_id * 10000)) end
	,(fs2.game_id) as source_game_id
	,coalesce(t.team_concat,'CFB_TBD') || '_' || coalesce(a.abbr,'TBD') || '_' || TO_CHAR(fs2.gamedate, 'YYYYMMDD')
	,fs2.q1_away,fs2.q2_away,fs2.q3_away,fs2.q4_away,case when fs2.q5_away = 0 and fs2.q5_home = 0 then null else fs2.q5_away end
	,fs2.q1_home ,fs2.q2_home ,fs2.q3_home ,fs2.q4_home ,case when fs2.q5_away = 0 and fs2.q5_home = 0 then null else fs2.q5_home end
--	,t.abbr , a.abbr , fs2.shortname, fs2.*
-- SELECT COUNT(*)
from src.foot_schedule fs2 	-- 14940
left join ball.team t on fs2.league_id = t.league_id and fs2.home_team_id = t.source_team_id
left join ball.team a on fs2.league_id = a.league_id and fs2.away_team_id = a.source_team_id
where t.team_id  is not null or a.team_id is not null	-- 14923
order by fs2.game_id desc
;




select *
from ball.game g
where g.home_team_id = 50603 or g.away_team_id = 50603
order by g.game_dt desc;


--NBA_23_24_LAL_###_OKC
select * from ball.sched s ;
select * from ball.season s order by s.reg_end_dt DESC;

--insert into ball.season (season_id,league_id,yr_var,season_concat,reg_start_dt,champ_dt,yr)
--select 202025,2,'2025','NFL_2025','2025-09-04'::date,'2026-02-08'::date,2025
--union
--select 502025,5,'2025','CFB_2025','2025-08-23'::date,'2026-01-19'::date,2025
--;


--SELECT COUNT(*), g.game_id 
--from ball.game g 	-- 14923
--join ball.season s on g.league_id = s.league_id and g.game_dt between s.reg_start_dt - interval '2 month' and s.champ_dt + interval '2 month'
--group by g.game_id 
--having COUNT(*) > 1;


insert into ball.sched (league_id,season_id,team_id,game_num,opp_team_id,sched_concat,game_dt,home, won,team, opp, game_id, is_pre, is_post)
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
	,case when g.game_dt < s.reg_start_dt then true else false end as is_pre
	,case when g.game_dt > s.reg_end_dt then true else false end as is_post
--	,g.*
	-- SELECT COUNT(*)
from ball.game g 	-- 14923
join ball.season s on g.league_id = s.league_id and g.game_dt between s.reg_start_dt - interval '2 month' and s.champ_dt + interval '2 month'
where 1=1
	and g.home_team_id not in (20000,50000)
--	and s.season_id is null 
	and g.game_dt > '2011-07-05'	-- 14591
--order by g.game_dt 
	
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
	,case when g.game_dt < s.reg_start_dt then true else false end as is_pre
	,case when g.game_dt > s.reg_end_dt then true else false end as is_post
--	,g.*
	-- SELECT COUNT(*)
from ball.game g 	-- 14923
join ball.season s on g.league_id = s.league_id and g.game_dt between s.reg_start_dt - interval '2 month' and s.champ_dt + interval '2 month'
where 1=1
	and g.away_team_id not in (20000,50000)
--	and s.season_id is null 
	and g.game_dt > '2011-07-05'	-- 14591
	
;-- 27962

select * from ball.sched s ;

--NBA_23_24_LAL_###_OKC

UPDATE ball.sched
set sched_concat = a.sched_concat
	,game_num = a.game_num  
from (
	select s.sched_id
		,s.sched_concat || '_' || t.abbr || '_' || (row_number() OVER(partition by s.season_id, s.team_id, s.is_pre, s.is_post order by s.GAME_DT ))::varchar 
						|| '_' || o.abbr || case when is_post then '_POST' when is_pre then '_PRE' else '' end as SCHED_CONCAT
		,row_number() OVER(partition by s.SEASON_ID, s.team_id, s.is_pre, s.is_post order by s.GAME_DT ) as GAME_NUM
	from ball.sched s
	join ball.team t on s.team_id  = t.team_id 
	join ball.team o on s.opp_team_id  = o.team_id 
--	where s.season_id   = 202024 and s.team_id   = 22503
	) a
where ball.sched.sched_id  = a.sched_id;


