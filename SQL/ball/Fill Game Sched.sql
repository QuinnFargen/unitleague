
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








