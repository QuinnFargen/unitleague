CREATE OR REPLACE PROCEDURE odd.sp_load_api_to_oddbet_daily()
 LANGUAGE plpgsql
AS $procedure$
begin


	DROP TABLE IF EXISTS new_odds;
	CREATE TEMP TABLE new_odds AS
	WITH src AS (
	    SELECT
	        the_odds_api_bet_id AS bet_source_id,
	        a.game_id, 
	        case when b.name = a.home_team then a.home_team_id when b.name = a.away_team then a.away_team_id else null end as team_id,
	--        b.name,  a.home_team , a.home_team_id , a.away_team , a.away_team_id,
	--        a.commence_time as game_dt,        a.sport_key,
	        markets_key AS bet_type,
	        b.bookmakers_key  AS bookmaker,
	        price::float AS price,
	        point::float AS points,
	        b.markets_last_update::timestamp  AS start_dt
	    FROM api.the_odds_api_bet b
	    join api.the_odds_api a on a.the_odds_api_id  = b.the_odds_api_id 
	    where a.imported = true 		-- is imported into api_bet
			and a.game_id is not null	-- is tagged with game_id
	    	and b.imported = false		-- hasn't been imported into odd.bet
	),
	src_hash as (
	
		select 
			s.*
			,md5(bookmaker || s.game_id::text || coalesce(s.team_id::text,'') || bet_type) as bet_hash
			,g.game_concat || '_' || case when t.abbr is not null then t.abbr else s.bet_type end || '_' || s.bookmaker as bet_concat
			,row_number() OVER(partition by s.game_id, s.team_id, s.bet_type, s.bookmaker, s.price, s.points order by s.start_dt ) as unique_bet
		from src s
		join ball.game g on s.game_id = g.game_id 
		left join ball.team t on s.team_id = t.team_id
	
	),
	changed AS (
	    SELECT
	        s.*
	        ,b.bet_id AS existing_bet_id
			,row_number() OVER(partition by s.game_id, s.team_id, s.bet_type, s.bookmaker order by s.start_dt ) as oldest_bet
			,lead(s.start_dt) OVER(partition by s.game_id, s.team_id, s.bet_type, s.bookmaker order by s.start_dt ) as end_dt
	    FROM src_hash s
	    LEFT JOIN odd.bet b ON s.game_id = b.game_id AND b.active = true and s.bet_hash = b.bet_hash
	    where 1=1
	    	and s.unique_bet = 1
	        -- No active record yet OR values have changed
	        and (b.bet_id IS NULL
	        OR b.price <> s.price
	        OR b.points <> s.points)
	)
	select * from changed;

	
	-- 2. Close out old records that changed
	UPDATE odd.bet b
	SET end_dt = c.start_dt,
	    active = false
	FROM new_odds c
	WHERE b.bet_id = c.existing_bet_id
		and c.oldest_bet = 1 -- Update the prior existing active bet with the oldest of the new batch
	  	AND b.active = true;
	
	-- 3. Insert new versions
	INSERT INTO odd.bet (
	    bet_source_id, bet_hash, game_id, bet_type, bookmaker, team_id, bet_concat,
	    price, points, start_dt, end_dt, active
	)
	SELECT
	    bet_source_id, bet_hash, game_id, bet_type, bookmaker, team_id, bet_concat,
	    price, points, start_dt, end_dt, case when end_dt is not null then false else true end
	FROM new_odds;
	

	UPDATE api.the_odds_api_bet a
	SET imported = true;


end; $procedure$
;
