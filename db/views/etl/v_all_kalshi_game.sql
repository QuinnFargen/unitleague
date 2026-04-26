CREATE OR REPLACE VIEW etl.v_all_kalshi_game
as
WITH src AS (
	  select distinct
	  	*,
	    -- part2 contains date+middle, e.g. "25NOV02SEAWAS" or "25NOV02NOLA"
	    split_part(ticker, '-', 2) AS part2,
	    split_part(ticker, '-', 3) AS team
	    -- select *
	  FROM etl.all_kalshi_market a
		where a.imported = false
	),
	parsed AS (
	  SELECT
	    *,
	    -- date is the first 7 chars of part2: 25NOV02
	    to_date(substring(part2 FROM 1 FOR 7), 'YYMONDD') AS game_date,
	    -- middle is everything after the 7-char date prefix
	    substring(part2 FROM 8) AS middle
	  FROM src
	),
	imports as (
	SELECT
	  kalshi_market_id,
	  event_ticker,
	  ticker,
	  insert_ts::date as pull_dt,
	  case 	when event_ticker like 'KXNBAGAME%' then 1
	  		when event_ticker like 'KXNFLGAME%' then 2
	  		when event_ticker like 'KXNHLGAME%' then 3
	  		when event_ticker like 'KXMLBGAME%' then 4
	  		when event_ticker like 'KXNCAAFGAME%' then 5
	  		when event_ticker like 'KXNCAAFCSGAME%' then 5
	  		when event_ticker like 'KXNCAABGAME%' then 6
	  		when event_ticker like 'KXNCAAMBGAME%' then 6
	  		else null end as league_id,
	  volume,
	  yes_ask,
	  liquidity,
	  game_date -1 as game_dt,
	  yes_sub_title,
	  team,
	  -- determine opponent by removing the team from middle,
	  -- either as a trailing suffix or a leading prefix
	  CASE
	    WHEN right(middle, char_length(team)) = team
	      THEN left(middle, char_length(middle) - char_length(team))
	    WHEN left(middle, char_length(team)) = team
	      THEN right(middle, char_length(middle) - char_length(team))
	    ELSE
	      -- fallback: NULL (or you could fallback to middle)
	      NULL
	  END AS opp_team
	FROM parsed
	),
	inserts as (
	select 
		h.league_id
		,h.game_dt
		,h.event_ticker
		,h.pull_dt
		,md5(h.league_id::varchar || h.game_dt || h.event_ticker || h.pull_dt || h.volume || a.volume || h.yes_ask || a.yes_ask ) as kalshi_hash
		,h.kalshi_market_id as home_kalshi_market_id
		,a.kalshi_market_id as away_kalshi_market_id
		,h.team as home_team
		,h.yes_ask as home_yes
	    ,h.volume as home_volume 
	    ,h.liquidity as home_liquidity 
		,h.yes_sub_title as home_title
		,a.team as away_team
	    ,a.yes_ask as away_yes 
	    ,a.volume as away_volume 
	    ,a.liquidity as away_liquidity 
		,a.yes_sub_title as away_title
	from imports h
	join imports a on h.event_ticker = a.event_ticker and h.ticker <> a.ticker 
	where 1=1
		and right(h.ticker, (length(h.team) + 1 + length(h.team))) = h.team || '-' || h.team	-- Filter on h to home team
	)
	select
		i.league_id
		,i.game_dt
		,i.event_ticker
		,i.pull_dt
		,i.kalshi_hash
		,i.home_kalshi_market_id
		,i.away_kalshi_market_id
		,i.home_team
		,i.home_yes
		,i.home_volume
		,i.home_liquidity
		,i.home_title
		,i.away_team
		,i.away_yes
		,i.away_volume
		,i.away_liquidity
		,i.away_title
	from inserts i
	left join etl.all_kalshi_game g on i.league_id = g.league_id and i.kalshi_hash = g.kalshi_hash
	where g.kalshi_game_id is null
	;