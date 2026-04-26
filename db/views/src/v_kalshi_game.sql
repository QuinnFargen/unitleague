CREATE OR REPLACE VIEW src.v_kalshi_game
as
select distinct
	b.kalshi_hash
	,b.event_ticker
	,b.league_id
	,b.game_dt
	,b.pull_dt
	,b.home_team
	,b.away_team 
	,b.home_yes 
	,b.home_volume
	,b.home_liquidity 
	,b.away_yes 
	,b.away_volume 
	,b.away_liquidity
	,b.insert_ts
from etl.all_kalshi_game b
left join src.kalshi_game s on b.kalshi_hash = s.kalshi_hash
where b.imported = false
	and s.kalshi_hash is null
;