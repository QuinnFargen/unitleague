CREATE OR REPLACE VIEW src.v_the_odds_api
as
with cte_odds as (
	select distinct
		md5(id || bookmakers_key || markets_key || "name" || (markets_last_update::timestamp)::varchar ) as odd_hash
		,b.id
		,b.bookmakers_key
		,b.markets_key
		,b."name" as market_name
		,b.markets_last_update::timestamp as markets_last_update_ts
		,b.sport_key 
		,b.sport_title 
		,b.commence_time::timestamp as commence_ts
		,b.home_team 
		,b.away_team 
		,b.bookmakers_title 
		,b.price::float as price
		,b.point::float as point
	from etl.all_the_odds_api_bet b
	where b.imported = false
)
select  
	e.odd_hash
	,e.id
	,e.bookmakers_key
	,e.markets_key
	,e.market_name
	,e.markets_last_update_ts
	,e.sport_key
	,e.sport_title
	,e.commence_ts
	,e.home_team
	,e.away_team
	,e.bookmakers_title
	,e.price
	,e.point		
from cte_odds e
left join src.the_odds_api s on e.odd_hash = s.odd_hash
where s.odd_hash is null
;