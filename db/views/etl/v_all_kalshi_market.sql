CREATE OR REPLACE VIEW etl.v_all_kalshi_market
AS 
	SELECT 
		a.event_ticker,
		a.ticker,
		a.expected_expiration_time,
		a.title,
		a.yes_sub_title,
		a.volume,
		a.volume_24h,
		a.yes_ask,
		a.yes_bid,
		a.liquidity,
		a.open_interest,
		a.rules_primary,
		a.status
		-- SELECT *
	from etl.stg_kalshi_market a;