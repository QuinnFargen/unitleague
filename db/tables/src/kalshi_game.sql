CREATE TABLE IF NOT EXISTS src.kalshi_game (
	kalshi_hash varchar(32) NOT NULL,
	event_ticker varchar NULL,
    league_id int2 NULL,
	game_dt date NULL,
	pull_dt date NULL,
	home_team varchar NULL,
	away_team varchar NULL,
	home_yes float4 NULL,
	home_volume float4 NULL,
	home_liquidity float4 NULL,
	away_yes float4 NULL,
	away_volume float4 NULL,
	away_liquidity float4 NULL,
	insert_ts timestamp DEFAULT now() NOT NULL
);