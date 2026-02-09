CREATE TABLE IF NOT EXISTS ball.team (
  team_id int primary KEY,		-- Manually created values since starting with LEAGUE_ID x 10000, grouped by conf/div
  league_id smallint NOT NULL,
  abbr varchar(10) NOT NULL,
  team_concat varchar(20), --NBA_LAL
  name varchar(50),
  location varchar(50),		-- "CITY, ST" right 2 state used for Weather Averages Historically
  conf varchar(20),		-- 1000 increment	-- 2 per League
  div varchar(20),		-- 100	Increment
  lat decimal(8,6),
  lon decimal(9,6),
  weather smallint	-- 0 = Dome
);