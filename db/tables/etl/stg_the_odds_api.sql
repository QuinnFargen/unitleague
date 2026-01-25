CREATE TABLE IF NOT EXISTS api.stg_the_odds_api (
  id varchar,
  sport_key varchar,
  sport_title varchar,
  commence_time varchar,
  home_team varchar,
  away_team varchar,   
  full_json json
);