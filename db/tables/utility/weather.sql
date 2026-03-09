CREATE TABLE IF NOT EXISTS utility.weather (
  weather_id    smallint PRIMARY KEY,
  category      VARCHAR(100),
  description   VARCHAR(100),
  icon          VARCHAR(100)
);