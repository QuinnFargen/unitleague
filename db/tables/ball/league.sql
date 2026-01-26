
CREATE TABLE IF NOT EXISTS ball.league (
  league_id smallint primary key,
  abbr varchar(3),
  name varchar(50),
  yr_orig smallint,
  yr_data smallint,
  sport varchar(20),
  weather bit		-- 1 outside, 0 indoor stadium
);