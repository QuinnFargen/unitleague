
CREATE TABLE IF NOT EXISTS ball.league (
  league_id smallint primary key,
  abbr varchar(3),
  name varchar(50),
  yr_orig smallint,
  yr_data smallint,
  sport varchar(20),
  weather bit		-- 1 outside, 0 indoor stadium
);

insert into ball.league (league_id, abbr, name, yr_orig, sport, weather)
	  select 1, 'NBA', 'National Basketball Association', 1949, 'BASKET', '0'::bit
union select 2, 'NFL', 'National Football League', 1920, 'FOOT', '1'::bit
union select 3, 'NHL', 'National Hockey League', 1917, 'PUCK', '0'::bit
union select 4, 'MLB', 'Major League Baseball', 1869, 'BASE', '1'::bit
union select 5, 'CFB', 'College Football', 1869, 'FOOT', '1'::bit
union select 6, 'CBB', 'College Basketball', 1939, 'BASKET', '0'::bit;