CREATE TABLE IF NOT EXISTS ball.team_name (
  league_id smallint,
  team_id int,
  name varchar(100),
  source varchar(100),
  insert_ts timestamp not null default now()
);