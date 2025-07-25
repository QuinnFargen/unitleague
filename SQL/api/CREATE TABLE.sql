create schema "api";	-- Kestra demanded it be lowercase...

CREATE TABLE "api"."stg_the_odds_api" (
  "id" varchar,
  "sport_key" varchar,
  "sport_title" varchar,
  "commence_time" varchar,
  "home_team" varchar,
  "away_team" varchar,   
  "full_json" json
);


CREATE TABLE "api"."the_odds_api" (
  "THE_ODDs_API_ID" bigint GENERATED BY DEFAULT AS IDENTITY,
  "id" varchar,
  "sport_key" varchar,
  "sport_title" varchar,
  "commence_time" varchar,
  "home_team" varchar,
  "away_team" varchar,   
  "full_json" json,
  "IMPORTED" bit default ('0'),
  "INSERT_DT" timestamp not null default now()
);

CREATE TABLE "api"."the_odds_api_bet" (
  "THE_ODDs_API_BET_ID" bigint GENERATED BY DEFAULT AS IDENTITY,
  "THE_ODDs_API_ID" bigint,
  "id" varchar(100),
  "bookmakers_key" varchar,
  "bookmakers_title" varchar,
  "markets_key" varchar,
  "markets_last_update" varchar,
  "name" varchar(100),  
  "price" varchar(100),  
  "point" varchar(100),  
  "IMPORTED" bit default ('0'),
  "INSERT_DT" timestamp not null default now()
);


-- Weather
-- Actual, Forecast Flag??
-- start/end dates when

-- Could do morning forecasts(Daily's) and again at time of games in evening without forecasts


CREATE TABLE "api"."open_weather" (
  "OPENWEATHER_API_ID" bigint GENERATED BY DEFAULT AS IDENTITY,
  "INSERT_DT" timestamp not null default now(),
  "LAT" decimal(8,6) not NULL,
  "LON" decimal(9,6) not NULL,
  "CURRENT_EPOCH" bigint not NULL,
  "CURRENT_DT" date not null, -- to_timestamp("CURRENT_EPOCH")::date
  "CURRENT_JSON" json,
  "DAILY_JSON" json,
  "IMPORTED" bit default ('0')
);

CREATE TABLE "api"."stg_open_weather" (
  "LAT" decimal(8,6) not NULL,
  "LON" decimal(9,6) not NULL,
  "CURRENT_EPOCH" bigint not NULL,
  "CURRENT_JSON" json,
  "DAILY_JSON" json
);


--SELECT extract(epoch FROM now());				-- 1737640390.593964
--SELECT to_timestamp(1737640390.593964);			-- 2025-01-23 07:53:10.593 -0600
--SELECT to_timestamp(1737640390);				-- 2025-01-23 07:53:10.000 -0600
--SELECT to_timestamp(1737640390.593964)::date;	-- 2025-01-23
----
----select -123.456789::decimal(6,3);
--
--select CURRENT_DATE - CURRENT_DATE + 2;
--select CURRENT_DATE + 2;





---------------------------------------------
-- MLB


CREATE TABLE "api"."stg_mlb_sched" (
	"GAME_ID" int4,
	"GAME_DT" date NOT NULL,
	"GAME_TIME" timestamp null,
	"HOME_TEAM" varchar NOT NULL,
	"AWAY_TEAM" varchar NOT NULL
);

CREATE TABLE "api"."stg_mlb_sched_unknown" (
	"GAME_ID" int4,
	"GAME_DT" date NOT NULL,
	"GAME_TIME" timestamp null,
	"HOME_TEAM" varchar NOT NULL,
	"AWAY_TEAM" varchar NOT null,
	"REASON" varchar,
  	"INSERT_DT" timestamp not null default now()
);



-- DROP TABLE api.mlb_api_batting;

CREATE TABLE api.mlb_api_batting (
	gamepk varchar(20) NOT NULL,
	gamedate date NOT NULL,
	team_id int4 NOT NULL,
	personid int4 NOT NULL,
	"name" text NOT NULL,
	"position" text NULL,
	battingorder text NULL,
	ab int4 NULL,
	r int4 NULL,
	h int4 NULL,
	doubles int4 NULL,
	triples int4 NULL,
	hr int4 NULL,
	rbi int4 NULL,
	sb int4 NULL,
	bb int4 NULL,
	k int4 NULL,
	lob int4 NULL,
	avg numeric(4, 3) NULL,
	obp numeric(4, 3) NULL,
	slg numeric(4, 3) NULL,
	substitution text NULL,
	note text NULL,
	note_description text NULL,
	CONSTRAINT mlb_api_batting_pkey PRIMARY KEY (gamepk, gamedate, team_id, personid)
);


-- DROP TABLE api.mlb_api_meta;

CREATE TABLE api.mlb_api_meta (
	gamepk varchar(20) NOT NULL,
	gamedate date NOT NULL,
	umpires text NULL,
	weather text NULL,
	wind text NULL,
	CONSTRAINT mlb_api_meta_pkey PRIMARY KEY (gamepk, gamedate)
);


-- DROP TABLE api.mlb_api_pitching;

CREATE TABLE api.mlb_api_pitching (
	gamepk varchar(20) NOT NULL,
	gamedate date NOT NULL,
	team_id int4 NOT NULL,
	personid int4 NOT NULL,
	"name" text NOT NULL,
	ip numeric(4, 1) NULL,
	h int4 NULL,
	r int4 NULL,
	er int4 NULL,
	bb int4 NULL,
	k int4 NULL,
	hr int4 NULL,
	p int4 NULL,
	s int4 NULL,
	era numeric(5, 2) NULL,
	note text NULL,
	CONSTRAINT mlb_api_pitching_pkey PRIMARY KEY (gamepk, gamedate, team_id, personid)
);



---------------------------------------------
-- NBA


-- DROP TABLE api.nba_api_traditional_box_score;

CREATE TABLE api.nba_api_traditional_box_score (
	game_id varchar(15) NOT NULL,
	team_id int4 NOT NULL,
	team_abbreviation text NOT NULL,
	team_city text NOT NULL,
	player_id int4 NOT NULL,
	player_name text NOT NULL,
	nickname text NULL,
	start_position bpchar(2) NULL,
	"comment" text NULL,
	minutes_played varchar(25) NULL,
	field_goals_made numeric(4, 1) NULL,
	field_goals_attempted numeric(4, 1) NULL,
	field_goal_pct numeric(4, 3) NULL,
	three_pointers_made numeric(4, 1) NULL,
	three_pointers_attempted numeric(4, 1) NULL,
	three_point_pct numeric(4, 3) NULL,
	free_throws_made numeric(4, 1) NULL,
	free_throws_attempted numeric(4, 1) NULL,
	free_throw_pct numeric(4, 3) NULL,
	offensive_rebounds numeric(4, 1) NULL,
	defensive_rebounds numeric(4, 1) NULL,
	total_rebounds numeric(4, 1) NULL,
	assists numeric(4, 1) NULL,
	steals numeric(4, 1) NULL,
	blocks numeric(4, 1) NULL,
	turnovers numeric(4, 1) NULL,
	personal_fouls numeric(4, 1) NULL,
	points numeric(4, 1) NULL,
	plus_minus numeric(4, 1) NULL,
	CONSTRAINT nba_api_traditional_box_score_pkey PRIMARY KEY (game_id, player_id)
);




---------------------------------------------
-- NHL


-- DROP TABLE api.nhl_api_box_score;

CREATE TABLE api.nhl_api_box_score (
	gameid int4 NOT NULL,
	"date" date NOT NULL,
	team varchar(10) NOT NULL,
	playerid int4 NOT NULL,
	playername varchar(100) NOT NULL,
	"position" varchar(10) NOT NULL,
	goals int4 NULL,
	assists int4 NULL,
	points int4 NULL,
	plusminus int4 NULL,
	pim int4 NULL,
	hits int4 NULL,
	sog int4 NULL,
	blockedshots int4 NULL,
	giveaways int4 NULL,
	takeaways int4 NULL,
	timeonice interval NULL,
	shifts int4 NULL,
	faceoffwinningpctg numeric(5, 2) NULL,
	CONSTRAINT nhl_api_box_score_pkey PRIMARY KEY (gameid, playerid)
);


-- DROP TABLE api.nhl_api_goalie_box_score;

CREATE TABLE api.nhl_api_goalie_box_score (
	gameid int4 NOT NULL,
	"date" date NOT NULL,
	team varchar(10) NOT NULL,
	playerid int4 NOT NULL,
	playername varchar(100) NOT NULL,
	"position" varchar(10) NOT NULL,
	evenstrengthshotsagainst varchar(10) NULL,
	powerplayshotsagainst varchar(10) NULL,
	shorthandedshotsagainst varchar(10) NULL,
	saveshotsagainst varchar(10) NULL,
	savepctg numeric(5, 3) NULL,
	evenstrengthgoalsagainst int4 NULL,
	powerplaygoalsagainst int4 NULL,
	shorthandedgoalsagainst int4 NULL,
	pim int4 NULL,
	goalsagainst int4 NULL,
	timeonice varchar(10) NULL,
	shotsagainst int4 NULL,
	saves int4 NULL,
	CONSTRAINT nhl_api_goalie_box_score_pkey PRIMARY KEY (gameid, playerid)
);
