CREATE SCHEMA "ODD";

-- Odds API

CREATE TABLE "ODD"."LINE" (
  "LINE_ID" bigint GENERATED BY DEFAULT AS IDENTITY,
  "GAME_ID" bigint NOT NULL,
  "LINE_TYPE" varchar(15) NOT NULL,		-- 'h2h, spread, total'
  "LINE_CONCAT" varchar(100), --NBA_LAL_OKC_20241015_H2H
  "BOOKMAKER" varchar(30),		-- DraftKings
  "TEAM_ID" smallint,
  "PRICE" float,
  "POINT" float,
  "ACTIVE" bit,
  "START_DT" timestamp not NULL,
  "END_DT" timestamp,
  "INSERT_DT" timestamp not null default now()
);

-- Different table for Props in future??

create table "ODD"."BETTOR"(
  "BETTOR_ID" bigint GENERATED BY DEFAULT AS identity,
  "NAME" varchar(100),
  "X_url" varchar(200),
  "INSERT_DT" timestamp not null default now()
);

create table "ODD"."LEAGUE"(
  "LEAGUE_ID" bigint GENERATED BY DEFAULT AS identity,
  "NAME" varchar(100),
  "DESC" varchar(500),
  "FANTASY" bit default ('0'),		-- Fantasy are made up leagues of online analysts
  "INSERT_DT" timestamp not null default now()
);

create table "ODD"."BBL"(	-- Bridge_Bettor_League
  "BBL_ID" bigint GENERATED BY DEFAULT AS identity,
  "BETTOR_ID" bigint not NULL,
  "LEAGUE_ID" bigint not NULL,
  "ACTIVE" bit default ('1'),
  "START_DT" timestamp,
  "END_DT" timestamp,
  "INSERT_DT" timestamp not null default now()
);

CREATE TABLE "ODD"."TXN" (
  "TXN_ID" bigint GENERATED BY DEFAULT AS IDENTITY,
  "INSERT_DT" timestamp not null default now(),
  "BETTOR_ID" bigint not NULL,
  "LINE_ID" bigint NOT NULL,
  "UNIT" float,
  "PRICE" float,
  "WON" bit default ('0'),
  "WON_TS" timestamp,
  "CANCELED" bit default ('0'),
  "CANCEL_TS" timestamp
);

