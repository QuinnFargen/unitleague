CREATE OR REPLACE PROCEDURE api.sp_stg_nfl_sched_to_game()
 LANGUAGE plpgsql
AS $procedure$
begin


	CREATE TEMP TABLE nfl_sched AS
		select 
			a."EVENT_ID",
			a."GAME_DATETIME",
			a."HOME_TEAM",
			a."AWAY_TEAM",
  			now() as "INSERT_DT_temp"
		from "api"."stg_nfl_sched" a;
		-- 2822


-- Put into unknown for duplicate game_id to avoid putting them into GAME twice
	INSERT INTO "api"."stg_mlb_sched_unknown" ("GAME_ID", "GAME_DATETIME", "HOME_TEAM", "AWAY_TEAM", "REASON")
		SELECT 
			a."EVENT_ID",
			a."GAME_DATETIME",
			a."HOME_TEAM",
			a."AWAY_TEAM",
			'DUPLICATE GAME_ID' as "REASON"
		FROM nfl_sched a
		JOIN (SELECT d."EVENT_ID" FROM nfl_sched d GROUP BY d."EVENT_ID" HAVING COUNT(*) > 1) b on a."EVENT_ID" = b."EVENT_ID";

	DELETE
--	SELECT COUNT(*)
	FROM nfl_sched b
	WHERE b."EVENT_ID" IN (SELECT "EVENT_ID" from "api"."stg_nfl_sched_unknown" c WHERE c."REASON" = 'DUPLICATE EVENT_ID');


-- Get TEAM_ID, LEAGUE_ID	
	CREATE TEMP TABLE nfl_sched_teams AS
		select 
			a."EVENT_ID",
			a."GAME_DATETIME",
			a."HOME_TEAM",
			a."AWAY_TEAM",
  			a."INSERT_DT_temp",
			b."TEAM_ID" as "HOME_TEAM_ID",
			c."TEAM_ID" as "AWAY_TEAM_ID",
			b."LEAGUE_ID"
		from nfl_sched a
		LEFT JOIN "BALL"."TEAM_NAME" b on a."HOME_TEAM" = b."NAME" and b."LEAGUE_ID" = 2
		LEFT JOIN "BALL"."TEAM_NAME" c on a."AWAY_TEAM" = c."NAME" and c."LEAGUE_ID" = 2
		;

-- Put into unknown team rows if haven't saw before
	INSERT INTO "api"."stg_mlb_sched_unknown" ("GAME_ID", "GAME_DT", "GAME_TIME", "HOME_TEAM", "AWAY_TEAM", "REASON")
		SELECT 
			a."GAME_ID",
			a."GAME_DT",
			a."GAME_TIME",
			a."HOME_TEAM",
			a."AWAY_TEAM",
			'UNKNOWN TEAM' as "REASON"
		FROM mlb_sched_teams a
		LEFT JOIN "api"."stg_mlb_sched_unknown" b on a."GAME_ID" = b."GAME_ID"
		WHERE 1=1
			AND (a."HOME_TEAM_ID" IS NULL OR a."AWAY_TEAM_ID" IS NULL)	-- No TEAM ID
			AND b."GAME_ID" IS NULL;									-- Not already in table

-- Put into GAME if not found before


	CREATE TEMP TABLE mlb_sched_games AS
		SELECT 
			b."LEAGUE_ID",
			a."HOME_TEAM_ID",
			a."AWAY_TEAM_ID",
			a."GAME_DT",
			a."GAME_TIME",
			ROW_NUMBER() OVER( PARTITION BY (a."HOME_TEAM_ID",a."AWAY_TEAM_ID",a."GAME_DT") ORDER BY (a."GAME_TIME") ) as "DOUBLE_HEADER",
			l."ABBR" || '_' || b."ABBR" || '_' || c."ABBR"  || '_' || to_char("GAME_DT", 'YYYYMMDD') AS "GAME_CONCAT",
			a."GAME_ID" as "SOURCE_GAME_ID"
		FROM mlb_sched_teams a
		JOIN "BALL"."TEAM" b on a."HOME_TEAM_ID" = b."TEAM_ID" 
		JOIN "BALL"."TEAM" c on a."AWAY_TEAM_ID" = c."TEAM_ID"
		JOIN "BALL"."LEAGUE" l on b."LEAGUE_ID" = l."LEAGUE_ID";


	INSERT INTO "BALL"."GAME" ("LEAGUE_ID", "HOME_TEAM_ID", "AWAY_TEAM_ID", "GAME_DT", "GAME_TIME", "GAME_CONCAT", "SOURCE_GAME_ID")
		SELECT 
			a."LEAGUE_ID", a."HOME_TEAM_ID", a."AWAY_TEAM_ID", a."GAME_DT", a."GAME_TIME"::TIME, 
			CASE WHEN b."IS_DOUBLE_HEADER" = 1 THEN a."GAME_CONCAT"	ELSE a."GAME_CONCAT" || '_' || a."DOUBLE_HEADER"::varchar END as "GAME_CONCAT", 
			a."SOURCE_GAME_ID"
		FROM mlb_sched_games A
		JOIN (
				SELECT a."HOME_TEAM_ID", a."AWAY_TEAM_ID", a."GAME_DT", MAX(a."DOUBLE_HEADER") as "IS_DOUBLE_HEADER"
				-- SELECT *
				FROM mlb_sched_games a
				GROUP BY a."HOME_TEAM_ID", a."AWAY_TEAM_ID", a."GAME_DT"
			) b on a."HOME_TEAM_ID" = b. "HOME_TEAM_ID" AND  a."AWAY_TEAM_ID" = b."AWAY_TEAM_ID" AND a."GAME_DT" = b."GAME_DT"
		LEFT JOIN "BALL"."GAME" g on a."SOURCE_GAME_ID" = g."SOURCE_GAME_ID"
		WHERE g."GAME_ID" is null
	;

	-- Logic for game changes/delays?

	INSERT INTO "api"."stg_mlb_sched_unknown" ("GAME_ID", "GAME_DT", "GAME_TIME", "HOME_TEAM", "AWAY_TEAM", "REASON")
		SELECT 
			a."GAME_ID",
			a."GAME_DT",
			a."GAME_TIME",
			a."HOME_TEAM",
			a."AWAY_TEAM",
			'GAME DATE OR TIME CHANGE' as "REASON"
		FROM mlb_sched_teams a
		JOIN "BALL"."GAME" b on a."GAME_ID" = b."SOURCE_GAME_ID"
		WHERE a."GAME_DT" <> b."GAME_DT"
			OR a."GAME_TIME"::TIME <> b."GAME_TIME"::TIME;



end; $procedure$
;
