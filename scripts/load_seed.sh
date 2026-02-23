#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------
# Load .env file if present
# --------------------------------------
if [ -f ".env" ]; then
  echo "Loading environment variables from .env"
  set -a
  source .env
  set +a
fi

# Validate required variables
: "${PGHOST:?PGHOST not set}"
: "${PGPORT:?PGPORT not set}"
: "${PGDATABASE:?PGDATABASE not set}"
: "${PGUSER:?PGUSER not set}"
: "${PGPASSWORD:?PGPASSWORD not set}"
: "${PGSSLMODE:?PGSSLMODE not set}"
: "${PGCHANNELBINDING:?PGCHANNELBINDING not set}"

export PGPASSWORD
export PGSSLMODE
export PGCHANNELBINDING


SEED_DIR="/Users/quinnfargen/Documents/GitHub/unitleague/db/seed"    # folder where CSVs are stored


psql "postgresql://${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}" <<EOF
TRUNCATE TABLE src.foot_espn_box_defensive;
\copy src.foot_espn_box_defensive FROM '${SEED_DIR}/src.foot_espn_box_defensive.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_box_offensive;
\copy src.foot_espn_box_offensive FROM '${SEED_DIR}/src.foot_espn_box_offensive.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_box_special;
\copy src.foot_espn_box_special FROM '${SEED_DIR}/src.foot_espn_box_special.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_game_team_summary;
\copy src.foot_espn_game_team_summary FROM '${SEED_DIR}/src.foot_espn_game_team_summary.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_schedule;
\copy src.foot_espn_schedule FROM '${SEED_DIR}/src.foot_espn_schedule.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE src.basket_espn_schedule;
\copy src.basket_espn_schedule FROM '${SEED_DIR}/src.foot_espn_schedule.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE src.the_odds_api;
\copy src.the_odds_api FROM '${SEED_DIR}/src.foot_espn_schedule.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE ball.league;
\copy ball.league FROM '${SEED_DIR}/ball.league.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.season;
\copy ball.season FROM '${SEED_DIR}/ball.season.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.meta;
\copy ball.meta(league_id,meta_type,meta_keyid,meta_value,meta_source) FROM '${SEED_DIR}/ball.meta.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.team;
\copy ball.team FROM '${SEED_DIR}/ball.team.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.game;
TRUNCATE TABLE ball.sched;
call utility.seed_ball_game_sched();

EOF

echo "All seeding completed."