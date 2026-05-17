#!/usr/bin/env bash
set -euo pipefail

# --------------------------------------
# Parse --env flag
# --------------------------------------
ENV_TARGET=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env) ENV_TARGET="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

: "${ENV_TARGET:?Usage: $0 --env <local|neon>}"
ENV_FILE=".env.${ENV_TARGET}"
[ -f "$ENV_FILE" ] || { echo "Env file not found: $ENV_FILE"; exit 1; }

echo "Loading environment from $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

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


SEED_DIR="$(git rev-parse --show-toplevel)/db/seed"


psql "postgresql://${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE}" <<EOF

TRUNCATE TABLE src.espn_schedule;
\copy src.espn_schedule FROM '${SEED_DIR}/src.espn_schedule.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.espn_week;
\copy src.espn_week FROM '${SEED_DIR}/src.espn_week.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE src.foot_espn_box_defensive;
\copy src.foot_espn_box_defensive FROM '${SEED_DIR}/src.foot_espn_box_defensive.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_box_offensive;
\copy src.foot_espn_box_offensive FROM '${SEED_DIR}/src.foot_espn_box_offensive.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_box_special;
\copy src.foot_espn_box_special FROM '${SEED_DIR}/src.foot_espn_box_special.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE src.foot_espn_game_team_summary;
\copy src.foot_espn_game_team_summary FROM '${SEED_DIR}/src.foot_espn_game_team_summary.csv' DELIMITER ',' CSV HEADER;



TRUNCATE TABLE src.the_odds_api;
\copy src.the_odds_api FROM '${SEED_DIR}/src.the_odds_api.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE utility.weather;
\copy utility.weather FROM '${SEED_DIR}/utility.weather.csv' DELIMITER ',' CSV HEADER;
TRUNCATE TABLE utility.job;
\copy utility.job FROM '${SEED_DIR}/utility.job.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE ball.league;
\copy ball.league FROM '${SEED_DIR}/ball.league.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.season;
\copy ball.season FROM '${SEED_DIR}/ball.season.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.meta;
\copy ball.meta(league_id,meta_type,meta_key,meta_keyid,meta_value,meta_source) FROM '${SEED_DIR}/ball.meta.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE ball.team;
\copy ball.team FROM '${SEED_DIR}/ball.team.csv' DELIMITER ',' CSV HEADER;

# all 3 truncates in procedure:
  # TRUNCATE TABLE ball.game;
  # TRUNCATE TABLE ball.sched;
  # truncate table ball.week;
call utility.seed_ball_game_sched();

EOF

echo "All seeding completed."