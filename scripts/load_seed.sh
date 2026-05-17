#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root so relative paths (db/*, .env.*) resolve correctly
REPO_ROOT="$(git rev-parse --show-toplevel)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Starting database DDL application..."

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
# ENV_FILE=".env.${ENV_TARGET}"
# [ -f "$ENV_FILE" ] || { echo "Env file not found: $ENV_FILE"; exit 1; }

# Look for .env.<target> next to the script, then at the repo root
if [ -f "$SCRIPT_DIR/.env.${ENV_TARGET}" ]; then
  ENV_FILE="$SCRIPT_DIR/.env.${ENV_TARGET}"
elif [ -f "$REPO_ROOT/.env.${ENV_TARGET}" ]; then
  ENV_FILE="$REPO_ROOT/.env.${ENV_TARGET}"
else
  echo "Env file not found: looked in $SCRIPT_DIR and $REPO_ROOT"; exit 1
fi

# Always run from repo root so relative paths (db/*) resolve correctly
cd "$REPO_ROOT"

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
TRUNCATE TABLE src.kalshi_game;
\copy src.kalshi_game FROM '${SEED_DIR}/src.kalshi_game.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE utility.weather;
\copy utility.weather FROM '${SEED_DIR}/utility.weather.csv' DELIMITER ',' CSV HEADER;
TRUNCATE TABLE utility.job;
\copy utility.job(job_name,source_schema,source_table,target_schema,target_table,transform_view,load_type,unique_keys,schedule,is_active) FROM '${SEED_DIR}/utility.job.csv' DELIMITER ',' CSV HEADER;


EOF

echo "All seeding completed."