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


-- -----------------------------------------------
-- odd.bettor / odd.syndicate / odd.runner
-- -----------------------------------------------
TRUNCATE TABLE odd.runner;
TRUNCATE TABLE odd.syndicate CASCADE;
TRUNCATE TABLE odd.bettor RESTART IDENTITY;

\copy odd.bettor(bettor_id, apple_sub, apple_email, apple_name, profile_name, symbol, color) FROM '${SEED_DIR}/odd.bettor.csv' DELIMITER ',' CSV HEADER;
\copy odd.syndicate(syndicate_id,name,description,code,is_public,password,max_runner,created_by_bettor_id,symbol,color,start_units,is_started) FROM '${SEED_DIR}/odd.syndicate.csv' DELIMITER ',' CSV HEADER;
\copy odd.runner(runner_id, bettor_id, syndicate_id, role, profile_name, symbol, color, active) FROM '${SEED_DIR}/odd.runner.csv' DELIMITER ',' CSV HEADER;

SELECT setval(pg_get_serial_sequence('odd.bettor',    'bettor_id'),    max(bettor_id))    FROM odd.bettor;
SELECT setval(pg_get_serial_sequence('odd.syndicate', 'syndicate_id'), max(syndicate_id)) FROM odd.syndicate;
SELECT setval(pg_get_serial_sequence('odd.runner',    'runner_id'),    max(runner_id))    FROM odd.runner;

-- -----------------------------------------------
-- odd.txn / odd.parlay / odd.leg
-- -----------------------------------------------
TRUNCATE TABLE odd.leg;
TRUNCATE TABLE odd.parlay RESTART IDENTITY;
TRUNCATE TABLE odd.txn RESTART IDENTITY;

-- Unit balance grants (txn_type_id=3, one per bettor)
INSERT INTO odd.txn (bettor_id, syndicate_id, unit, price, txn_type_id, description)
VALUES
  (1, 0, 100, 1.0, 3, 'initial units'),
  (2, 0, 100, 1.0, 3, 'initial units'),
  (3, 0, 100, 1.0, 3, 'initial units'),
  (4, 0, 100, 1.0, 3, 'initial units'),
  (5, 0, 100, 1.0, 3, 'initial units');

-- Bet-based txns — only if odd.bet has been materialized by dbt
DO \$\$
BEGIN
  IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'odd' AND tablename = 'bet') THEN

    -- Past-week straight bets: bettor 1 in syndicate 1 (ML)
    INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, game_dt, txn_type_id)
    SELECT 1, 1, b.bet_hash, 1.0, b.price, g.game_dt, 1
    FROM odd.bet b
    join ball.game g on b.game_id = g.game_id 
    WHERE g.game_dt BETWEEN current_date - 7 AND current_date - 1
      AND b.bet_type = 'ML'
      AND b.active = true
    ORDER BY g.game_dt DESC
    LIMIT 3;

    -- Past-week straight bets: bettor 2 in syndicate 1 (SPR)
    INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, game_dt, txn_type_id)
    SELECT 2, 1, b.bet_hash, 0.5, b.price, g.game_dt, 1
    FROM odd.bet b
    join ball.game g on b.game_id = g.game_id 
    WHERE g.game_dt BETWEEN current_date - 7 AND current_date - 1
      AND b.bet_type = 'SPR'
      AND b.active = true
    ORDER BY g.game_dt DESC
    LIMIT 2;

    -- Future straight bets: bettor 1 in syndicate 1 (ML)
    INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, game_dt, txn_type_id)
    SELECT 1, 1, b.bet_hash, 2.0, b.price, g.game_dt, 1
    FROM odd.bet b
    join ball.game g on b.game_id = g.game_id 
    WHERE g.game_dt >= current_date
      AND b.bet_type = 'ML'
      AND b.active = true
    ORDER BY g.game_dt
    LIMIT 3;

    -- Future straight bets: bettor 4 in syndicate 2 (ML)
    INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, game_dt, txn_type_id)
    SELECT 4, 2, b.bet_hash, 1.0, b.price, g.game_dt, 1
    FROM odd.bet b
    join ball.game g on b.game_id = g.game_id 
    WHERE g.game_dt >= current_date
      AND b.bet_type = 'ML'
      AND b.active = true
    ORDER BY g.game_dt
    LIMIT 2;

    -- Parlay: bettor 1, syndicate 1 — 2 future ML legs
    WITH legs AS (
      SELECT b.bet_hash, b.price
      FROM odd.bet b
      join ball.game g on b.game_id = g.game_id 
      WHERE g.game_dt >= current_date
        AND b.bet_type = 'ML'
        AND b.active = true
      ORDER BY g.game_dt
      LIMIT 2
    ),
    inserted_parlay AS (
      INSERT INTO odd.parlay (price_mult)
      SELECT round(exp(sum(ln(price)))::numeric, 4) FROM legs
      RETURNING parlay_id, price_mult
    )
    INSERT INTO odd.leg (parlay_id, bet_hash, price)
    SELECT ip.parlay_id, l.bet_hash, l.price
    FROM legs l CROSS JOIN inserted_parlay ip;

    INSERT INTO odd.txn (bettor_id, syndicate_id, parlay_id, unit, price, game_dt, txn_type_id)
    SELECT 1, 1, p.parlay_id, 0.5, p.price_mult, current_date + 1, 2
    FROM odd.parlay p
    ORDER BY p.parlay_id DESC
    LIMIT 1;

  ELSE
    RAISE NOTICE 'odd.bet not found — skipping bet-based txns. Run dbt first to materialize odd.bet.';
  END IF;
END
\$\$;

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


TRUNCATE TABLE odd.enhancement;
\copy odd.enhancement(enhancement_type,name,description,bet_type,config,active) FROM '${SEED_DIR}/odd.enhancement.csv' DELIMITER ',' CSV HEADER;


TRUNCATE TABLE utility.weather;
\copy utility.weather FROM '${SEED_DIR}/utility.weather.csv' DELIMITER ',' CSV HEADER;
TRUNCATE TABLE utility.job;
\copy utility.job(job_name,source_schema,source_table,target_schema,target_table,transform_view,load_type,unique_keys,schedule,is_active) FROM '${SEED_DIR}/utility.job.csv' DELIMITER ',' CSV HEADER;


EOF

echo "All seeding completed."