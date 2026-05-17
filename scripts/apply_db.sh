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

PSQL="psql \
  --host=$PGHOST \
  --port=$PGPORT \
  --username=$PGUSER \
  --dbname=$PGDATABASE \
  --set=ON_ERROR_STOP=on"




# --------------------------------------
# 1. Apply migrations (versioned, once)
# --------------------------------------
# echo "Applying migrations..."

# for file in db/migrations/*.sql; do
#   echo "Running migration: $file"
#   $PSQL -f "$file"
# done

# --------------------------------------
# 2. Apply base DDL (idempotent)
# --------------------------------------
echo "Applying schemas..."
# $PSQL -f db/ddl/extensions.sql

for file in db/schemas/*.sql; do
  echo "Applying schema: $file"
  $PSQL -f "$file"
done

echo "Applying tables..."
for dir in db/tables/*; do
  for file in "$dir"/*.sql; do
    echo "Applying table: $file"
    $PSQL -f "$file"
  done
done

echo "Applying views..."
for dir in db/views/*; do
  for file in "$dir"/*.sql; do
    echo "Applying view: $file"
    $PSQL -f "$file"
  done
done

# echo "Applying indexes..."
# for file in db/ddl/indexes/*.sql; do
#   echo "Applying index: $file"
#   $PSQL -f "$file"
# done

# # --------------------------------------
# # 3. Apply functions and procedures
# # --------------------------------------
echo "Applying functions..."
for dir in db/functions/*; do
  for file in "$dir"/*.sql; do
    echo "Applying function: $file"
    $PSQL -f "$file"
  done
done


echo "DDL application completed successfully."