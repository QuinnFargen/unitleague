#!/usr/bin/env bash
set -euo pipefail

echo "Starting database DDL application..."

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