-- One-time migration for the syndicate tab ordering / single-league PR.
-- Fresh databases already get these columns from db/tables/odd/{runner,syndicate}.sql.
--
-- Run order on an existing database:
--   1. dbt run -s mart_league        (adds mart.league.current_season_id, used for yr below)
--   2. psql -f scripts/pr_syndicate_tab_ordering.sql
--   3. dbt run                       (odd_enhance_options now reads odd.syndicate.league_id)

BEGIN;

-- odd.runner.sort_order: position in the bettor's Syndicate tab; NULL = hidden.
-- Backfill only when the column is new so a re-run never un-hides rows.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'odd' AND table_name = 'runner' AND column_name = 'sort_order'
  ) THEN
    ALTER TABLE odd.runner ADD COLUMN sort_order int;

    UPDATE odd.runner r
    SET sort_order = o.rn
    FROM (
      SELECT runner_id, row_number() over (partition by bettor_id order by syndicate_id) AS rn
      FROM odd.runner
      WHERE active = true
    ) o
    WHERE r.runner_id = o.runner_id;
  END IF;
END $$;

-- odd.syndicate: multi-league (league_ids int[] / config.league_ids) -> single league_id + season yr.
ALTER TABLE odd.syndicate ADD COLUMN IF NOT EXISTS syndicate_type text;
ALTER TABLE odd.syndicate ADD COLUMN IF NOT EXISTS league_id smallint;
ALTER TABLE odd.syndicate ADD COLUMN IF NOT EXISTS yr smallint;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'odd' AND table_name = 'syndicate' AND column_name = 'league_ids'
  ) THEN
    UPDATE odd.syndicate
    SET league_id = coalesce(league_ids[1], (config -> 'league_ids' ->> 0)::int)
    WHERE league_id IS NULL;

    ALTER TABLE odd.syndicate DROP COLUMN league_ids;
  END IF;
END $$;

UPDATE odd.syndicate
SET config = (config::jsonb - 'league_ids')::json
WHERE config IS NOT NULL AND config::jsonb ? 'league_ids';

UPDATE odd.syndicate s
SET yr = se.yr
FROM mart.league l
JOIN ball.season se ON se.season_id = l.current_season_id
WHERE l.league_id = s.league_id
  AND s.yr IS NULL;

COMMIT;
