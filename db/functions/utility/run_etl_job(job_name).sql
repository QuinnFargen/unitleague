CREATE OR REPLACE PROCEDURE utility.run_etl_job(p_job_name TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job              utility.job%ROWTYPE;
    v_view_schema      TEXT;
    v_view_name        TEXT;
    v_load_type        TEXT;
    v_target_loc       TEXT;
    v_insert_cols      TEXT;
    v_select_cols      TEXT;
    v_update_set       TEXT;
    v_join_condition   TEXT;
    v_sql              TEXT;
BEGIN
    /* --------------------------------------------------------
       Fetch job utilitydata
    --------------------------------------------------------- */
    SELECT *
    INTO v_job
    FROM utility.job
    WHERE job_name = p_job_name
      AND is_active = TRUE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'ETL job % not found or inactive', p_job_name;
    END IF;

    /* --------------------------------------------------------
       Parse transform view schema/name
    --------------------------------------------------------- */
    v_view_schema := v_job.target_schema;
    v_view_name   := 'v_' || v_job.target_table;
    v_load_type   := v_job.load_type;
    v_target_loc  := format('%I.%I', v_job.target_schema, v_job.target_table);

    /* --------------------------------------------------------
       Build column lists (intersection of view + target)
    --------------------------------------------------------- */
    SELECT
        string_agg(format('%I', c.column_name), ', ') AS insert_cols,
        string_agg(format('src.%I', c.column_name), ', ') AS select_cols
    INTO v_insert_cols, v_select_cols
    FROM information_schema.columns c
    WHERE c.table_schema = v_view_schema
      AND c.table_name   = v_view_name
      AND c.column_name IN (
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = v_job.target_schema
            AND table_name   = v_job.target_table
      );

    IF v_insert_cols IS NULL THEN
        RAISE EXCEPTION 'No matching columns between % and %',
            v_job.transform_view, v_target_loc;
    END IF;

    /* --------------------------------------------------------
       FULL LOAD (truncate + insert)
    --------------------------------------------------------- */
    IF v_job.load_type = 'full' THEN
        v_sql := format(
            'TRUNCATE TABLE %s;
             INSERT INTO %s (%s)
             SELECT %s FROM %I.%I;',
            v_target_loc,
            v_target_loc,
            v_insert_cols,
            v_select_cols,
            v_view_schema,
            v_view_name
        );

        EXECUTE v_sql;
        RETURN;
    END IF;

    /* --------------------------------------------------------
       APPEND LOAD (insert)
    --------------------------------------------------------- */
    IF v_job.load_type = 'append' THEN
        v_sql := format(
             'INSERT INTO %s (%s)
             SELECT %s FROM %I.%I;',
            v_target_loc,
            v_insert_cols,
            v_select_cols,
            v_view_schema,
            v_view_name
        );

        EXECUTE v_sql;
        RETURN;
    END IF;

    /* --------------------------------------------------------
       MERGE LOAD
    --------------------------------------------------------- */
    IF v_job.load_type = 'merge' THEN
        IF v_job.unique_keys IS NULL THEN
            RAISE EXCEPTION 'MERGE load requires unique_keys';
        END IF;

        /* Build join condition */
        SELECT string_agg(
            format('t.%1$I = src.%1$I', trim(k)),
            ' AND '
        )
        INTO v_join_condition
        FROM regexp_split_to_table(v_job.unique_keys, ',') k;

        /* Build UPDATE SET clause (exclude key columns) */
        SELECT string_agg(
            format('%1$I = src.%1$I', c.column_name),
            ', '
        )
        INTO v_update_set
        FROM information_schema.columns c
        WHERE c.table_schema = v_view_schema
          AND c.table_name   = v_view_name
          AND c.column_name NOT IN (
              SELECT trim(k)
              FROM regexp_split_to_table(v_job.unique_keys, ',') k
          )
          AND c.column_name IN (
              SELECT column_name
              FROM information_schema.columns
              WHERE table_schema = v_job.target_schema
                AND table_name   = v_job.target_table
          );

        v_sql := format(
            'MERGE INTO %s t
             USING %I.%I src
             ON %s
             WHEN MATCHED THEN
                 UPDATE SET %s
             WHEN NOT MATCHED THEN
                 INSERT (%s)
                 VALUES (%s);',
            v_target_loc,
            v_view_schema,
            v_view_name,
            v_join_condition,
            v_update_set,
            v_insert_cols,
            v_select_cols
        );

        EXECUTE v_sql;
        RETURN;
    END IF;

    RAISE EXCEPTION 'Unsupported load_type: %', v_job.load_type;

END;
$$;