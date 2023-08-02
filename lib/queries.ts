export const QUERIES = {
  enableLogs: [
    `LOAD 'auto_explain';`,
    `SET session_preload_libraries = auto_explain;`,
    `SET auto_explain.log_min_duration = 0;`,
    `SET auto_explain.log_analyze = true;`,
    `SET auto_explain.log_buffers = true;`,
    `SET auto_explain.log_timing = true;`,
    `SET auto_explain.log_verbose = true;`,
    `SET auto_explain.log_nested_statements = true;`,
    `SET log_statement = 'mod';`,
    `SET log_destination = 'csvlog';`,
    `SET log_rotation_age = 60;`,
    `SET log_min_duration_statement = 0;`,
  ],
  loadLogs: 'SELECT public.load_postgres_log_files();',
  getLogs: (time: string, byTrace: boolean, dbName: string) => `
    SELECT log_time, database_name, command_tag, virtual_transaction_id, message, detail, internal_query, query_id
    FROM logs.postgres_logs 
    WHERE command_tag IN ('SELECT', 'UPDATE', 'INSERT', 'DELETE')  
      ${byTrace ? `AND message LIKE '%traceparent=%'` : ''}
      ${dbName ? `AND database_name = '${dbName}'` : ''}
      AND message LIKE '%plan:%' 
      AND log_time > '${time}'
  ;`,
  getQueryIds: (time: string) => `
    SELECT virtual_transaction_id, query_id
    FROM (
      SELECT virtual_transaction_id, query_id, COUNT(*) AS appearance_count,
      RANK() OVER (PARTITION BY virtual_transaction_id ORDER BY COUNT(*) DESC) AS appearance_rank
      FROM logs.postgres_logs
      WHERE query_id <> 0 
        AND command_tag NOT IN ('', 'authentication', 'idle', 'BEGIN', 'COMMIT', 'SHOW')
        AND log_time > '${time}'
      GROUP BY virtual_transaction_id, query_id
    ) ranked
  ;`,
  createLogFunction: `
    CREATE OR REPLACE FUNCTION public.load_postgres_log_files(v_schema_name TEXT DEFAULT 'logs', v_table_name TEXT DEFAULT 'postgres_logs', v_prefer_csv BOOLEAN DEFAULT TRUE)
    RETURNS TEXT
    AS
    $BODY$
    DECLARE
    v_csv_supported INT := 0;
    v_hour_pattern_used INT := 0;
    v_filename TEXT;
    v_dt timestamptz;
    v_dt_max timestamptz;
    v_partition_name TEXT;
    v_ext_exists INT := 0;
    v_server_exists INT := 0;
    v_table_exists INT := 0;
    v_server_name TEXT := 'log_server';
    v_filelist_sql TEXT;
    v_enable_csv BOOLEAN := TRUE;
    BEGIN
      EXECUTE FORMAT('SELECT count(1) FROM pg_catalog.pg_extension WHERE extname=%L', 'log_fdw') INTO v_ext_exists;
      IF v_ext_exists = 0 THEN
        CREATE EXTENSION log_fdw;
      END IF;

      EXECUTE 'SELECT count(1) FROM pg_catalog.pg_foreign_server WHERE srvname=$1' INTO v_server_exists USING v_server_name;
      IF v_server_exists = 0 THEN
        EXECUTE FORMAT('CREATE SERVER %s FOREIGN DATA WRAPPER log_fdw', v_server_name);
      END IF;

      EXECUTE FORMAT('CREATE SCHEMA IF NOT EXISTS %I', v_schema_name);

      -- Set the search path to make sure the tables are created in dblogs schema
      EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'search_path', v_schema_name);

      -- The db log files are in UTC timezone so that date extracted from filename will also be UTC.
      --    Setting timezone to get correct table constraints.
      EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'timezone', 'UTC');

      -- Check the parent table exists
      EXECUTE 'SELECT count(1) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2' INTO v_table_exists USING v_schema_name, v_table_name;
      IF v_table_exists = 1 THEN
        RAISE NOTICE 'Table % already exists. It will be dropped.', v_table_name;
        EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'WARNING');
        EXECUTE FORMAT('DROP TABLE %I CASCADE', v_table_name);
        EXECUTE FORMAT('SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'NOTICE');
        v_table_exists = 0;
      END IF;

      -- Check the pg log format
      SELECT 1 INTO v_csv_supported FROM pg_catalog.pg_settings WHERE name='log_destination' AND setting LIKE '%csvlog%';
      IF v_csv_supported = 1 AND v_prefer_csv = TRUE THEN
        RAISE NOTICE 'CSV log format will be used.';
        v_filelist_sql = FORMAT('SELECT file_name FROM public.list_postgres_log_files() WHERE file_name LIKE %L ORDER BY 1 DESC LIMIT 2', '%.csv');
      ELSE
        RAISE NOTICE 'Default log format will be used.';
        v_filelist_sql = FORMAT('SELECT file_name FROM public.list_postgres_log_files() WHERE file_name NOT LIKE %L ORDER BY 1 DESC LIMIT 2', '%.csv');
        v_enable_csv = FALSE;
      END IF;

      FOR v_filename IN EXECUTE (v_filelist_sql)
      LOOP
        RAISE NOTICE 'Processing log file - %', v_filename;

        IF v_enable_csv = TRUE THEN
          -- Dynamically checking the file name pattern so that both allowed file names patters are parsed
          IF v_filename like 'postgresql.log.____-__-__-____.csv' THEN
            v_dt=substring(v_filename from 'postgresql.log.#"%#"-____.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__.csv' for '#')::int);
            v_dt_max = v_dt + INTERVAL '1 HOUR';
            v_dt=substring(v_filename from 'postgresql.log.#"%#"-____.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__.csv' for '#')::int) + INTERVAL '1 MINUTE' * (substring(v_filename from 'postgresql.log.____-__-__-__#"%#".csv' for '#')::int);
          ELSIF v_filename like 'postgresql.log.____-__-__-__.csv' THEN
            v_dt=substring(v_filename from 'postgresql.log.#"%#"-__.csv' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#".csv' for '#')::int);
            v_dt_max = v_dt + INTERVAL '1 HOUR';
          ELSIF v_filename like 'postgresql.log.____-__-__.csv' THEN
            v_dt=substring(v_filename from 'postgresql.log.#"%#".csv' for '#')::timestamp;
            v_dt_max = v_dt + INTERVAL '1 DAY';
          ELSE
            RAISE NOTICE '        Skipping file';
            CONTINUE;
          END IF;
          ELSE
            IF v_filename like 'postgresql.log.____-__-__-____' THEN
              v_dt=substring(v_filename from 'postgresql.log.#"%#"-____' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"__' for '#')::int) + INTERVAL '1 MINUTE' * (substring(v_filename from 'postgresql.log.____-__-__-__#"%#"' for '#')::int);
            ELSIF v_filename like 'postgresql.log.____-__-__-__' THEN
              v_dt=substring(v_filename from 'postgresql.log.#"%#"-__' for '#')::timestamp + INTERVAL '1 HOUR' * (substring(v_filename from 'postgresql.log.____-__-__-#"%#"' for '#')::int);
            ELSIF v_filename like 'postgresql.log.____-__-__' THEN
              v_dt=substring(v_filename from 'postgresql.log.#"%#"' for '#')::timestamp;
            ELSE
              RAISE NOTICE '        Skipping file';
              CONTINUE;
          END IF;
        END IF;
        v_partition_name=CONCAT(v_table_name, '_', to_char(v_dt, 'YYYYMMDD_HH24MI'));
        EXECUTE FORMAT('SELECT public.create_foreign_table_for_log_file(%L, %L, %L)', v_partition_name, v_server_name, v_filename);

        IF v_table_exists = 0 THEN
          EXECUTE FORMAT('CREATE TABLE %I (LIKE %I INCLUDING ALL)', v_table_name, v_partition_name);
          v_table_exists = 1;
        END IF;

        EXECUTE FORMAT('ALTER TABLE %I INHERIT %I', v_partition_name, v_table_name);

        IF v_enable_csv = TRUE THEN
          EXECUTE FORMAT('ALTER TABLE %I ADD CONSTRAINT check_date_range CHECK (log_time>=%L and log_time < %L)', v_partition_name, v_dt, v_dt_max);
        END IF;

      END LOOP;

      RETURN FORMAT('Postgres logs loaded to table %I.%I', v_schema_name, v_table_name);
    END;
    $BODY$
    LANGUAGE plpgsql;`,
};
