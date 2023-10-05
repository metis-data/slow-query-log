export const QUERIES = {
  getVersion: '/* metis */ SELECT version();',
  checkAvailability: {
    'SHOW logging_collector;': { name: 'logging_collector', val: 'on' },
    'SHOW log_destination;': { name: 'log_destination', val: 'csvlog' },
    'SHOW log_filename;': { name: 'log_filename', val: 'postgresql.log.%Y-%m-%d-%H' },
    'SHOW log_rotation_age;': { name: 'log_rotation_age', val: '1h' },
  },
  getExtensions: '/* metis */ SELECT name FROM pg_available_extensions;',
  createExtension: (extension: string) => `/* metis */ CREATE EXTENSION IF NOT EXISTS ${extension}`,
  enablePlans: [
    `SET pg_store_plans.log_analyze = true;`,
    `SET pg_store_plans.log_timing = true;`,
    `SET pg_store_plans.track = 'all';`,
    `SET pg_store_plans.plan_format = 'json';`,
    `SET pg_store_plans.log_buffers = true;`,
    `SET compute_query_id = 'on'`,
  ],
  enableLogs: [
    `LOAD 'auto_explain';`,
    `SET auto_explain.log_format = 'json';`,
    `SET auto_explain.log_min_duration = 0;`,
    `SET auto_explain.log_analyze = true;`,
    `SET auto_explain.log_buffers = true;`,
    `SET auto_explain.log_timing = true;`,
    `SET auto_explain.log_verbose = true;`,
    `SET auto_explain.log_nested_statements = true;`,
    `SET log_statement = 'mod';`,
    `SET log_min_duration_statement = 0;`,
    `SET compute_query_id = 'on'`,
  ],
  setSampleRate: (rate: number) => `ALTER SYSTEM SET log_statement_sample_rate = ${rate};`,
  reloadConf: '/* metis */ SELECT pg_reload_conf();',
  loadLogs: '/* metis */ SELECT public.load_postgres_log_files();',
  getLogs: (time: string) => `
    /* metis */
    SELECT log_time, database_name, command_tag, virtual_transaction_id, message, detail, internal_query, query_id
    FROM logs.postgres_logs 
    WHERE command_tag IN ('SELECT', 'UPDATE', 'INSERT', 'DELETE', 'BIND', 'PARSE')
      AND log_time > '${time}'
  ;`,
  getPlans: (time: string) => `
    /* metis */
    SELECT pd.datname as database_name, query, plan, last_call, psp.mean_time as duration, pss.queryid as query_id from pg_stat_statements pss
    JOIN pg_store_plans psp on pss.queryid = psp.queryid and pss.dbid = psp.dbid
    JOIN pg_database pd on psp.dbid = pd.oid
    WHERE last_call > '${time}'
    ORDER BY last_call desc
    ;`,
  createLogFunction: `
    CREATE OR REPLACE FUNCTION public.load_postgres_log_files(v_schema_name TEXT DEFAULT 'logs', v_table_name TEXT DEFAULT 'postgres_logs', v_prefer_csv BOOLEAN DEFAULT TRUE)
    RETURNS TEXT
    AS
    $BODY$
    DECLARE
    v_extension_name TEXT;
    v_log_fdw_available BOOLEAN;
    v_store_plans_available BOOLEAN;
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
      EXECUTE FORMAT('SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name=%L)', 'pg_store_plans') INTO v_store_plans_available;
      IF v_store_plans_available = TRUE THEN
        v_extension_name := 'pg_store_plans';
      ELSE
        EXECUTE FORMAT('SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name=%L)', 'log_fdw') INTO v_log_fdw_available;
        IF v_log_fdw_available = TRUE THEN
          v_extension_name := 'log_fdw';
        ELSE
          v_extension_name := 'file_fdw';
        END IF;
      END IF;
      IF v_extension_name = 'file_fdw' THEN
        CREATE OR REPLACE FUNCTION public.list_postgres_log_files()
        RETURNS TABLE (file_name TEXT)
        AS 
        $BODY_1$
        DECLARE
        v_log_file_path TEXT;
        v_log_file_dir TEXT;
        v_full_path TEXT;
        BEGIN
          EXECUTE 'SHOW data_directory' INTO v_log_file_path;
          EXECUTE 'SHOW log_directory' INTO v_log_file_dir;
          v_full_path := v_log_file_path || '/' || v_log_file_dir;
          RETURN QUERY EXECUTE FORMAT('/* metis */ SELECT * FROM pg_ls_dir(%L) as file_name', v_full_path);
        END;
        $BODY_1$ 
        LANGUAGE plpgsql;
        
        CREATE OR REPLACE FUNCTION public.create_foreign_table_for_log_file(IN table_name TEXT, IN server_name TEXT, IN log_file_name TEXT)
        RETURNS void
        AS 
        $BODY_2$
        DECLARE
        v_log_file_path TEXT;
        v_log_file_dir TEXT;
        v_full_path TEXT;
        BEGIN
          EXECUTE 'SHOW data_directory' INTO v_log_file_path;
          EXECUTE 'SHOW log_directory' INTO v_log_file_dir;
          v_full_path := v_log_file_path || '/' || v_log_file_dir || '/' || log_file_name;
          EXECUTE FORMAT('CREATE FOREIGN TABLE %I (
            log_time timestamp(3) with time zone,
            user_name text,
            database_name text,
            process_id integer,
            connection_from text,
            session_id text,
            session_line_num bigint,
            command_tag text,
            session_start_time timestamp with time zone,
            virtual_transaction_id text,
            transaction_id bigint,
            error_severity text,
            sql_state_code text,
            message text,
            detail text,
            hint text,
            internal_query text,
            internal_query_pos integer,
            context text,
            query text,
            query_pos integer,
            location text,
            application_name text,
            backend_type text,
            leader_pid integer,
            query_id bigint
          ) SERVER %I OPTIONS ( filename %L, format %L )',
          table_name,
          server_name,
          v_full_path,
          'csv');
        END;
        $BODY_2$ 
        LANGUAGE plpgsql;
      END IF;

      EXECUTE '/* metis */ SELECT count(1) FROM pg_catalog.pg_foreign_server WHERE srvname=$1' INTO v_server_exists USING v_server_name;
      IF v_server_exists = 0 THEN
        EXECUTE FORMAT('CREATE SERVER %s FOREIGN DATA WRAPPER %I', v_server_name, v_extension_name);
      END IF;

      EXECUTE FORMAT('CREATE SCHEMA IF NOT EXISTS %I', v_schema_name);

      -- Set the search path to make sure the tables are created in dblogs schema
      EXECUTE FORMAT('/* metis */ SELECT set_config(%L, %L, TRUE)', 'search_path', v_schema_name);

      -- The db log files are in UTC timezone so that date extracted from filename will also be UTC.
      --    Setting timezone to get correct table constraints.
      EXECUTE FORMAT('/* metis */ SELECT set_config(%L, %L, TRUE)', 'timezone', 'UTC');

      -- Check the parent table exists
      EXECUTE '/* metis */ SELECT count(1) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2' INTO v_table_exists USING v_schema_name, v_table_name;
      IF v_table_exists = 1 THEN
        RAISE NOTICE 'Table % already exists. It will be dropped.', v_table_name;
        EXECUTE FORMAT('/* metis */ SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'WARNING');
        EXECUTE FORMAT('DROP TABLE %I CASCADE', v_table_name);
        EXECUTE FORMAT('/* metis */ SELECT set_config(%L, %L, TRUE)', 'client_min_messages', 'NOTICE');
        v_table_exists = 0;
      END IF;

      -- Check the pg log format
      /* metis */ SELECT 1 INTO v_csv_supported FROM pg_catalog.pg_settings WHERE name='log_destination' AND setting LIKE '%csvlog%';
      IF v_csv_supported = 1 AND v_prefer_csv = TRUE THEN
        RAISE NOTICE 'CSV log format will be used.';
        v_filelist_sql = FORMAT('/* metis */ SELECT file_name FROM public.list_postgres_log_files() WHERE file_name LIKE %L ORDER BY 1 DESC LIMIT 2', '%.csv');
      ELSE
        RAISE NOTICE 'Default log format will be used.';
        v_filelist_sql = FORMAT('/* metis */ SELECT file_name FROM public.list_postgres_log_files() WHERE file_name NOT LIKE %L ORDER BY 1 DESC LIMIT 2', '%.csv');
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
        EXECUTE FORMAT('/* metis */ SELECT public.create_foreign_table_for_log_file(%L, %L, %L)', v_partition_name, v_server_name, v_filename);

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
