[![metis](https://static-asserts-public.s3.eu-central-1.amazonaws.com/metis-min-logo.png)](https://www.metisdata.io/)

# Metis Slow Query Log 

[Documentation](https://docs.metisdata.io)

## Overview

This is a Metis complementary package that enables postgres slow query log and auto analyze.
Using postgres extension file_fdw/log_fdw it collects relevant queries from databases instrumented by postgres instrumentation 
(available in Metis SDKs), having sql commenter enabled and writing traceparent comment on queries.
Those queries are exported to Metis platform to be analyzed and monitored.


## Usage

- Run
```shell
npm install --save @metis-data/slow-query-log
```


- Set the collector from your code:
```typescript
// With autoRun enabled
import { MetisSqlCollector } from '@metis-data/slow-query-log';

const metis = new MetisSqlCollector({ autoRun: true });
```

```typescript
// Without autoRun
import { MetisSqlCollector } from '@metis-data/slow-query-log';

const metis = new MetisSqlCollector();

// Call this function to send slow query logs from last 2 log files 
// with logs that added after the last call to run(), (or 10 minutes on first call)
metis.run();
```

- Options: 

    Options can be set from the constructor or from environment variables
    - autoRun: will send slow query log automatically every 1 minute, if set to false, calls to run() should be handled manually
    - connectionString: database url, must be set from this configuration or DATABASE_URL
    - metisApiKey: api key generated from metis platform, must be set from this configuration or METIS_API_KEY
    - logFetchInterval: intervals of fetching logs by millisecond, 1 minute by default
    - serviceName: service name to appear on Metis platform, "default" string by default 
    - debug: boolean, if true prints logs to provided logger or to console by default
    - logger: must implement log and error functions, by default ```{ log: console.log, error: console.error }``` 
      and will log "log" level only with debug: true

- Environment variables:

    Setting the environment variables is equivalent to some of the options above and only one of them is needed
    - DATABASE_URL: same as options.connectionString
    - METIS_API_KEY: same as options.metisApiKey
    - LOG_FETCH_INTERVAL: same as options.logFetchInterval
    - METIS_SERVICE_NAME: same as options.serviceName
    - METIS_DEBUG: same as options.debug

- Database setup:

    This package tries to install postgres file_fdw/log_fdw extension and run some configurations queries in the database, so the
    connection must be of a user with the appropriate permissions.
    If it is the first time of enabling postgres logs, some of the parameters requires a server restart.
    For managed databases (like aws rds) the next parameters must be set: 

| parameter                          | value                        |
|------------------------------------|------------------------------|
| session_preload_libraries          | auto_explain                 |
| auto_explain.log_min_duration      | 0                            |
| auto_explain.log_format            | 'json'                       |
| auto_explain.log_analyze           | true                         |
| auto_explain.log_buffers           | true                         |
| auto_explain.log_timing            | true                         |
| auto_explain.log_verbose           | true                         |
| auto_explain.log_nested_statements | true                         |
| logging_collector                  | true                         | 
| log_statement                      | 'mod'                        |
| log_destination                    | 'csvlog'                     |
| log_filename                       | 'postgresql.log.%Y-%m-%d-%H' |
| log_rotation_age                   | 60                           |
| log_min_duration_statement         | 0                            |
| compute_query_id                   | 'on'                         |

## Issues
If you would like to report a potential issue please use [Issues](https://github.com/metis-data/slow-query-log/issues)