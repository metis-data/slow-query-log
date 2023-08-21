[![metis](https://static-asserts-public.s3.eu-central-1.amazonaws.com/metis-min-logo.png)](https://www.metisdata.io/)

# Metis Slow Query Log 

[Documentation](https://docs.metisdata.io)

## Overview

This is a Metis complementary package that enables postgres slow query log and auto analyze.
Using postgres extension file_fdw/log_fdw it collects relevant queries from databases instrumented by postgres instrumentation 
(available in Metis SDKs), having sql commenter enabled and writing traceparent comment on queries.
Those queries are exported to Metis platform to be analyzed and monitored.

**Note:** compute_query_id flag which is part of the feature is available from postgres version 14 or later.


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
// with logs that added after the last call to run(), (or 1 minute on first call)
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

    This package tries to install postgres file_fdw/log_fdw extension, so the
    connection must be of a user with the appropriate permissions.

    For managed databases (like aws rds) the next parameters must be set: 

| parameter                          | value                            | db needs restart? |
|------------------------------------|----------------------------------|-------------------|
| session_preload_libraries          | auto_explain                     | yes               |
| logging_collector                  | 'on'                             | yes (locally)     |
| log_destination                    | 'csvlog'                         | yes (locally)     |
| log_filename                       | 'postgresql.log.%Y-%m-%d-%H'     | yes (locally)     |
| log_rotation_age                   | 60                               | yes (locally)     |
| auto_explain.log_min_duration      | 0                                | no                |
| auto_explain.log_format            | 'json'                           | no                |
| auto_explain.log_analyze           | true                             | no                |
| auto_explain.log_buffers           | true                             | no                |
| auto_explain.log_timing            | true                             | no                |
| auto_explain.log_verbose           | true                             | no                |
| auto_explain.log_nested_statements | true                             | no                |
| log_statement                      | 'mod'                            | no                |
| log_min_duration_statement         | 0                                | no                |
| compute_query_id                   | 'on'                             | no                |

- RDS setup using aws cli:
    If it is the first time of enabling postgres logs on RDS, a new parameter group should be created with logging_collector=on.

    After enabling slow query log in your RDS, the rest of postgres variables can be set with aws cli:
    ```shell
    aws rds modify-db-parameter-group \
      --db-parameter-group-name your-parameter-group-name \
      --parameters \
        "ParameterName=shared_preload_libraries,ParameterValue=auto_explain,ApplyMethod=pending-reboot" \
        "ParameterName=log_destination,ParameterValue=csvlog,ApplyMethod=immediate" \
        "ParameterName=log_filename,ParameterValue=postgresql.log.%Y-%m-%d-%H,ApplyMethod=immediate" \
        "ParameterName=log_rotation_age,ParameterValue=60,ApplyMethod=immediate" \
        "ParameterName=log_statement,ParameterValue=mod,ApplyMethod=immediate" \
        "ParameterName=log_min_duration_statement,ParameterValue=0,ApplyMethod=immediate" \
        "ParameterName=compute_query_id,ParameterValue=on,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_format,ParameterValue=json,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_min_duration,ParameterValue=0,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_analyze,ParameterValue=true,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_buffers,ParameterValue=true,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_timing,ParameterValue=true,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_verbose,ParameterValue=true,ApplyMethod=immediate" \
        "ParameterName=auto_explain.log_nested_statements,ParameterValue=true,ApplyMethod=immediate"
    
    # reboot to apply shared_preload_libraries, this set will override an exists values
    # so if another library is needed make sure to add it to the string command
    aws rds reboot-db-instance --db-instance-identifier your-db-instance-id
    ```

- Docker/local database setup:

    If you are using postgres on docker container, you should set the required database parameters in the docker-compose file:
    ```dockerfile
    version: '3.1'

    services:
      db:
        image: postgres
        environment:
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: postgres
    
        command: postgres 
            -c shared_preload_libraries=auto_explain
            -c logging_collector=on 
            -c log_destination=csvlog 
            -c log_filename=postgresql.log.%Y-%m-%d-%H 
            -c log_rotation_age=60
            -c log_statement=mod
            -c log_min_duration_statement=0
            -c compute_query_id=on
            -c auto_explain.log_format=json
            -c auto_explain.log_min_duration=0
            -c auto_explain.log_analyze=true
            -c auto_explain.log_buffers=true
            -c auto_explain.log_timing=true
            -c auto_explain.log_verbose=true
            -c auto_explain.log_nested_statements=true
    #...
    ```

    If you are using any other local server, make sure to set those parameters in postgres config file postgresql.conf and restart the server.

## Issues
If you would like to report a potential issue please use [Issues](https://github.com/metis-data/slow-query-log/issues)