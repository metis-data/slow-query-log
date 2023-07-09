import { Client, Pool } from 'pg';
import { QUERIES } from './queries';
import {
  AWS_CONTEXT,
  getProps,
  MetisSqlCollectorConfigs,
  MetisSqlCollectorOptions,
  REQUEST_ID,
  REQUEST_ID_HEADER,
  Response,
  RESPONSE,
  X_RAY,
  X_RAY_HEADER,
} from './types';
import * as https from 'https';
import { parse } from 'pg-connection-string';

export class MetisSqlCollector {
  private readonly connections: Client[] | Pool[];
  // private readonly db: Pool;
  private readonly configs: MetisSqlCollectorConfigs;
  private readonly logFetchInterval: number;
  private readonly metisExporterUrl: string;
  private readonly metisApiKey: string;
  // Set the first call to fetch logs from last 10 minutes
  private lastLogTime: string = new Date(new Date().getTime() - 10 * 60 * 1000).toISOString().replace('T', ' ');

  constructor(props: MetisSqlCollectorOptions = {}) {
    const options: MetisSqlCollectorOptions = getProps(props);
    const dbConfig = parse(options.connectionString);
    // this.db = new Pool({ connectionString: options.connectionString });
    this.logFetchInterval = options.logFetchInterval;
    this.metisExporterUrl = options.metisExportUrl;
    this.metisApiKey = options.metisApiKey;
    this.configs = { dbHost: dbConfig.host, serviceName: options.serviceName };
    this.connections = options.connections;

    this.enableSlowQueryLogs().then(async () => {
      await this.fetchLogs();
      console.log('Done pg setup');
    });
  }

  get queries() {
    return QUERIES;
  }

  private async enableSlowQueryLogs() {
    return Promise.all(
      [...this.queries.enableLogs, this.queries.createLogFunction].map(async (setupQuery) => {
        try {
          await this.connections[0]?.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  private async fetchLogs() {
    this.connections.map((conn) => {
      if (conn.constructor.name === 'Client') {
        conn.on('notice', (notice) => {
          console.log(notice);
        });
      } else if (conn.constructor.name === 'Pool' || conn.constructor.name === 'BoundPool') {
        conn.on('connect', (client) => {
          client.on('notice', (notice) => {
            console.log(notice);
          });
        });
      }
    });

    // setInterval(async () => {
    //   await this.db.query(this.queries.loadLogs);
    //   const res = await this.db.query(this.queries.getLogs(this.lastLogTime));
    //   if (res.rows.length) {
    //     this.setLastLogTime(res.rows.at(-1));
    //     const spans = this.parseLogs(res.rows);
    //     await this.exportLogs(spans);
    //   }
    // }, this.logFetchInterval);
  }

  private setLastLogTime(lastLog: any) {
    if (lastLog) {
      this.lastLogTime = new Date(lastLog.log_time).toISOString().replace('T', ' ');
    }
  }

  private parseLogs(rawLogs: any[]) {
    return rawLogs.map((log) => {
      try {
        const { log_time: logTime, database_name: dbName, message } = log;
        const [durationString, planObj] = message.split('plan:');
        const parsed = JSON.parse(planObj);
        const { ['Query Text']: query, ...plan } = parsed;
        const { traceId, spanId } = this.parseContext(query);
        const { duration, endTime } = this.parseDuration(logTime, durationString);
        return JSON.stringify({
          kind: 'SpanKind.CLIENT',
          context: {
            trace_id: traceId,
            span_id: spanId,
          },
          start_time: logTime,
          end_time: endTime,
          duration: plan.Plan['Actual Total Time'] || duration,
          attributes: {
            ['db.statement.metis']: query,
            ['db.statement.metis.plan']: JSON.stringify(plan),
            ['db.name']: dbName,
            ['db.system']: 'postgresql',
            ['net.host.name']: this.configs.dbHost,
            ['net.peer.name']: this.configs.dbHost,
          },
          resource: {
            ['telemetry.sdk.language']: 'slow-query-log-collector',
            ['service.name']: this.configs.serviceName,
          },
        });
      } catch (e) {
        console.log('Parse failed');
        // TODO handle error
      }
    });
  }

  private parseContext(query: string) {
    try {
      const [_, traceparent] = query.split('traceparent=');
      const [__, traceId, spanId] = traceparent.split('-');
      return { traceId, spanId };
    } catch (e) {
      console.log('Parsing context failed');
      // TODO handle error
    }
  }

  private parseDuration(startTime: string, durationString: string) {
    try {
      const [_, durationStr] = durationString.trim().split(' ');
      const duration = parseFloat(durationStr);
      const res = new Date(startTime);
      res.setMilliseconds(res.getMilliseconds() + Math.floor(duration));
      return { duration, endTime: res };
    } catch (e) {
      console.log('Parsing duration failed');
      // TODO handle error
    }
  }

  private async exportLogs(data: string[]) {
    for (const send of MetisSqlCollector.chunk(data)) {
      try {
        const dataString = JSON.stringify(send);
        const res: Response = await this.post(this.metisExporterUrl, dataString, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': dataString.length,
            'x-api-key': this.metisApiKey,
          },
        });

        const contexts = {
          [AWS_CONTEXT]: {
            [X_RAY]: res.headers[X_RAY_HEADER] as string,
            [REQUEST_ID]: res.headers[REQUEST_ID_HEADER] as string,
            [RESPONSE]: res.text,
          },
        };

        if (res.statusCode >= 400) {
          console.log('Res status is bad');
          // TODO handle error with contexts
        }
      } catch (e) {
        console.log('Error in logs export');
        // TODO handle error with contexts
      }
    }
  }

  private static *chunk(data: string[], limit = 150_000) {
    if (!data) {
      return [];
    }

    let result = [];
    let counter = 0;
    for (const item of data) {
      counter += item.length;
      result.push(item);
      if (counter >= limit) {
        yield result;
        counter = 0;
        result = [];
      }
    }
    yield result;
  }

  private async post(url: string, data: string, options: any): Promise<Response> {
    return new Promise((resolve, reject) => {
      const req = https.request(url, options, (res) => {
        const body = [];
        res.on('data', (chunk) => body.push(chunk));
        res.on('end', () => {
          let text;
          try {
            text = Buffer.concat(body).toString();
            const json = JSON.parse(text);
            resolve({
              headers: res.headers,
              statusCode: res.statusCode,
              json,
              text,
            });
          } catch (e) {
            resolve({
              headers: res.headers,
              statusCode: res.statusCode,
              text,
              error: e,
            });
          }
        });
      });

      req.on('error', (err) => {
        reject(err);
      });

      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Request time out'));
      });

      req.write(data);
      req.end();
    });
  }
}
