import { Pool } from 'pg';
import { QUERIES } from './queries';
import {
  AWS_CONTEXT,
  getProps,
  LogRow,
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
import { v4 as uuid } from 'uuid';

export class MetisSqlCollector {
  private readonly db: Pool;
  private readonly configs: MetisSqlCollectorConfigs;
  private readonly logger: { log: any; error: any };
  private readonly logFetchInterval: number;
  private readonly metisExporterUrl: string;
  private readonly metisApiKey: string;
  private readonly inDebug: boolean;
  // Set the first call to fetch logs from last 10 minutes
  private lastLogTime: string = new Date(new Date().getTime() - 10 * 60 * 1000).toISOString().replace('T', ' ');

  constructor(props: MetisSqlCollectorOptions = {}) {
    const options: MetisSqlCollectorOptions = getProps(props);
    const dbConfig = parse(options.connectionString);
    this.db = new Pool({ connectionString: options.connectionString });
    this.logFetchInterval = options.logFetchInterval;
    this.metisExporterUrl = options.metisExportUrl;
    this.metisApiKey = options.metisApiKey;
    this.configs = { dbHost: dbConfig.host, serviceName: options.serviceName };
    this.logger = options.logger;
    this.inDebug = options.debug;

    this.enableSlowQueryLogs()
      .then(async () => {
        if (props.autoRun) {
          await this.autoFetchLogs();
        }
        this.logger.log('Done pg setup');
      })
      .catch((e) => {
        this.logger.log(e);
      });
  }

  get queries() {
    return QUERIES;
  }

  public async run() {
    await this.fetchLogs();
  }

  private log(message: string) {
    if (this.inDebug) {
      this.logger.log(message);
    }
  }

  private async enableSlowQueryLogs() {
    return Promise.all(
      [...this.queries.enableLogs, this.queries.createLogFunction].map(async (setupQuery) => {
        try {
          await this.db.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  private async autoFetchLogs() {
    setInterval(async () => {
      await this.fetchLogs();
    }, this.logFetchInterval);
  }

  private async fetchLogs() {
    await this.db.query(this.queries.loadLogs);
    const res = await this.db.query(this.queries.getLogs(this.lastLogTime));
    if (res.rows.length) {
      this.setLastLogTime(res.rows.at(-1));
      const spans = this.parseLogs(res.rows);
      await this.exportLogs(spans);
    }
  }

  private setLastLogTime(lastLog: any) {
    if (lastLog) {
      this.lastLogTime = new Date(lastLog.log_time).toISOString().replace('T', ' ');
    }
  }

  private parseLogs(rawLogs: LogRow[]) {
    return rawLogs
      .map((log) => {
        if (log.message.includes('logs.postgres_logs')) return;
        try {
          const { log_time: logTime, database_name: dbName, message, query_id: queryId } = log;
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
              ['db.query.id']: queryId,
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
          this.log(`Parse failed: ${e}`);
          return;
        }
      })
      .filter((log) => log);
  }

  private parseContext(query: string) {
    const traceparent = query.split('traceparent=')[1];
    const traceId = traceparent.split('-')[1];
    return { traceId, spanId: uuid() };
  }

  private parseDuration(startTime: string, durationString: string) {
    const [_, durationStr] = durationString.trim().split(' ');
    const duration = parseFloat(durationStr);
    const res = new Date(startTime);
    res.setMilliseconds(res.getMilliseconds() + Math.floor(duration));
    return { duration, endTime: res };
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
          throw new Error(`Bad status code: ${res.statusCode}, ${JSON.stringify(contexts)}`);
        }
      } catch (e) {
        this.log(`Error in logs export: ${e}`);
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
