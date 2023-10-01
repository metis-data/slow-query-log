import { Client } from 'pg';
import { QUERIES } from './queries';
import {
  AWS_CONTEXT,
  CommandTag,
  ExcludedQueriesPrefixes,
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
  private readonly configs: MetisSqlCollectorConfigs;
  private readonly logger: { log?: any; info?: any; error: any };
  private readonly logFetchInterval: number;
  private readonly metisExporterUrl: string;
  private readonly metisApiKey: string;
  private readonly dbName: string;
  private readonly byTrace: boolean;
  private readonly autoRun: boolean;
  private readonly inDebug: boolean;
  private logSampleRate: number;
  private isAvailable: boolean = false;
  // Set the first call to fetch logs from last 1 minute
  private lastLogTime: string = new Date(new Date().getTime() - 60_000).toISOString().replace('T', ' ');

  constructor(props: MetisSqlCollectorOptions = {}) {
    const options: MetisSqlCollectorOptions = getProps(props);
    const dbConfig = parse(options.connectionString);
    this.logFetchInterval = options.logFetchInterval;
    this.logSampleRate = options.logSampleRate;
    this.metisExporterUrl = options.metisExportUrl;
    this.metisApiKey = options.metisApiKey;
    this.configs = { dbHost: dbConfig.host, serviceName: options.serviceName };
    this.logger = options.logger;
    this.inDebug = options.debug;
    this.dbName = options.dbName;
    this.byTrace = options.byTrace;
    this.autoRun = props.autoRun;
  }

  get queries() {
    return QUERIES;
  }

  public async setup(client: Client) {
    await this.checkFeatureAvailability(client);
    if (!this.isAvailable) return;
    await this.setSampleRate(client, this.logSampleRate);
    await this.enableSlowQueryLogs(client);
    if (this.autoRun) {
      await this.autoFetchLogs(client);
    }
  }

  public async run(client: Client) {
    await this.fetchLogs(client);
  }

  public async setSampleRate(client: Client, rate: number) {
    try {
      await client.query(this.queries.setSampleRate(rate));
      await client.query(this.queries.reloadConf);
      this.logSampleRate = rate;
    } catch (e) {
      this.log(`Could not set sample rate: ${e.message}`);
    }
  }

  private log(message: string) {
    if (this.inDebug) {
      this.logger.log(message);
    }
  }

  private async checkFeatureAvailability(client: Client) {
    try {
      await Promise.all(
        Object.entries(this.queries.checkAvailability).map(async ([query, res]: [query: string, res: any]) => {
          const result = (await client.query(query)).rows;
          if (result[0]?.version?.startsWith('PostgreSQL')) {
            const version = parseFloat(result[0].version.split(' ')?.[1] || '');
            if (version < 14) {
              this.log('Postgres version must be 14.x or later');
              throw new Error('Database configuration are not set to enable slow query log');
            }
          } else {
            if (result[0][res.name] !== res.val && !result[0][res.name].includes(res.val)) {
              this.log(`Database parameter ${res.name} should be set to ${res.val}`);
              throw new Error('Database configuration are not set to enable slow query log');
            }
          }
        }),
      );
    } catch (e) {
      this.isAvailable = false;
    }
    this.isAvailable = true;
  }

  private async enableSlowQueryLogs(client: Client) {
    return Promise.all(
      [...this.queries.enableLogs, this.queries.createLogFunction].map(async (setupQuery) => {
        try {
          await client.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  private async autoFetchLogs(client: Client) {
    setInterval(async () => {
      await this.fetchLogs(client);
    }, this.logFetchInterval);
  }

  private async fetchLogs(client: Client) {
    await client.query(this.queries.loadLogs);
    const res = await client.query(this.queries.getLogs(this.lastLogTime, this.byTrace, this.dbName));
    if (res.rows.length) {
      this.setLastLogTime(res.rows.at(-1));
      const spans = this.parseLogs(res.rows);
      await this.exportLogs(spans);
    }
  }

  private getQueryIdFromTransaction(transactionId: string, logs: LogRow[]) {
    const transactionLogs = logs.filter((log) => log.virtual_transaction_id === transactionId);
    const logWithQueryId = transactionLogs.find((log) => log.query_id);
    return logWithQueryId?.query_id || '';
  }

  private setLastLogTime(lastLog: any) {
    if (lastLog) {
      this.lastLogTime = new Date(lastLog.log_time).toISOString().replace('T', ' ');
    }
  }

  private parseLogs(rawLogs: LogRow[]) {
    const bindLogs = rawLogs.filter((log) => log.command_tag === CommandTag.BIND);
    const parseLogs = rawLogs.filter((log) => log.command_tag === CommandTag.PARSE);
    const logsWithPlan = rawLogs.filter((log) => log.message.includes('plan:'));
    return logsWithPlan
      .map((log) => {
        try {
          const { log_time: logTime, database_name: dbName, virtual_transaction_id: transactionId, message } = log;
          const queryId = log.query_id ?? this.getQueryIdFromTransaction(transactionId, [...bindLogs, ...parseLogs]);
          const [durationString, ...planObj] = message.split('plan:');
          const parsed = JSON.parse(planObj.join('plan:').trim());
          const { ['Query Text']: query, ...plan } = parsed;
          if (!query || ExcludedQueriesPrefixes.some((prefix) => query.trim().startsWith(prefix))) return undefined;
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
          return undefined;
        }
      })
      .filter((log) => log);
  }

  private parseContext(query: string) {
    const traceparent = query?.split('traceparent=')?.[1];
    const traceId = traceparent?.split('-')?.[1];
    return { traceId: traceId || uuid(), spanId: uuid() };
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
