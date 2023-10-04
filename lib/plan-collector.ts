import { Client } from 'pg';
import { QUERIES } from './queries';
import {
  AWS_CONTEXT,
  Extensions,
  getHandler,
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
import { Handler } from './handlers/handler.handler';

export class MetisSqlCollector {
  private handler: Handler;
  private readonly configs: MetisSqlCollectorConfigs;
  private readonly logger: { log?: any; info?: any; error: any };
  private readonly logFetchInterval: number;
  private readonly metisExporterUrl: string;
  private readonly metisApiKey: string;
  private readonly dbName: string;
  private readonly exportResults: boolean;
  private readonly byTrace: boolean;
  private readonly autoRun: boolean;
  private readonly inDebug: boolean;
  private logSampleRate: number;
  private extension: Extensions;

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
    this.exportResults = options.exportResults;
    this.byTrace = options.byTrace;
    this.autoRun = props.autoRun;
  }

  get queries() {
    return QUERIES;
  }

  public async setup(client: Client) {
    await this.getAvailableExtension(client);
    this.handler = getHandler[this.extension](this.logger, this.queries, this.configs, this.dbName, this.byTrace);
    const isAvailable = await this.handler.checkFeatureAvailability(client);
    if (!isAvailable) return;

    await this.handler.createExtension(client, this.extension);
    await this.setSampleRate(client, this.logSampleRate);
    await this.handler.enableFeature(client);
    if (this.autoRun) {
      await this.autoFetchLogs(client);
    }
  }

  public async run(client: Client) {
    const spans = await this.handler.fetchData(client, this.extension);
    if (this.exportResults) {
      await this.exportLogs(spans);
    }

    return spans;
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

  private async getAvailableExtension(client: Client) {
    // This has a certain order of precedence:
    // First, pg_store_plans, then either file_fdw/log_fdw depends on availability
    const { rows } = await client.query(this.queries.getExtensions);
    const extensions = rows.map((extension) => extension.name);
    if (extensions.includes(Extensions.FILE_FDW)) this.extension = Extensions.FILE_FDW;
    if (extensions.includes(Extensions.LOG_FDW)) this.extension = Extensions.LOG_FDW;
    if (extensions.includes(Extensions.PG_STORE_PLANS)) this.extension = Extensions.PG_STORE_PLANS;
  }

  private async autoFetchLogs(client: Client) {
    setInterval(async () => {
      const spans = await this.handler.fetchData(client, this.extension);

      if (this.exportResults) {
        await this.exportLogs(spans);
      }
    }, this.logFetchInterval);
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
