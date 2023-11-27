import { Client } from 'pg';
import { QUERIES } from './queries';
import {
  AWS_CONTEXT,
  Extensions,
  getHandler,
  getProps,
  MetisSqlCollectorOptions,
  REQUEST_ID,
  REQUEST_ID_HEADER,
  Response,
  RESPONSE,
  toObj,
  X_RAY,
  X_RAY_HEADER,
} from './types';
import * as https from 'https';
import { parse } from 'pg-connection-string';
import { Handler } from './handlers/handler.handler';

export class MetisSqlCollector {
  private readonly connections: string[];
  private readonly handlers: Handler[];
  private readonly logger: { log?: any; info?: any; error: any };
  private readonly logFetchInterval: number;
  private readonly metisExporterUrl: string;
  private readonly metisApiKey: string;
  private readonly serviceName: string;
  private readonly exportResults: boolean;
  private readonly autoRun: boolean;

  constructor(props: MetisSqlCollectorOptions = {}) {
    const options: MetisSqlCollectorOptions = getProps(props);
    this.connections = this.parseConnections(options.connectionStrings);
    this.handlers = [];
    this.logFetchInterval = options.logFetchInterval;
    this.metisExporterUrl = options.metisExportUrl;
    this.metisApiKey = options.metisApiKey;
    this.serviceName = options.serviceName;
    this.logger = options.logger;
    this.exportResults = options.exportResults;
    this.autoRun = props.autoRun;
  }

  public async setup() {
    const res = await this.setupHandlers();

    if (this.autoRun) {
      await this.autoFetchLogs();
    }

    return res;
  }

  public async run() {
    const res = await Promise.all(this.handlers.map((handler) => handler.fetchData()));
    const spans = toObj(res);
    if (this.exportResults) {
      await this.exportLogs(spans);
    }

    return spans;
  }

  private parseConnections(connectionStrings: string | string[]) {
    return Array.isArray(connectionStrings) ? connectionStrings : connectionStrings.split(';');
  }

  private async setupHandlers() {
    const res = {};
    await Promise.all(
      this.connections.map(async (connection) => {
        let client: Client;
        try {
          const { host, port } = parse(connection);
          client = await this.getDbClient(connection);

          const extension = await this.getAvailableExtension(client);
          if (!extension) throw new Error('extension is not available');

          const handler = getHandler[extension](this.logger, {
            extension,
            host: `${host}:${port}`,
            serviceName: this.serviceName,
            connectionString: connection,
          });
          const isAvailable = await handler.checkFeatureAvailability(client);
          if (isAvailable) {
            this.handlers.push(handler);
            await handler.setup(client);
          } else throw new Error('feature is not available on this database');

          await client.end();
          res[connection] = 'Setup succeeded';
        } catch (e) {
          res[connection] = `Setup failed error: ${e.message}`;
          await client?.end();
        }
      }),
    );

    return res;
  }

  private async getDbClient(connectionString: string) {
    try {
      const client = new Client({ connectionString });
      await client.connect();
      return client;
    } catch (e) {
      this.logger.error(`Could not connect to database ${connectionString}`);
    }
  }

  private async getAvailableExtension(client: Client) {
    // This has a certain order of precedence:
    // First, pg_store_plans, then either file_fdw/log_fdw depends on availability
    const { rows } = await client.query(QUERIES.getExtensions);
    const extensions = rows.map((extension) => extension.name);
    if (extensions.includes(Extensions.PG_STORE_PLANS)) return Extensions.PG_STORE_PLANS;
    if (extensions.includes(Extensions.LOG_FDW)) return Extensions.LOG_FDW;
    if (extensions.includes(Extensions.FILE_FDW)) return Extensions.FILE_FDW;
  }

  private async autoFetchLogs() {
    setInterval(async () => {
      await this.run();
    }, this.logFetchInterval);
  }

  private async exportLogs(data: any) {
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
        this.logger.error(`Error in logs export: ${e}`);
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
      if (counter + item.length > limit) {
        yield result;
        counter = 0;
        result = [];
      } else {
        counter += item.length;
        result.push(item);
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
