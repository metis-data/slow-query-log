import { Client } from 'pg';
import { LogRow, MetisHandlerConfigs } from '../types';
import { v4 as uuid } from 'uuid';
import { QUERIES } from '../queries';

export class Handler {
  protected lastLogTime: string = new Date(new Date().getTime() - 60_000).toISOString().replace('T', ' ');
  protected readonly configs: MetisHandlerConfigs;
  protected readonly extension: string;
  protected readonly logger: any;
  protected readonly dbNames: string[];

  constructor(logger: any, configs: MetisHandlerConfigs) {
    this.configs = configs;
    this.logger = logger;
    this.dbNames = [];
  }

  public async setup(client: Client) {
    const dbNames = await this.getDatabaseNames(client);
    this.dbNames.push(...dbNames);
    await Promise.all(
      this.dbNames.map(async (db) => {
        const connectionString = `${this.configs.connectionString}/${db}`;
        const dbClient = await this.getDbClient(connectionString);
        await this.createExtension(dbClient, this.configs.extension);
        await this.enableFeature(dbClient);

        await dbClient.end();
      }),
    );
  }

  protected async getDbClient(connectionString: string) {
    try {
      const client = new Client({ connectionString });
      await client.connect();
      return client;
    } catch (e) {
      this.logger.error(`Could not connect to database ${connectionString}`);
    }
  }

  protected setLastLogTime(lastLog: any) {
    if (lastLog) {
      this.lastLogTime = new Date(lastLog.log_time || lastLog.last_call).toISOString().replace('T', ' ');
    }
  }

  protected getQueryIdFromTransaction(transactionId: string, logs: LogRow[]) {
    const transactionLogs = logs.filter((log) => log.virtual_transaction_id === transactionId);
    const logWithQueryId = transactionLogs.find((log) => log.query_id);
    return logWithQueryId?.query_id || '';
  }

  protected parseContext(query: string) {
    const traceparent = query?.split('traceparent=')?.[1];
    const traceId = traceparent?.split('-')?.[1];
    return { traceId: traceId || uuid(), spanId: uuid() };
  }

  protected parseDuration(startTime: string, durationString: string) {
    const [_, durationStr] = durationString.trim().split(' ');
    const duration = parseFloat(durationStr);
    const res = new Date(startTime);
    res.setMilliseconds(res.getMilliseconds() + Math.floor(duration));
    return { duration, endTime: res };
  }

  public async checkFeatureAvailability(client: Client): Promise<boolean> {
    // Check that pg version is 14+
    const { rows: versionRows } = await client.query(QUERIES.getVersion);
    const version = parseFloat(versionRows[0].version.split(' ')?.[1] || '');
    if (version < 14) {
      this.logger.log('Postgres version must be 14.x or later');
      return false;
    }

    return true;
  }

  public async createExtension(client: Client, extension?: string): Promise<void> {}
  public async enableFeature(client: Client): Promise<any> {}
  public async fetchData(extension?: string): Promise<any> {}

  private async getDatabaseNames(client: Client): Promise<string[]> {
    const { rows } = await client.query(QUERIES.getDatabaseNames);
    return rows.map((row) => row.datname);
  }
}
