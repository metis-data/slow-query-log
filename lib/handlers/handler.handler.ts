import { Client } from 'pg';
import { LogRow, MetisSqlCollectorConfigs } from '../types';
import { v4 as uuid } from 'uuid';

export class Handler {
  protected lastLogTime: string = new Date(new Date().getTime() - 60_000).toISOString().replace('T', ' ');
  protected readonly logger: any;
  protected readonly queries: any;
  protected readonly configs: MetisSqlCollectorConfigs;
  protected readonly dbName: string;
  protected readonly byTrace: boolean;

  constructor(logger: any, queries: any, configs: MetisSqlCollectorConfigs, dbName: string, byTrace: boolean) {
    this.logger = logger;
    this.queries = queries;
    this.configs = configs;
    this.dbName = dbName;
    this.byTrace = byTrace;
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
    const { rows: versionRows } = await client.query(this.queries.getVersion);
    const version = parseFloat(versionRows[0].version.split(' ')?.[1] || '');
    if (version < 14) {
      this.logger.log('Postgres version must be 14.x or later');
      return false;
    }

    return true;
  }

  public async createExtension(client: Client, extension?: string): Promise<void> {}
  public async enableFeature(client: Client): Promise<any> {}
  public async fetchData(client: Client, extension?: string): Promise<any> {}
}
