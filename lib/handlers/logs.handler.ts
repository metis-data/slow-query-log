import { Client } from 'pg';
import { Handler } from './handler.handler';
import { CommandTag, ExcludedQueriesPrefixes, LogRow, toObj } from '../types';
import { QUERIES } from '../queries';

export class LogsHandler extends Handler {
  public async checkFeatureAvailability(client: Client) {
    let isAvailable = await super.checkFeatureAvailability(client);

    if (!isAvailable) return isAvailable;

    // Check if slow query log params that need to be configured by the user are well set
    await Promise.all(
      Object.entries(QUERIES.checkAvailability).map(
        async ([query, res]: [query: string, res: { name: string; val: string }]) => {
          const { rows } = await client.query(query);
          if (rows[0]?.[res.name] !== res.val && !rows[0]?.[res.name].includes(res.val)) {
            this.logger.log(`Database parameter ${res.name} should be set to ${res.val}`);
            isAvailable = false;
          }
        },
      ),
    );
    return isAvailable;
  }

  public async createExtension(client: Client, extension: string) {
    await client.query(QUERIES.createExtension(extension));
  }

  public async enableFeature(client: Client) {
    return Promise.all(
      [...QUERIES.enableLogs, QUERIES.createLogFunction].map(async (setupQuery) => {
        try {
          await client.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  public async fetchData() {
    try {
      const client = await this.getDbClient(this.configs.connectionString);
      await client.query(QUERIES.loadLogs);
      const { rows } = await client.query(QUERIES.getLogs(this.lastLogTime));
      await client.end();

      if (rows.length) {
        this.setLastLogTime(rows.at(-1));
        return { [this.configs.host]: this.parseLogs(rows) };
      }

      return { [this.configs.host]: 'No data' };
    } catch (e) {
      return { [this.configs.host]: `Failed to fetch data: ${e.message}` };
    }
  }

  private parseLogs(rawLogs: LogRow[]) {
    const res = {};
    const bindLogs = rawLogs.filter((log) => log.command_tag === CommandTag.BIND);
    const parseLogs = rawLogs.filter((log) => log.command_tag === CommandTag.PARSE);
    const logsWithPlan = rawLogs.filter((log) => log.message.includes('plan:'));
    logsWithPlan.map((log) => {
      try {
        const { log_time: logTime, database_name: dbName, virtual_transaction_id: transactionId, message } = log;
        const queryId =
          log.query_id && log.query_id !== '0'
            ? log.query_id
            : this.getQueryIdFromTransaction(transactionId, [...bindLogs, ...parseLogs]);
        const [durationString, ...planObj] = message.split('plan:');
        const parsed = JSON.parse(planObj.join('plan:').trim());
        const { ['Query Text']: query, ...plan } = parsed;
        if (!query || ExcludedQueriesPrefixes.some((prefix) => query.trim().startsWith(prefix))) return undefined;
        const { traceId, spanId } = this.parseContext(query);
        const { duration, endTime } = this.parseDuration(logTime, durationString);
        if (!res[dbName]) res[dbName] = [];
        res[dbName].push(
          JSON.stringify({
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
              ['net.host.name']: this.configs.host,
              ['net.peer.name']: this.configs.host,
            },
            resource: {
              ['telemetry.sdk.language']: 'slow-query-log-collector',
              ['service.name']: this.configs.serviceName,
            },
          }),
        );
      } catch (e) {
        this.logger.log(`Parse failed: ${e}`);
      }
    });

    return res;
  }
}
