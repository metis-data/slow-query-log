import { Client } from 'pg';
import { Handler } from './handler.handler';
import { CommandTag, ExcludedQueriesPrefixes, Extensions, LogRow, MetisSqlCollectorConfigs } from '../types';

export class LogsHandler extends Handler {
  public async checkFeatureAvailability(client: Client) {
    let isAvailable = await super.checkFeatureAvailability(client);

    if (!isAvailable) return isAvailable;

    // Check if slow query log params that need to be configured by the user are well set
    await Promise.all(
      Object.entries(this.queries.checkAvailability).map(
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
    await client.query(this.queries.createExtension(extension));
  }

  public async enableFeature(client: Client) {
    return Promise.all(
      [...this.queries.enableLogs, this.queries.createLogFunction].map(async (setupQuery) => {
        try {
          await client.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  public async fetchData(client: Client, extension: string) {
    await client.query(this.queries.loadLogs(extension));
    const { rows } = await client.query(this.queries.getLogs(this.lastLogTime, this.byTrace, this.dbName));
    if (rows.length) {
      this.setLastLogTime(rows.at(-1));
      return this.parseLogs(rows);
    }

    return [];
  }

  private parseLogs(rawLogs: LogRow[]) {
    const bindLogs = rawLogs.filter((log) => log.command_tag === CommandTag.BIND);
    const parseLogs = rawLogs.filter((log) => log.command_tag === CommandTag.PARSE);
    const logsWithPlan = rawLogs.filter((log) => log.message.includes('plan:'));
    return logsWithPlan
      .map((log) => {
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
          this.logger.log(`Parse failed: ${e}`);
          return undefined;
        }
      })
      .filter((log) => log);
  }
}
