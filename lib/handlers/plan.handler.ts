import { Client } from 'pg';
import { Handler } from './handler.handler';
import { ExcludedQueriesPrefixes, Extensions, MetisSqlCollectorConfigs, PlanRow } from '../types';

export class PlanHandler extends Handler {
  public async createExtension(client: Client) {
    await client.query(this.queries.createExtension(Extensions.STAT_STATEMENTS));
    await client.query(this.queries.createExtension(Extensions.PG_STORE_PLANS));
  }

  public async enableFeature(client: Client) {
    return Promise.all(
      this.queries.enablePlans.map(async (setupQuery) => {
        try {
          await client.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  public async fetchData(client: Client, extension: string) {
    const { rows } = await client.query(this.queries.getPlans(this.lastLogTime, this.dbName));
    if (rows.length) {
      this.setLastLogTime(rows.at(-1));
      return this.parseLogs(rows);
    }

    return [];
  }

  private parseLogs(rawPlans: PlanRow[]) {
    return rawPlans
      .map((row) => {
        try {
          const { query, plan: planStr, last_call: startTime, duration: _duration, query_id: queryId } = row;
          const plan = JSON.parse(planStr);
          if (!query || ExcludedQueriesPrefixes.some((prefix) => query.trim().startsWith(prefix))) return undefined;
          const { traceId, spanId } = this.parseContext(query);
          const duration = Math.ceil(plan.Plan?.['Execution Time'] || _duration);
          return JSON.stringify({
            kind: 'SpanKind.CLIENT',
            context: {
              trace_id: traceId,
              span_id: spanId,
            },
            start_time: startTime,
            end_time: new Date(new Date(startTime).getTime() + duration).toISOString(),
            duration,
            attributes: {
              ['db.statement.metis']: query,
              ['db.statement.metis.plan']: planStr,
              ['db.name']: this.dbName,
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
