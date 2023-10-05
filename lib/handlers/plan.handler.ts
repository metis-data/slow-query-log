import { Client } from 'pg';
import { Handler } from './handler.handler';
import { ExcludedQueriesPrefixes, Extensions, PlanRow } from '../types';
import { QUERIES } from '../queries';

export class PlanHandler extends Handler {
  public async createExtension(client: Client) {
    await client.query(QUERIES.createExtension(Extensions.STAT_STATEMENTS));
    await client.query(QUERIES.createExtension(Extensions.PG_STORE_PLANS));
  }

  public async enableFeature(client: Client) {
    return Promise.all(
      QUERIES.enablePlans.map(async (setupQuery) => {
        try {
          await client.query(setupQuery);
        } catch (e) {}
      }),
    );
  }

  public async fetchData() {
    try {
      const client = await this.getDbClient(this.configs.connectionString);
      const { rows } = await client.query(QUERIES.getPlans(this.lastLogTime));

      if (rows.length) {
        this.setLastLogTime(rows[0]);
        return { [this.configs.host]: this.parseLogs(rows) };
      }

      return { [this.configs.host]: 'No data' };
    } catch (e) {
      return { [this.configs.host]: `Failed to fetch data: ${e.message}` };
    }
  }

  private parseLogs(rawPlans: PlanRow[]) {
    const res = {};
    rawPlans.map((row) => {
      try {
        const {
          database_name: dbName,
          query,
          plan: planStr,
          last_call: startTime,
          duration: _duration,
          query_id: queryId,
        } = row;
        const plan = JSON.parse(planStr);
        if (!query || ExcludedQueriesPrefixes.some((prefix) => query.trim().startsWith(prefix))) return undefined;
        const { traceId, spanId } = this.parseContext(query);
        const duration = Math.ceil(plan.Plan?.['Execution Time'] || _duration);
        if (!res[dbName]) res[dbName] = [];
        res[dbName].push(
          JSON.stringify({
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
