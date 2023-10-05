import { PlanHandler } from './handlers/plan.handler';
import { LogsHandler } from './handlers/logs.handler';

export enum Extensions {
  PG_STORE_PLANS = 'pg_store_plans',
  FILE_FDW = 'file_fdw',
  LOG_FDW = 'log_fdw',
  STAT_STATEMENTS = 'pg_stat_statements',
}

export type MetisSqlCollectorOptions = {
  connectionStrings?: string | string[];
  metisApiKey?: string;
  logFetchInterval?: number;
  metisExportUrl?: string;
  serviceName?: string;
  exportResults?: boolean;
  logger?: any;
  debug?: boolean;
  autoRun?: boolean;
};

export type MetisHandlerConfigs = {
  connectionString?: string;
  host?: string;
  extension?: string;
  serviceName?: string;
};

const DefaultProps = {
  connectionStrings: process.env.DATABASE_URL,
  metisApiKey: process.env.METIS_API_KEY,
  logFetchInterval: parseInt(process.env.LOG_FETCH_INTERVAL, 10) || 60_000,
  metisExportUrl: process.env.METIS_EXPORTER_URL || 'https://ingest.metisdata.io/',
  serviceName: process.env.METIS_SERVICE_NAME || 'default',
  debug: process.env.METIS_DEBUG === 'true',
  autoRun: false,
  exportResults: true,
  logger: { log: console.log, info: console.log, error: console.error },
};

export function getProps(props: MetisSqlCollectorOptions) {
  const logger = props.logger || DefaultProps.logger;
  if (!logger.log && logger.info) logger.log = logger.info;
  if (!props.connectionStrings && !DefaultProps.connectionStrings) {
    logger.error('connection string must be provided');
  }
  if (props.exportResults && (!props.metisApiKey || DefaultProps.metisApiKey)) {
    logger.error('with exportResults enabled metis api key must be provided');
  }
  return {
    connectionStrings: props.connectionStrings || DefaultProps.connectionStrings,
    logFetchInterval: props.logFetchInterval || DefaultProps.logFetchInterval,
    metisExportUrl: props.metisExportUrl || DefaultProps.metisExportUrl,
    serviceName: props.serviceName || DefaultProps.serviceName,
    debug: props.debug || DefaultProps.debug,
    autoRun: props.autoRun || DefaultProps.autoRun,
    exportResults: props.exportResults || DefaultProps.exportResults,
    logger,
  };
}

export const AWS_CONTEXT = 'AWS Context';
export const X_RAY = 'X Ray Trace Id';
export const REQUEST_ID = 'Request Id';
export const RESPONSE = 'Response';
export const X_RAY_HEADER = 'x-amzn-trace-id';
export const REQUEST_ID_HEADER = 'x-amzn-requestid';

export type Response = {
  statusCode: number;
  json?: any;
  text: string;
  headers: { [key: string]: string | string[] };
  error?: Error;
};

export type LogRow = {
  log_time: string;
  database_name: string;
  command_tag: string;
  virtual_transaction_id: string;
  message: string;
  detail?: string;
  internal_query?: string;
  query_id?: string;
};

export type PlanRow = {
  query: string;
  plan: any;
  last_call: string;
  duration: string;
  query_id: string;
};

export enum CommandTag {
  SELECT = 'SELECT',
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE',
  BIND = 'BIND',
  PARSE = 'PARSE',
}

export const ExcludedQueriesPrefixes = ['/* metis */'];

export const getHandler = {
  [Extensions.PG_STORE_PLANS]: (logger: any, configs: MetisHandlerConfigs) => new PlanHandler(logger, configs),
  [Extensions.FILE_FDW]: (logger: any, configs: MetisHandlerConfigs) => new LogsHandler(logger, configs),
  [Extensions.LOG_FDW]: (logger: any, configs: MetisHandlerConfigs) => new LogsHandler(logger, configs),
};

export function toObj(arr: any[]) {
  return arr.reduce((acc, obj) => ({ ...acc, ...obj }), {});
}
