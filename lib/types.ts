import { Client } from 'pg';

export type MetisSqlCollectorOptions = {
  connectionString?: string;
  client?: Client;
  metisApiKey?: string;
  logFetchInterval?: number;
  metisExportUrl?: string;
  serviceName?: string;
  dbName?: string;
  byTrace?: boolean;
  logger?: any;
  debug?: boolean;
  autoRun?: boolean;
};

export type MetisSqlCollectorConfigs = {
  dbHost?: string;
  serviceName?: string;
};

const DefaultProps = {
  connectionString: process.env.DATABASE_URL,
  metisApiKey: process.env.METIS_API_KEY,
  logFetchInterval: parseInt(process.env.LOG_FETCH_INTERVAL, 10) || 60_000,
  metisExportUrl: process.env.METIS_EXPORTER_URL || 'https://ingest.metisdata.io/',
  serviceName: process.env.METIS_SERVICE_NAME || 'default',
  debug: process.env.METIS_DEBUG === 'true',
  autoRun: false,
  byTrace: true,
  dbName: process.env.DB_NAME || '',
  logger: { log: console.log, error: console.error },
};

export function getProps(props: MetisSqlCollectorOptions) {
  const logger = props.logger || DefaultProps.logger;
  if (!props.connectionString && !DefaultProps.connectionString && !props.client) {
    logger.error('connection string or database client must be provided');
  }
  if (!props.metisApiKey && !DefaultProps.metisApiKey) {
    logger.error('metis api key is missing');
  }
  return {
    connectionString: props.connectionString || DefaultProps.connectionString,
    metisApiKey: props.metisApiKey || DefaultProps.metisApiKey,
    logFetchInterval: props.logFetchInterval || DefaultProps.logFetchInterval,
    metisExportUrl: props.metisExportUrl || DefaultProps.metisExportUrl,
    serviceName: props.serviceName || DefaultProps.serviceName,
    debug: props.debug || DefaultProps.debug,
    autoRun: props.autoRun || DefaultProps.autoRun,
    byTrace: props.byTrace === false ? false : DefaultProps.byTrace,
    dbName: props.dbName || DefaultProps.dbName,
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
