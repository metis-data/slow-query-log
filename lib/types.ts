import { Client, Pool } from 'pg';

export type MetisSqlCollectorOptions = {
  connections?: Pool[] | Client[];
  connectionString?: string;
  metisApiKey?: string;
  logFetchInterval?: number;
  metisExportUrl?: string;
  serviceName?: string;
};

export type MetisSqlCollectorConfigs = {
  dbHost?: string;
  serviceName?: string;
};

const DefaultProps = {
  connections: [],
  connectionString: process.env.CONNECTION_STRING,
  metisApiKey: process.env.METIS_API_KEY,
  logFetchInterval: parseInt(process.env.LOG_FETCH_INTERVAL, 10) || 120_000,
  metisExportUrl: process.env.METIS_EXPORTER_URL || 'https://ingest.metisdata.io/',
  serviceName: process.env.METIS_SERVICE_NAME || '',
};

export function getProps(props: MetisSqlCollectorOptions) {
  if (!props.connectionString && !DefaultProps.connectionString) {
    console.log('connection string is missing');
    // TODO handle error
  }
  if (!props.metisApiKey && !DefaultProps.metisApiKey) {
    console.log('api key is missing');
    // TODO handle error
  }
  return {
    connections: props.connections || DefaultProps.connections,
    connectionString: props.connectionString || DefaultProps.connectionString,
    metisApiKey: props.metisApiKey || DefaultProps.metisApiKey,
    logFetchInterval: props.logFetchInterval || DefaultProps.logFetchInterval,
    metisExportUrl: props.metisExportUrl || DefaultProps.metisExportUrl,
    serviceName: props.serviceName || DefaultProps.serviceName,
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
