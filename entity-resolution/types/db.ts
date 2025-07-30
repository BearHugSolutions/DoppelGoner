// types/db.ts

import { Pool, PoolConfig } from 'pg';

// Database connection configuration
export interface DbConfig extends PoolConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean | object;
}

// Database client configuration and helpers (assuming these remain the same)
export function getDbConfig(): DbConfig {
  return {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'dataplatform',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || '',
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  };
}

let pool: Pool | null = null;

export function getDbPool(): Pool {
  if (!pool) {
    pool = new Pool(getDbConfig());
    pool.on('connect', () => {
      console.log('New database connection established');
    });
    pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
      process.exit(-1);
    });
  }
  return pool;
}

export async function queryOne<T = any>(
  dbQuery: string,
  params: any[] = []
): Promise<T | null> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(dbQuery, params);
    return result.rows[0] || null;
  } finally {
    client.release();
  }
}

export async function query<T = any>(
  dbQuery: string,
  params: any[] = []
): Promise<T[]> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(dbQuery, params);
    return result.rows;
  } finally {
    client.release();
  }
}

export async function execute(
  dbQuery: string,
  params: any[] = []
): Promise<number> {
  const client = await getDbPool().connect();
  try {
    const result = await client.query(dbQuery, params);
    return result.rowCount || 0;
  } finally {
    client.release();
  }
}

export async function withTransaction<T>(
  callback: (client: any) => Promise<T>
): Promise<T> {
  const client = await getDbPool().connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
