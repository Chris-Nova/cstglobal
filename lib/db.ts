// lib/db.ts
// Singleton PostgreSQL connection pool using `pg`
// Usage: import { query } from '@/lib/db'

import { Pool, PoolClient, QueryResult } from 'pg';

declare global {
  // Prevent multiple pool instances in Next.js dev hot-reload
  var _pgPool: Pool | undefined;
}

function createPool(): Pool {
  return new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production'
      ? { rejectUnauthorized: true }
      : false,
    max: 20,               // max connections in pool
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  });
}

const pool: Pool = global._pgPool ?? createPool();
if (process.env.NODE_ENV !== 'production') global._pgPool = pool;

// ── Simple query helper ──────────────────────────────────────
export async function query<T = any>(
  text: string,
  params?: unknown[]
): Promise<QueryResult<T>> {
  const start = Date.now();
  const result = await pool.query<T>(text, params);
  const duration = Date.now() - start;

  if (process.env.LOG_QUERIES === 'true') {
    console.log('[DB]', { text: text.slice(0, 80), duration, rows: result.rowCount });
  }
  return result;
}

// ── Transaction helper ───────────────────────────────────────
export async function withTransaction<T>(
  fn: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

export default pool;
