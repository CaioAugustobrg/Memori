import type { PoolClient, Pool } from 'pg';
import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

/**
 * Checks if the provided connection is a `pg` Pool or PoolClient.
 */
function isPostgresConnection(conn: any): boolean {
  // node-postgres connections typically expose 'query' and 'release' (if client)
  // or 'query' and 'connect' (if pool)
  return conn && typeof conn.query === 'function';
}

export class PostgresAdapter implements StorageAdapter {
  private client: PoolClient | Pool;

  constructor(conn: any) {
    this.client = conn;
  }

  public async execute<T = any>(operation: string, binds: any[] = []): Promise<T[]> {
    // In node-postgres, placeholders are $1, $2, etc., but our raw migrations and driver
    // might use %s or ? depending on how we write them. We'll write the driver to use $X syntax.
    try {
      const result = await this.client.query(operation, binds);
      return result.rows;
    } catch (err) {
      throw err;
    }
  }

  public async commit(): Promise<void> {
    await this.execute('COMMIT');
  }

  public async rollback(): Promise<void> {
    await this.execute('ROLLBACK');
  }

  public getDialect(): string {
    return 'postgresql';
  }

  public async close(): Promise<void> {
    // If it's a client checked out from a pool, release it.
    if ('release' in this.client && typeof this.client.release === 'function') {
      this.client.release();
    } else if ('end' in this.client && typeof this.client.end === 'function') {
      await this.client.end();
    }
  }
}

// Automatically register this adapter
Registry.registerAdapter(isPostgresConnection, PostgresAdapter);
