import type { PoolClient, Pool } from 'pg';
import { StorageAdapter, SqlBindValue } from '../base.js';
import { Registry } from '../registry.js';

function isPostgresConnection(conn: unknown): boolean {
  return (
    conn != null &&
    typeof (conn as Pool).query === 'function' &&
    typeof (conn as { execute?: unknown }).execute !== 'function'
  );
}

export class PostgresAdapter implements StorageAdapter {
  private client: PoolClient | Pool;
  constructor(conn: unknown) {
    this.client = conn as PoolClient | Pool;
  }

  public async execute<T = Record<string, unknown>>(
    operation: string,
    binds: SqlBindValue[] = []
  ): Promise<T[]> {
    const result = await this.client.query(operation, binds);
    return result.rows as T[];
  }

  public async begin(): Promise<void> {
    await this.client.query('BEGIN');
  }
  public async commit(): Promise<void> {
    await this.client.query('COMMIT');
  }
  public async rollback(): Promise<void> {
    await this.client.query('ROLLBACK');
  }
  public getDialect(): string {
    return 'postgresql';
  }

  public async close(): Promise<void> {
    if ('release' in this.client) {
      this.client.release();
    } else {
      await this.client.end();
    }
  }
}

Registry.registerAdapter(isPostgresConnection, PostgresAdapter);
