import type { PoolClient, Pool } from 'pg';
import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

function isPostgresConnection(conn: any): boolean {
  // Ensure it's not MySQL by checking that .execute is missing
  return conn && typeof conn.query === 'function' && typeof conn.execute !== 'function';
}

export class PostgresAdapter implements StorageAdapter {
  private client: PoolClient | Pool;
  constructor(conn: any) {
    this.client = conn;
  }

  public async execute<T = any>(operation: string, binds: any[] = []): Promise<T[]> {
    const result = await this.client.query(operation, binds);
    return result.rows;
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
    if ('release' in this.client) {
      (this.client as any).release();
    } else if ('end' in this.client) {
      await (this.client as any).end();
    }
  }
}

Registry.registerAdapter(isPostgresConnection, PostgresAdapter);
