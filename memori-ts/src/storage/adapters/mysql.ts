import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

/**
 * Checks if the connection is a mysql2 Promise Pool or Connection.
 */
function isMysqlConnection(conn: any): boolean {
  return conn && typeof conn.execute === 'function' && typeof conn.query === 'function';
}

export class MysqlAdapter implements StorageAdapter {
  private client: any;

  constructor(conn: any) {
    this.client = conn;
  }

  public async execute<T = any>(operation: string, binds: any[] = []): Promise<T[]> {
    try {
      const [rows] = await this.client.execute(operation, binds);
      return Array.isArray(rows) ? (rows as T[]) : [];
    } catch (err) {
      throw err;
    }
  }

  public async commit(): Promise<void> {
    await this.client.query('COMMIT');
  }
  public async rollback(): Promise<void> {
    await this.client.query('ROLLBACK');
  }
  public getDialect(): string {
    return 'mysql';
  }

  public async close(): Promise<void> {
    if ('end' in this.client && typeof this.client.end === 'function') {
      await this.client.end();
    } else if ('release' in this.client && typeof this.client.release === 'function') {
      this.client.release();
    }
  }
}

Registry.registerAdapter(isMysqlConnection, MysqlAdapter);
