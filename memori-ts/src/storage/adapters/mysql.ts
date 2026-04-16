import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

/**
 * Checks if the connection is a mysql2 Promise Pool or Connection.
 */
function isMysqlConnection(conn: any): boolean {
  return conn && typeof conn.execute === 'function' && typeof conn.query === 'function';
}

export class MysqlAdapter implements StorageAdapter {
  private client: any; // Using any because Pool and Connection have similar interfaces in mysql2/promise

  constructor(conn: any) {
    this.client = conn;
  }

  public async execute<T = any>(operation: string, binds: any[] = []): Promise<T[]> {
    try {
      // mysql2 execute returns [rows, fields]
      const [rows] = await this.client.execute(operation, binds);
      return Array.isArray(rows) ? (rows as T[]) : []; // Returns empty array for INSERT/UPDATE results
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

// Automatically register this adapter
Registry.registerAdapter(isMysqlConnection, MysqlAdapter);
