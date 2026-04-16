import type { Database } from 'better-sqlite3';
import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

function isSqliteConnection(conn: any): boolean {
  // Check for '.pragma' to distinguish from MySQL
  return conn && typeof conn.prepare === 'function' && typeof conn.pragma === 'function';
}

export class SqliteAdapter implements StorageAdapter {
  private client: Database;
  constructor(conn: any) {
    this.client = conn;
    this.client.pragma('journal_mode = WAL');
    this.client.pragma('foreign_keys = ON');
  }

  public execute<T = any>(operation: string, binds: any[] = []): T[] {
    if (!this.client.open) return [];
    try {
      const stmt = this.client.prepare(operation);
      return stmt.reader ? (stmt.all(...binds) as T[]) : (stmt.run(...binds), []);
    } catch (err) {
      throw err;
    }
  }

  public commit(): void {
    if (this.client.open && this.client.inTransaction) this.client.prepare('COMMIT').run();
  }
  public rollback(): void {
    if (this.client.open && this.client.inTransaction) this.client.prepare('ROLLBACK').run();
  }
  public getDialect(): string {
    return 'sqlite';
  }
  public close(): void {
    if (this.client.open) this.client.close();
  }
}

Registry.registerAdapter(isSqliteConnection, SqliteAdapter);
