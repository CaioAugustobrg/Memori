import type { Database } from 'better-sqlite3';
import { StorageAdapter } from '../base.js';
import { Registry } from '../registry.js';

function isSqliteConnection(conn: any): boolean {
  return conn && typeof conn.prepare === 'function';
}

export class SqliteAdapter implements StorageAdapter {
  private client: Database;

  constructor(conn: any) {
    this.client = conn;
    this.client.pragma('journal_mode = WAL');
    this.client.pragma('foreign_keys = ON');
  }

  public execute<T = any>(operation: string, binds: any[] = []): T[] {
    // Defensive check: Don't execute if the connection was closed by the main thread
    if (!this.client.open) {
      console.warn(
        `[Sqlite] Attempted to execute query on closed connection: ${operation.substring(0, 50)}...`
      );
      return [];
    }

    try {
      const stmt = this.client.prepare(operation);
      if (stmt.reader) {
        return stmt.all(...binds) as T[];
      } else {
        stmt.run(...binds);
        return [];
      }
    } catch (err) {
      throw err;
    }
  }

  public commit(): void {
    if (this.client.inTransaction) this.client.prepare('COMMIT').run();
  }

  public rollback(): void {
    if (this.client.inTransaction) this.client.prepare('ROLLBACK').run();
  }

  public getDialect(): string {
    return 'sqlite';
  }

  public close(): void {
    if (this.client.open) this.client.close();
  }
}

Registry.registerAdapter(isSqliteConnection, SqliteAdapter);
