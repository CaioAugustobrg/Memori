export interface StorageAdapter {
  execute<T = any>(operation: string, binds?: any[]): Promise<T[]> | T[];
  commit(): Promise<void> | void;
  rollback(): Promise<void> | void;
  getDialect(): string;
  close(): Promise<void> | void;
}

export interface Migration {
  description: string;
  operation?: string;
  operations?: string[];
}

export abstract class BaseDriver {
  public abstract readonly requiresRollbackOnError: boolean;
  public abstract readonly migrations: Record<number, Migration[]>;

  constructor(protected readonly conn: StorageAdapter) {}

  public conversation!: any;
  public conversationMessage!: any;
  public conversationMessages!: any;
  public entity!: any;
  public entityFact!: any;
  public knowledgeGraph!: any;
  public process!: any;
  public processAttribute!: any;
  public schema!: any;
  public session!: any;
}
