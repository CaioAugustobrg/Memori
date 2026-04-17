import { randomUUID, createHash } from 'node:crypto';
import { StorageAdapter, BaseDriver } from '../base.js';
import { sqliteMigrations } from '../migrations/sqlite.js';
import { Registry } from '../registry.js';
import { CandidateFactRow, SemanticTriplePayload } from '../../types/storage.js';

function generateUniq(inputs: string[]): string {
  const hash = createHash('sha256');
  for (const input of inputs) {
    hash.update(input);
  }
  return hash.digest('hex');
}

function formatEmbeddingForDb(embedding: number[]): Buffer {
  const float32Array = new Float32Array(embedding);
  return Buffer.from(float32Array.buffer);
}

function execSync<T = Record<string, unknown>>(
  conn: StorageAdapter,
  sql: string,
  binds?: (string | number | boolean | null | Buffer | Uint8Array | (string | number)[])[]
) {
  return conn.execute<T>(sql, binds) as T[];
}

class ConversationMessage {
  constructor(private readonly conn: StorageAdapter) {}
  public create(
    conversationId: number | string,
    role: string,
    type: string | null,
    content: string
  ): void {
    void this.conn.execute(
      `INSERT INTO memori_conversation_message(uuid, conversation_id, role, type, content) VALUES (?, ?, ?, ?, ?)`,
      [randomUUID(), conversationId, role, type, content]
    );
  }
}

class ConversationMessages {
  constructor(private readonly conn: StorageAdapter) {}
  public read(conversationId: number | string): Array<{ role: string; content: string }> {
    const results = execSync<{ role: string; content: string }>(
      this.conn,
      `SELECT role, content FROM memori_conversation_message WHERE conversation_id = ?`,
      [conversationId]
    );
    return results.map((row) => ({ content: row.content, role: row.role }));
  }
}

class Conversation {
  constructor(
    private readonly conn: StorageAdapter,
    public readonly message: ConversationMessage,
    public readonly messages: ConversationMessages
  ) {}

  public create(sessionId: number | string, timeoutMinutes: number): number | string | null {
    const existing = execSync<{ id: number | string; last_activity: string }>(
      this.conn,
      `SELECT c.id, COALESCE(MAX(m.date_created), c.date_created) as last_activity
         FROM memori_conversation c LEFT JOIN memori_conversation_message m ON m.conversation_id = c.id
        WHERE c.session_id = ? GROUP BY c.id, c.date_created`,
      [sessionId]
    );

    if (existing.length > 0) {
      const existingRow = existing[0];
      const result = execSync<{ minutes_since_activity: number }>(
        this.conn,
        `SELECT (strftime('%s', 'now') - strftime('%s', ?)) / 60 as minutes_since_activity`,
        [existingRow.last_activity]
      );
      if (result.length > 0 && result[0].minutes_since_activity <= timeoutMinutes) {
        return existingRow.id;
      }
    }

    void this.conn.execute(
      `INSERT INTO memori_conversation(uuid, session_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), sessionId]
    );
    const newConv = execSync<{ id: number | string }>(
      this.conn,
      `SELECT id FROM memori_conversation WHERE session_id = ?`,
      [sessionId]
    );
    return newConv.length > 0 ? newConv[0].id : null;
  }

  public update(id: number | string, summary: string): this {
    if (!summary) return this;
    void this.conn.execute(`UPDATE memori_conversation SET summary = ? WHERE id = ?`, [
      summary,
      id,
    ]);
    return this;
  }
}

class Entity {
  constructor(private readonly conn: StorageAdapter) {}
  public create(externalId: string | number): number | string | null {
    void this.conn.execute(
      `INSERT INTO memori_entity(uuid, external_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), externalId]
    );
    const res = execSync<{ id: number | string }>(
      this.conn,
      `SELECT id FROM memori_entity WHERE external_id = ?`,
      [externalId]
    );
    return res.length > 0 ? res[0].id : null;
  }
}

class EntityFact {
  constructor(private readonly conn: StorageAdapter) {}

  public create(
    entityId: number | string,
    facts: string[],
    factEmbeddings?: number[][],
    conversationId?: number | string | null
  ): this {
    if (facts.length === 0) return this;

    for (let i = 0; i < facts.length; i++) {
      const fact = facts[i];
      const embedding = factEmbeddings && i < factEmbeddings.length ? factEmbeddings[i] : [];

      if (embedding.length === 0) continue;

      const embeddingFormatted = formatEmbeddingForDb(embedding);
      const uniq = generateUniq([fact]);

      void this.conn.execute(
        `INSERT INTO memori_entity_fact(uuid, entity_id, content, content_embedding, num_times, date_last_time, uniq)
         VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?) ON CONFLICT (entity_id, uniq) DO UPDATE SET num_times = memori_entity_fact.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
        [randomUUID(), entityId, fact, embeddingFormatted, uniq]
      );

      if (conversationId) {
        const factRow = execSync<{ id: number | string }>(
          this.conn,
          `SELECT id FROM memori_entity_fact WHERE entity_id = ? AND uniq = ?`,
          [entityId, uniq]
        );
        const factId = factRow.length > 0 ? factRow[0].id : null;

        if (factId) {
          void this.conn.execute(
            `INSERT INTO memori_entity_fact_mention(uuid, entity_id, fact_id, conversation_id) VALUES (?, ?, ?, ?) ON CONFLICT (entity_id, fact_id, conversation_id) DO NOTHING`,
            [randomUUID(), entityId, factId, conversationId]
          );
        }
      }
    }
    return this;
  }

  public createWithoutEmbedding(entityId: number | string, content: string): void {
    const uniq = generateUniq([content]);
    void this.conn.execute(
      `INSERT INTO memori_entity_fact(uuid, entity_id, content, content_embedding, num_times, date_last_time, uniq)
       VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?)
       ON CONFLICT (entity_id, uniq) DO UPDATE SET num_times = memori_entity_fact.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
      [randomUUID(), entityId, content, Buffer.alloc(0), uniq]
    );
  }

  public getEmbeddings(entityId: string | number, limit: number = 1000) {
    const results = execSync<{ id: number | string; content_embedding: Buffer | null }>(
      this.conn,
      `SELECT id, content_embedding FROM memori_entity_fact WHERE entity_id = ? ORDER BY date_last_time DESC, num_times DESC, id DESC LIMIT ?`,
      [entityId, limit]
    );
    return results
      .filter(
        (r): r is { id: number | string; content_embedding: Buffer } =>
          r.content_embedding != null && r.content_embedding.length > 0
      )
      .map((r) => ({
        id: r.id,
        content_embedding_b64: Buffer.from(r.content_embedding).toString('base64'),
      }));
  }

  public getFactsByIds(factIds: (string | number)[]): CandidateFactRow[] {
    if (factIds.length === 0) return [];
    const placeholders = factIds.map(() => '?').join(',');
    const factRows = execSync<{ id: number | string; content: string; date_created: string }>(
      this.conn,
      `SELECT id, content, date_created FROM memori_entity_fact WHERE id IN (${placeholders})`,
      factIds
    );

    if (factRows.length === 0) return [];
    const factsById = new Map<number | string, CandidateFactRow>();
    const facts: CandidateFactRow[] = [];

    for (const row of factRows) {
      const fact: CandidateFactRow = {
        id: row.id,
        content: row.content,
        date_created: row.date_created ? new Date(row.date_created).toISOString() : '',
        summaries: [],
      };
      facts.push(fact);
      factsById.set(row.id, fact);
    }

    const summaryRows = execSync<{
      fact_id: number | string;
      content: string;
      date_created: string;
    }>(
      this.conn,
      `SELECT m.fact_id, c.summary AS content, COALESCE(c.date_updated, c.date_created) AS date_created
         FROM memori_entity_fact_mention m JOIN memori_conversation c ON c.id = m.conversation_id
        WHERE m.fact_id IN (${placeholders}) AND c.summary IS NOT NULL AND c.summary <> ''`,
      factIds
    );

    for (const row of summaryRows) {
      const fact = factsById.get(row.fact_id);
      if (fact) {
        (fact.summaries ??= []).push({
          content: row.content,
          date_created: row.date_created ? new Date(row.date_created).toISOString() : '',
        });
      }
    }
    return facts;
  }
}

class KnowledgeGraph {
  constructor(private readonly conn: StorageAdapter) {}
  public create(entityId: number | string, semanticTriples: SemanticTriplePayload[]): this {
    if (semanticTriples.length === 0) return this;
    for (const triple of semanticTriples) {
      const subjName = typeof triple.subject === 'object' ? triple.subject.name : triple.subject;
      const subjType = typeof triple.subject === 'object' ? triple.subject.type : 'entity';
      const pred = triple.predicate;
      const objName = typeof triple.object === 'object' ? triple.object.name : triple.object;
      const objType = typeof triple.object === 'object' ? triple.object.type : 'entity';

      const subjectUniq = generateUniq([subjName, subjType]);
      void this.conn.execute(
        `INSERT INTO memori_subject(uuid, name, type, uniq) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), subjName, subjType, subjectUniq]
      );
      const subjRes = execSync<{ id: number | string }>(
        this.conn,
        `SELECT id FROM memori_subject WHERE uniq = ?`,
        [subjectUniq]
      );
      const subjectId = subjRes.length > 0 ? subjRes[0].id : null;

      const predicateUniq = generateUniq([pred]);
      void this.conn.execute(
        `INSERT INTO memori_predicate(uuid, content, uniq) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), pred, predicateUniq]
      );
      const predRes = execSync<{ id: number | string }>(
        this.conn,
        `SELECT id FROM memori_predicate WHERE uniq = ?`,
        [predicateUniq]
      );
      const predicateId = predRes.length > 0 ? predRes[0].id : null;

      const objectUniq = generateUniq([objName, objType]);
      void this.conn.execute(
        `INSERT INTO memori_object(uuid, name, type, uniq) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), objName, objType, objectUniq]
      );
      const objRes = execSync<{ id: number | string }>(
        this.conn,
        `SELECT id FROM memori_object WHERE uniq = ?`,
        [objectUniq]
      );
      const objectId = objRes.length > 0 ? objRes[0].id : null;

      if (entityId && subjectId && predicateId && objectId) {
        void this.conn.execute(
          `INSERT INTO memori_knowledge_graph(uuid, entity_id, subject_id, predicate_id, object_id, num_times, date_last_time)
           VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP) ON CONFLICT (entity_id, subject_id, predicate_id, object_id) DO UPDATE SET num_times = memori_knowledge_graph.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
          [randomUUID(), entityId, subjectId, predicateId, objectId]
        );
      }
    }
    return this;
  }
}

class Process {
  constructor(private readonly conn: StorageAdapter) {}
  public create(externalId: string | number): number | string | null {
    void this.conn.execute(
      `INSERT INTO memori_process(uuid, external_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), externalId]
    );
    const res = execSync<{ id: number | string }>(
      this.conn,
      `SELECT id FROM memori_process WHERE external_id = ?`,
      [externalId]
    );
    return res.length > 0 ? res[0].id : null;
  }
}

class ProcessAttribute {
  constructor(private readonly conn: StorageAdapter) {}
  public create(processId: number | string, attributes: string[]): this {
    if (attributes.length === 0) return this;
    for (const attribute of attributes) {
      const uniq = generateUniq([attribute]);
      void this.conn.execute(
        `INSERT INTO memori_process_attribute(uuid, process_id, content, num_times, date_last_time, uniq)
         VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, ?) ON CONFLICT (process_id, uniq) DO UPDATE SET num_times = memori_process_attribute.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
        [randomUUID(), processId, attribute, uniq]
      );
    }
    return this;
  }
}

class Session {
  constructor(private readonly conn: StorageAdapter) {}
  public create(
    uuid: string | number | null,
    entityId: number | string | null,
    processId: number | string | null
  ): number | string | null {
    void this.conn.execute(
      `INSERT INTO memori_session(uuid, entity_id, process_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
      [uuid, entityId, processId]
    );
    const res = execSync<{ id: number | string }>(
      this.conn,
      `SELECT id FROM memori_session WHERE uuid = ?`,
      [uuid]
    );
    return res.length > 0 ? res[0].id : null;
  }
}

class SchemaVersion {
  constructor(private readonly conn: StorageAdapter) {}
  public create(num: number): void {
    void this.conn.execute(`INSERT INTO memori_schema_version(num) VALUES (?)`, [num]);
  }
  public delete(): void {
    void this.conn.execute(`DELETE FROM memori_schema_version`);
  }
  public read(): number | null {
    const res = execSync<{ num: number | string }>(
      this.conn,
      `SELECT num FROM memori_schema_version`
    );
    return res.length > 0 ? Number(res[0].num) : null;
  }
}

class Schema {
  public readonly version: SchemaVersion;
  constructor(conn: StorageAdapter) {
    this.version = new SchemaVersion(conn);
  }
}

export class SqliteDriver extends BaseDriver {
  public readonly requiresRollbackOnError = false;
  public readonly migrations = sqliteMigrations;

  constructor(conn: StorageAdapter) {
    super(conn);
    this.conversationMessage = new ConversationMessage(conn);
    this.conversationMessages = new ConversationMessages(conn);
    this.conversation = new Conversation(
      conn,
      this.conversationMessage as ConversationMessage,
      this.conversationMessages as ConversationMessages
    );
    this.entity = new Entity(conn);
    this.entityFact = new EntityFact(conn);
    this.knowledgeGraph = new KnowledgeGraph(conn);
    this.process = new Process(conn);
    this.processAttribute = new ProcessAttribute(conn);
    this.schema = new Schema(conn);
    this.session = new Session(conn);
  }
}

Registry.registerDriver('sqlite', SqliteDriver);
