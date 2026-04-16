import { randomUUID, createHash } from 'node:crypto';
import { StorageAdapter, BaseDriver } from '../base.js';
import { sqliteMigrations } from '../migrations/sqlite.js';
import { Registry } from '../registry.js';
import { CandidateFactRow } from '../../types/storage.js';

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

class ConversationMessage {
  constructor(private readonly conn: StorageAdapter) {}
  public create(
    conversationId: number | string,
    role: string,
    type: string | null,
    content: string
  ): void {
    this.conn.execute(
      `INSERT INTO memori_conversation_message(uuid, conversation_id, role, type, content) VALUES (?, ?, ?, ?, ?)`,
      [randomUUID(), conversationId, role, type, content]
    );
  }
}

class ConversationMessages {
  constructor(private readonly conn: StorageAdapter) {}
  public read(conversationId: number | string): Array<{ role: string; content: string }> {
    const results = this.conn.execute(
      `SELECT role, content FROM memori_conversation_message WHERE conversation_id = ?`,
      [conversationId]
    ) as any[];
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
    const existing = this.conn.execute(
      `SELECT c.id, COALESCE(MAX(m.date_created), c.date_created) as last_activity
         FROM memori_conversation c LEFT JOIN memori_conversation_message m ON m.conversation_id = c.id
        WHERE c.session_id = ? GROUP BY c.id, c.date_created`,
      [sessionId]
    ) as any[];

    if (existing.length > 0) {
      const existingRow = existing[0];
      const result = this.conn.execute(
        `SELECT (strftime('%s', 'now') - strftime('%s', ?)) / 60 as minutes_since_activity`,
        [existingRow.last_activity]
      ) as any[];
      if (result.length > 0 && result[0].minutes_since_activity <= timeoutMinutes) {
        return existingRow.id;
      }
    }

    this.conn.execute(
      `INSERT INTO memori_conversation(uuid, session_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), sessionId]
    );
    this.conn.commit();
    const newConv = this.conn.execute(`SELECT id FROM memori_conversation WHERE session_id = ?`, [
      sessionId,
    ]) as any[];
    return newConv.length > 0 ? newConv[0].id : null;
  }

  public update(id: number | string, summary: string): this {
    if (!summary) return this;
    this.conn.execute(`UPDATE memori_conversation SET summary = ? WHERE id = ?`, [summary, id]);
    this.conn.commit();
    return this;
  }
}

class Entity {
  constructor(private readonly conn: StorageAdapter) {}
  public create(externalId: string): number | string | null {
    this.conn.execute(
      `INSERT INTO memori_entity(uuid, external_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), externalId]
    );
    this.conn.commit();
    const res = this.conn.execute(`SELECT id FROM memori_entity WHERE external_id = ?`, [
      externalId,
    ]) as any[];
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
    if (!facts || facts.length === 0) return this;

    for (let i = 0; i < facts.length; i++) {
      const fact = facts[i];
      const embedding = factEmbeddings && i < factEmbeddings.length ? factEmbeddings[i] : [];

      // Prevent saving facts with empty embeddings which crash the Rust engine
      if (embedding.length === 0) continue;

      const embeddingFormatted = formatEmbeddingForDb(embedding);
      const uniq = generateUniq([fact]);

      this.conn.execute(
        `INSERT INTO memori_entity_fact(uuid, entity_id, content, content_embedding, num_times, date_last_time, uniq) 
         VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?) ON CONFLICT (entity_id, uniq) DO UPDATE SET num_times = memori_entity_fact.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
        [randomUUID(), entityId, fact, embeddingFormatted, uniq]
      );

      if (conversationId) {
        const factRow = this.conn.execute(
          `SELECT id FROM memori_entity_fact WHERE entity_id = ? AND uniq = ?`,
          [entityId, uniq]
        ) as any[];
        const factId = factRow.length > 0 ? factRow[0].id : null;

        if (factId) {
          this.conn.execute(
            `INSERT INTO memori_entity_fact_mention(uuid, entity_id, fact_id, conversation_id) VALUES (?, ?, ?, ?) ON CONFLICT (entity_id, fact_id, conversation_id) DO NOTHING`,
            [randomUUID(), entityId, factId, conversationId]
          );
        }
      }
    }
    return this;
  }

  // Stores a fact content with a zero-length embedding placeholder.
  // The fact will not appear in vector recall but is persisted for auditing/future use.
  // content_embedding schema is NOT NULL so we store an empty buffer rather than null.
  public createWithoutEmbedding(entityId: number | string, content: string): void {
    const uniq = generateUniq([content]);
    this.conn.execute(
      `INSERT INTO memori_entity_fact(uuid, entity_id, content, content_embedding, num_times, date_last_time, uniq)
       VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?)
       ON CONFLICT (entity_id, uniq) DO UPDATE SET num_times = memori_entity_fact.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
      [randomUUID(), entityId, content, Buffer.alloc(0), uniq]
    );
    this.conn.commit();
  }

  public getEmbeddings(entityId: string | number, limit: number = 1000) {
    const results = this.conn.execute(
      `SELECT id, content_embedding FROM memori_entity_fact WHERE entity_id = ? ORDER BY date_last_time DESC, num_times DESC, id DESC LIMIT ?`,
      [entityId, limit]
    ) as any[];
    // Ensure we only return rows with valid, non-empty embeddings
    return results
      .filter((r) => r.content_embedding && r.content_embedding.length > 0)
      .map((r) => ({
        id: r.id,
        content_embedding_b64: Buffer.from(r.content_embedding).toString('base64'),
      }));
  }

  public getFactsByIds(factIds: (string | number)[]): CandidateFactRow[] {
    if (!factIds || factIds.length === 0) return [];
    const placeholders = factIds.map(() => '?').join(',');
    const factRows = this.conn.execute(
      `SELECT id, content, date_created FROM memori_entity_fact WHERE id IN (${placeholders})`,
      factIds
    ) as any[];

    if (factRows.length === 0) return [];
    const factsById = new Map<number | string, CandidateFactRow>();
    const facts: CandidateFactRow[] = [];

    for (const row of factRows) {
      const fact = {
        id: row.id,
        content: row.content,
        date_created: row.date_created ? new Date(row.date_created).toISOString() : '',
        summaries: [],
      };
      facts.push(fact);
      factsById.set(row.id, fact);
    }

    const summaryRows = this.conn.execute(
      `SELECT m.fact_id, c.summary AS content, COALESCE(c.date_updated, c.date_created) AS date_created
         FROM memori_entity_fact_mention m JOIN memori_conversation c ON c.id = m.conversation_id
        WHERE m.fact_id IN (${placeholders}) AND c.summary IS NOT NULL AND c.summary <> ''`,
      factIds
    ) as any[];

    for (const row of summaryRows) {
      const fact = factsById.get(row.fact_id);
      if (fact) {
        fact.summaries!.push({
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
  public create(entityId: number | string, semanticTriples: any[]): this {
    if (!semanticTriples || semanticTriples.length === 0) return this;
    for (const triple of semanticTriples) {
      const subjName = triple.subject?.name || triple.subject_name;
      const subjType = triple.subject?.type || triple.subject_type || 'entity';
      const pred = triple.predicate;
      const objName = triple.object?.name || triple.object_name;
      const objType = triple.object?.type || triple.object_type || 'entity';

      const subjectUniq = generateUniq([subjName, subjType]);
      this.conn.execute(
        `INSERT INTO memori_subject(uuid, name, type, uniq) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), subjName, subjType, subjectUniq]
      );
      const subjRes = this.conn.execute(`SELECT id FROM memori_subject WHERE uniq = ?`, [
        subjectUniq,
      ]) as any[];
      const subjectId = subjRes.length > 0 ? subjRes[0].id : null;

      const predicateUniq = generateUniq([pred]);
      this.conn.execute(
        `INSERT INTO memori_predicate(uuid, content, uniq) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), pred, predicateUniq]
      );
      const predRes = this.conn.execute(`SELECT id FROM memori_predicate WHERE uniq = ?`, [
        predicateUniq,
      ]) as any[];
      const predicateId = predRes.length > 0 ? predRes[0].id : null;

      const objectUniq = generateUniq([objName, objType]);
      this.conn.execute(
        `INSERT INTO memori_object(uuid, name, type, uniq) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`,
        [randomUUID(), objName, objType, objectUniq]
      );
      const objRes = this.conn.execute(`SELECT id FROM memori_object WHERE uniq = ?`, [
        objectUniq,
      ]) as any[];
      const objectId = objRes.length > 0 ? objRes[0].id : null;

      if (entityId && subjectId && predicateId && objectId) {
        this.conn.execute(
          `INSERT INTO memori_knowledge_graph(uuid, entity_id, subject_id, predicate_id, object_id, num_times, date_last_time) 
           VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP) ON CONFLICT (entity_id, subject_id, predicate_id, object_id) DO UPDATE SET num_times = memori_knowledge_graph.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
          [randomUUID(), entityId, subjectId, predicateId, objectId]
        );
      }
    }
    this.conn.commit();
    return this;
  }
}

class Process {
  constructor(private readonly conn: StorageAdapter) {}
  public create(externalId: string): number | string | null {
    this.conn.execute(
      `INSERT INTO memori_process(uuid, external_id) VALUES (?, ?) ON CONFLICT DO NOTHING`,
      [randomUUID(), externalId]
    );
    this.conn.commit();
    const res = this.conn.execute(`SELECT id FROM memori_process WHERE external_id = ?`, [
      externalId,
    ]) as any[];
    return res.length > 0 ? res[0].id : null;
  }
}

class ProcessAttribute {
  constructor(private readonly conn: StorageAdapter) {}
  public create(processId: number | string, attributes: string[]): this {
    if (!attributes || attributes.length === 0) return this;
    for (const attribute of attributes) {
      const uniq = generateUniq([attribute]);
      this.conn.execute(
        `INSERT INTO memori_process_attribute(uuid, process_id, content, num_times, date_last_time, uniq) 
         VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, ?) ON CONFLICT (process_id, uniq) DO UPDATE SET num_times = memori_process_attribute.num_times + 1, date_last_time = CURRENT_TIMESTAMP`,
        [randomUUID(), processId, attribute, uniq]
      );
    }
    this.conn.commit();
    return this;
  }
}

class Session {
  constructor(private readonly conn: StorageAdapter) {}
  public create(
    uuid: string,
    entityId: number | string,
    processId: number | string
  ): number | string | null {
    this.conn.execute(
      `INSERT INTO memori_session(uuid, entity_id, process_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING`,
      [uuid, entityId, processId]
    );
    this.conn.commit();
    const res = this.conn.execute(`SELECT id FROM memori_session WHERE uuid = ?`, [uuid]) as any[];
    return res.length > 0 ? res[0].id : null;
  }
}

class SchemaVersion {
  constructor(private readonly conn: StorageAdapter) {}
  public create(num: number): void {
    this.conn.execute(`INSERT INTO memori_schema_version(num) VALUES (?)`, [num]);
  }
  public delete(): void {
    this.conn.execute(`DELETE FROM memori_schema_version`);
  }
  public read(): number | null {
    const res = this.conn.execute<{ num: number | string }>(
      `SELECT num FROM memori_schema_version`
    ) as any[];
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
    this.conversation = new Conversation(conn, this.conversationMessage, this.conversationMessages);
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
