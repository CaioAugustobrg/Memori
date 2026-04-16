import { randomUUID, createHash } from 'node:crypto';
import { StorageAdapter, BaseDriver } from '../base.js';
import { mysqlMigrations } from '../migrations/mysql.js';
import { Registry } from '../registry.js';
import { CandidateFactRow } from '../../types/storage.js';

// --- Utility Functions ---

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

// --- Component Classes ---

class ConversationMessage {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(
    conversationId: number | string,
    role: string,
    type: string | null,
    content: string
  ): Promise<void> {
    await this.conn.execute(
      `
      INSERT INTO memori_conversation_message(
          uuid, conversation_id, role, type, content
      ) VALUES (?, ?, ?, ?, ?)
      `,
      [randomUUID(), conversationId, role, type, content]
    );
  }
}

class ConversationMessages {
  constructor(private readonly conn: StorageAdapter) {}

  public async read(
    conversationId: number | string
  ): Promise<Array<{ role: string; content: string }>> {
    const results = await this.conn.execute(
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

  public async create(
    sessionId: number | string,
    timeoutMinutes: number
  ): Promise<number | string | null> {
    const existing = await this.conn.execute(
      `
      SELECT c.id,
             COALESCE(MAX(m.date_created), c.date_created) as last_activity
        FROM memori_conversation c
        LEFT JOIN memori_conversation_message m ON m.conversation_id = c.id
       WHERE c.session_id = ?
       GROUP BY c.id, c.date_created
      `,
      [sessionId]
    );

    if (existing.length > 0) {
      const existingRow = existing[0];
      // MySQL specific date math
      const result = await this.conn.execute(
        `SELECT TIMESTAMPDIFF(MINUTE, ?, CURRENT_TIMESTAMP) as minutes_since_activity`,
        [existingRow.last_activity]
      );

      if (result.length > 0 && result[0].minutes_since_activity <= timeoutMinutes) {
        return existingRow.id;
      }
    }

    const uuid = randomUUID();
    await this.conn.execute(
      `INSERT IGNORE INTO memori_conversation(uuid, session_id) VALUES (?, ?)`,
      [uuid, sessionId]
    );
    await this.conn.commit();

    const newConv = await this.conn.execute(
      `SELECT id FROM memori_conversation WHERE session_id = ?`,
      [sessionId]
    );
    return newConv.length > 0 ? newConv[0].id : null;
  }

  public async update(id: number | string, summary: string): Promise<this> {
    if (!summary) return this;
    await this.conn.execute(`UPDATE memori_conversation SET summary = ? WHERE id = ?`, [
      summary,
      id,
    ]);
    await this.conn.commit();
    return this;
  }
}

class Entity {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(externalId: string): Promise<number | string | null> {
    await this.conn.execute(`INSERT IGNORE INTO memori_entity(uuid, external_id) VALUES (?, ?)`, [
      randomUUID(),
      externalId,
    ]);
    await this.conn.commit();

    const res = await this.conn.execute(`SELECT id FROM memori_entity WHERE external_id = ?`, [
      externalId,
    ]);
    return res.length > 0 ? res[0].id : null;
  }
}

class EntityFact {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(
    entityId: number | string,
    facts: string[],
    factEmbeddings?: number[][],
    conversationId?: number | string | null
  ): Promise<this> {
    if (!facts || facts.length === 0) return this;

    for (let i = 0; i < facts.length; i++) {
      const fact = facts[i];
      const embedding = factEmbeddings && i < factEmbeddings.length ? factEmbeddings[i] : [];
      const embeddingFormatted = formatEmbeddingForDb(embedding);
      const uniq = generateUniq([fact]);

      await this.conn.execute(
        `
        INSERT INTO memori_entity_fact(
            uuid, entity_id, content, content_embedding, num_times, date_last_time, uniq
        ) VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?)
        ON DUPLICATE KEY UPDATE 
            num_times = num_times + 1,
            date_last_time = CURRENT_TIMESTAMP
        `,
        [randomUUID(), entityId, fact, embeddingFormatted, uniq]
      );

      if (conversationId) {
        const factRow = await this.conn.execute(
          `SELECT id FROM memori_entity_fact WHERE entity_id = ? AND uniq = ?`,
          [entityId, uniq]
        );
        const factId = factRow.length > 0 ? factRow[0].id : null;

        if (factId) {
          await this.conn.execute(
            `
            INSERT IGNORE INTO memori_entity_fact_mention(
                uuid, entity_id, fact_id, conversation_id
            ) VALUES (?, ?, ?, ?)
            `,
            [randomUUID(), entityId, factId, conversationId]
          );
        }
      }
    }
    return this;
  }

  public async getEmbeddings(entityId: string | number, limit: number = 1000) {
    const results = await this.conn.execute(
      `
      SELECT id, content_embedding
        FROM memori_entity_fact
       WHERE entity_id = ?
       ORDER BY date_last_time DESC, num_times DESC, id DESC
       LIMIT ?
      `,
      [entityId, limit]
    );

    return results.map((r) => ({
      id: r.id,
      content_embedding_b64: Buffer.from(r.content_embedding).toString('base64'),
    }));
  }

  public async getFactsByIds(factIds: (string | number)[]): Promise<CandidateFactRow[]> {
    if (!factIds || factIds.length === 0) return [];

    const placeholders = factIds.map(() => '?').join(',');
    const factRows = await this.conn.execute(
      `SELECT id, content, date_created FROM memori_entity_fact WHERE id IN (${placeholders})`,
      factIds
    );

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

    const summaryRows = await this.conn.execute(
      `
      SELECT m.fact_id, c.summary AS content, COALESCE(c.date_updated, c.date_created) AS date_created
        FROM memori_entity_fact_mention m
        JOIN memori_conversation c ON c.id = m.conversation_id
       WHERE m.fact_id IN (${placeholders})
         AND c.summary IS NOT NULL AND c.summary <> ''
      `,
      factIds
    );

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

  public async create(entityId: number | string, semanticTriples: any[]): Promise<this> {
    if (!semanticTriples || semanticTriples.length === 0) return this;

    for (const triple of semanticTriples) {
      const subjName = triple.subject?.name || triple.subject_name;
      const subjType = triple.subject?.type || triple.subject_type || 'entity';
      const pred = triple.predicate;
      const objName = triple.object?.name || triple.object_name;
      const objType = triple.object?.type || triple.object_type || 'entity';

      const subjectUniq = generateUniq([subjName, subjType]);
      await this.conn.execute(
        `INSERT IGNORE INTO memori_subject(uuid, name, type, uniq) VALUES (?, ?, ?, ?)`,
        [randomUUID(), subjName, subjType, subjectUniq]
      );
      const subjRes = await this.conn.execute(`SELECT id FROM memori_subject WHERE uniq = ?`, [
        subjectUniq,
      ]);
      const subjectId = subjRes.length > 0 ? subjRes[0].id : null;

      const predicateUniq = generateUniq([pred]);
      await this.conn.execute(
        `INSERT IGNORE INTO memori_predicate(uuid, content, uniq) VALUES (?, ?, ?)`,
        [randomUUID(), pred, predicateUniq]
      );
      const predRes = await this.conn.execute(`SELECT id FROM memori_predicate WHERE uniq = ?`, [
        predicateUniq,
      ]);
      const predicateId = predRes.length > 0 ? predRes[0].id : null;

      const objectUniq = generateUniq([objName, objType]);
      await this.conn.execute(
        `INSERT IGNORE INTO memori_object(uuid, name, type, uniq) VALUES (?, ?, ?, ?)`,
        [randomUUID(), objName, objType, objectUniq]
      );
      const objRes = await this.conn.execute(`SELECT id FROM memori_object WHERE uniq = ?`, [
        objectUniq,
      ]);
      const objectId = objRes.length > 0 ? objRes[0].id : null;

      if (entityId && subjectId && predicateId && objectId) {
        await this.conn.execute(
          `
          INSERT INTO memori_knowledge_graph(
              uuid, entity_id, subject_id, predicate_id, object_id, num_times, date_last_time
          ) VALUES (?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
          ON DUPLICATE KEY UPDATE 
              num_times = num_times + 1,
              date_last_time = CURRENT_TIMESTAMP
          `,
          [randomUUID(), entityId, subjectId, predicateId, objectId]
        );
      }
    }
    await this.conn.commit();
    return this;
  }
}

class Process {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(externalId: string): Promise<number | string | null> {
    await this.conn.execute(`INSERT IGNORE INTO memori_process(uuid, external_id) VALUES (?, ?)`, [
      randomUUID(),
      externalId,
    ]);
    await this.conn.commit();

    const res = await this.conn.execute(`SELECT id FROM memori_process WHERE external_id = ?`, [
      externalId,
    ]);
    return res.length > 0 ? res[0].id : null;
  }
}

class ProcessAttribute {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(processId: number | string, attributes: string[]): Promise<this> {
    if (!attributes || attributes.length === 0) return this;

    for (const attribute of attributes) {
      const uniq = generateUniq([attribute]);
      await this.conn.execute(
        `
        INSERT INTO memori_process_attribute(
            uuid, process_id, content, num_times, date_last_time, uniq
        ) VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP, ?)
        ON DUPLICATE KEY UPDATE 
            num_times = num_times + 1,
            date_last_time = CURRENT_TIMESTAMP
        `,
        [randomUUID(), processId, attribute, uniq]
      );
    }
    await this.conn.commit();
    return this;
  }
}

class Session {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(
    uuid: string,
    entityId: number | string,
    processId: number | string
  ): Promise<number | string | null> {
    await this.conn.execute(
      `INSERT IGNORE INTO memori_session(uuid, entity_id, process_id) VALUES (?, ?, ?)`,
      [uuid, entityId, processId]
    );
    await this.conn.commit();

    const res = await this.conn.execute(`SELECT id FROM memori_session WHERE uuid = ?`, [uuid]);
    return res.length > 0 ? res[0].id : null;
  }
}

class SchemaVersion {
  constructor(private readonly conn: StorageAdapter) {}

  public async create(num: number): Promise<void> {
    await this.conn.execute(`INSERT INTO memori_schema_version(num) VALUES (?)`, [num]);
  }

  public async delete(): Promise<void> {
    await this.conn.execute(`DELETE FROM memori_schema_version`);
  }

  public async read(): Promise<number | null> {
    try {
      const res = await this.conn.execute<{ num: number | string }>(
        `SELECT num FROM memori_schema_version`
      );
      return res.length > 0 ? Number(res[0].num) : null;
    } catch (e) {
      return null;
    }
  }
}

class Schema {
  public readonly version: SchemaVersion;
  constructor(conn: StorageAdapter) {
    this.version = new SchemaVersion(conn);
  }
}

// --- Main Driver Class ---

export class MysqlDriver extends BaseDriver {
  public readonly requiresRollbackOnError = true;
  public readonly migrations = mysqlMigrations;

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

// Automatically register this driver syntax for MySQL
Registry.registerDriver('mysql', MysqlDriver);
