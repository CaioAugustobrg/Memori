import { StorageAdapter, BaseDriver } from './base.js';
import { Registry } from './registry.js';
import { Builder } from './builder.js';
import { Config } from '../core/config.js';
import {
  StorageBridge,
  WriteBatch,
  WriteAck,
  CandidateFactRow,
  EmbeddingRow,
} from '../types/storage.js';

import './adapters/postgresql.js';
import './drivers/postgresql.js';
import './adapters/sqlite.js';
import './drivers/sqlite.js';
import './adapters/mysql.js';
import './drivers/mysql.js';

export class StorageManager implements StorageBridge {
  private readonly adapter: StorageAdapter;
  private readonly driver: BaseDriver;
  private readonly config: Config;
  private embedder?: (texts: string[]) => number[][];

  constructor(rawConnection: any) {
    this.config = new Config();
    this.adapter = Registry.getAdapter(rawConnection);
    this.driver = Registry.getDriver(this.adapter);
  }

  public setEmbedder(fn: (texts: string[]) => number[][]): void {
    this.embedder = fn;
  }

  public async build(): Promise<void> {
    const builder = new Builder(this.config, this.adapter, this.driver);
    await builder.execute();
  }

  public async close(): Promise<void> {
    // Small delay to ensure any pending microtasks finish before closing
    await new Promise((resolve) => setTimeout(resolve, 100));
    await this.adapter.close();
  }

  public getDialect(): string {
    return this.adapter.getDialect();
  }

  // ====================================================================
  // FIXED: Correct async/await for ID lookups (MySQL/Postgres)
  // ====================================================================

  public async fetchEmbeddings(entityId: string, limit: number): Promise<EmbeddingRow[]> {
    // Await the ID lookup so eId is a real value, not a Promise object
    const eId = await this.driver.entity.create(entityId);
    const rows = await this.driver.entityFact.getEmbeddings(eId || entityId, limit);
    return rows;
  }

  public async fetchFactsByIds(ids: (number | string)[]): Promise<CandidateFactRow[]> {
    return await this.driver.entityFact.getFactsByIds(ids);
  }

  public async writeBatch(batch: WriteBatch): Promise<WriteAck> {
    if (this.adapter.getDialect() === 'sqlite') {
      return this.writeBatchSync(batch);
    }
    return await this.writeBatchAsync(batch);
  }

  private writeBatchSync(batch: WriteBatch): WriteAck {
    let written = 0;
    for (const op of batch.ops) {
      try {
        switch (op.op_type) {
          case 'entity_fact.create': {
            const eId = this.driver.entity.create(op.payload.entity_id);
            const internalEntityId = eId || op.payload.entity_id;
            let factEmbeddings = op.payload.fact_embeddings;
            if (
              (!factEmbeddings || factEmbeddings.length === 0) &&
              this.embedder &&
              op.payload.facts?.length > 0
            ) {
              factEmbeddings = this.embedder(op.payload.facts);
            }
            let internalConvId = null;
            if (op.payload.conversation_id) {
              const sId = this.driver.session.create(
                op.payload.conversation_id,
                internalEntityId,
                null
              );
              internalConvId = this.driver.conversation.create(
                sId || op.payload.conversation_id,
                30
              );
            }
            this.driver.entityFact.create(
              internalEntityId,
              op.payload.facts,
              factEmbeddings,
              internalConvId
            );
            break;
          }
          case 'knowledge_graph.create': {
            const eId = this.driver.entity.create(op.payload.entity_id);
            this.driver.knowledgeGraph.create(
              eId || op.payload.entity_id,
              op.payload.semantic_triples
            );
            break;
          }
          case 'process_attribute.create': {
            const pId = this.driver.process.create(op.payload.process_id);
            this.driver.processAttribute.create(
              pId || op.payload.process_id,
              Array.isArray(op.payload.attributes)
                ? op.payload.attributes
                : Object.values(op.payload.attributes)
            );
            break;
          }
          case 'conversation.update': {
            const sId = this.driver.session.create(op.payload.conversation_id, null, null);
            const convId = this.driver.conversation.create(sId || op.payload.conversation_id, 30);
            this.driver.conversation.update(
              convId || op.payload.conversation_id,
              op.payload.summary
            );
            break;
          }
          case 'upsert_fact': {
            const eId = this.driver.entity.create(op.payload.entity_id);
            if (op.payload.content)
              this.driver.entityFact.createWithoutEmbedding(
                eId || op.payload.entity_id,
                op.payload.content
              );
            break;
          }
        }
        written++;
      } catch (e) {
        console.warn(`[Memori] Sync WriteOp failed:`, e);
      }
    }
    return { written_ops: written };
  }

  private async writeBatchAsync(batch: WriteBatch): Promise<WriteAck> {
    let written = 0;
    for (const op of batch.ops) {
      try {
        switch (op.op_type) {
          case 'entity_fact.create': {
            const eId = await this.driver.entity.create(op.payload.entity_id);
            const internalEntityId = eId || op.payload.entity_id;
            let factEmbeddings = op.payload.fact_embeddings;
            if (
              (!factEmbeddings || factEmbeddings.length === 0) &&
              this.embedder &&
              op.payload.facts?.length > 0
            ) {
              factEmbeddings = this.embedder(op.payload.facts);
            }
            let internalConvId = null;
            if (op.payload.conversation_id) {
              const sId = await this.driver.session.create(
                op.payload.conversation_id,
                internalEntityId,
                null
              );
              internalConvId = await this.driver.conversation.create(
                sId || op.payload.conversation_id,
                30
              );
            }
            await this.driver.entityFact.create(
              internalEntityId,
              op.payload.facts,
              factEmbeddings,
              internalConvId
            );
            break;
          }
          case 'knowledge_graph.create': {
            const eId = await this.driver.entity.create(op.payload.entity_id);
            await this.driver.knowledgeGraph.create(
              eId || op.payload.entity_id,
              op.payload.semantic_triples
            );
            break;
          }
          case 'process_attribute.create': {
            const pId = await this.driver.process.create(op.payload.process_id);
            await this.driver.processAttribute.create(
              pId || op.payload.process_id,
              Array.isArray(op.payload.attributes)
                ? op.payload.attributes
                : Object.values(op.payload.attributes)
            );
            break;
          }
          case 'conversation.update': {
            const sId = await this.driver.session.create(op.payload.conversation_id, null, null);
            const convId = await this.driver.conversation.create(
              sId || op.payload.conversation_id,
              30
            );
            await this.driver.conversation.update(
              convId || op.payload.conversation_id,
              op.payload.summary
            );
            break;
          }
          case 'upsert_fact': {
            const eId = await this.driver.entity.create(op.payload.entity_id);
            if (op.payload.content)
              await this.driver.entityFact.createWithoutEmbedding(
                eId || op.payload.entity_id,
                op.payload.content
              );
            break;
          }
        }
        written++;
      } catch (e) {
        console.error(`[Memori] Async WriteOp failed:`, e);
      }
    }
    return { written_ops: written };
  }
}
