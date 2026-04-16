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

  /**
   * Wire in the Rust engine's fastembed so the write pipeline can generate
   * embeddings for facts that arrive without them from the cloud augmentation API.
   */
  public setEmbedder(fn: (texts: string[]) => number[][]): void {
    this.embedder = fn;
  }

  public async build(): Promise<void> {
    const builder = new Builder(this.config, this.adapter, this.driver);
    await builder.execute();
  }

  public async close(): Promise<void> {
    // Give the event loop one last tick to process any final background callbacks
    await new Promise((resolve) => setTimeout(resolve, 100));
    await this.adapter.close();
  }

  public getDialect(): string {
    return this.adapter.getDialect();
  }

  // ====================================================================
  // Synchronous overrides to satisfy the Rust Engine's requirements
  // ====================================================================

  public fetchEmbeddings(entityId: string, limit: number): any {
    const eId = this.driver.entity.create(entityId);
    const rows = this.driver.entityFact.getEmbeddings(eId || entityId, limit);
    console.log(`[Memori][fetchEmbeddings] entity="${entityId}" → internalId=${eId} → ${rows.length} embedding row(s) returned`);
    return rows;
  }

  public fetchFactsByIds(ids: (number | string)[]): any {
    const rows = this.driver.entityFact.getFactsByIds(ids);
    console.log(`[Memori][fetchFactsByIds] ${ids.length} id(s) requested → ${rows.length} fact(s) returned`);
    return rows;
  }

  public writeBatch(batch: WriteBatch): any {
    console.log(`[Memori][writeBatch] received ${batch.ops.length} op(s): ${batch.ops.map((o) => o.op_type).join(', ')}`);
    if (this.adapter.getDialect() === 'sqlite') {
      return this.writeBatchSync(batch);
    }
    return this.writeBatchAsync(batch);
  }

  private writeBatchSync(batch: WriteBatch): WriteAck {
    let written = 0;
    for (const op of batch.ops) {
      try {
        switch (op.op_type) {
          case 'entity_fact.create': {
            const eId = this.driver.entity.create(op.payload.entity_id);
            const internalEntityId = eId || op.payload.entity_id;

            // The cloud augmentation API returns facts without embeddings.
            // Generate them now using the Rust engine's fastembed model — same
            // model used for recall — so vectors are in the same space.
            let factEmbeddings = op.payload.fact_embeddings;
            if ((!factEmbeddings || factEmbeddings.length === 0) && this.embedder && op.payload.facts?.length > 0) {
              factEmbeddings = this.embedder(op.payload.facts);
              console.log(`[Memori][writeBatch] embedded ${factEmbeddings.length} fact(s) locally`);
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
            // upsert_fact carries no embeddings — the Rust engine cannot do vector
            // search on it, but we store the content so it is at least persisted.
            // The fact will be skipped by getEmbeddings and therefore won't surface
            // in recall until the Rust engine re-embeds it on a future write cycle.
            const eId = this.driver.entity.create(op.payload.entity_id);
            const internalEntityId = eId || op.payload.entity_id;
            if (op.payload.content) {
              this.driver.entityFact.createWithoutEmbedding(internalEntityId, op.payload.content);
            }
            break;
          }
        }
        written++;
      } catch (e) {
        console.warn(`[Memori] Failed to process WriteOp ${op.op_type}:`, e);
      }
    }
    return { written_ops: written };
  }

  /**
   * Asynchronous write logic for PostgreSQL/MySQL.
   * Handles the standard async drivers used in Node.js.
   */
  private async writeBatchAsync(batch: WriteBatch): Promise<WriteAck> {
    let written = 0;
    for (const op of batch.ops) {
      try {
        switch (op.op_type) {
          case 'entity_fact.create': {
            const eId = await this.driver.entity.create(op.payload.entity_id);
            const internalEntityId = eId || op.payload.entity_id;

            let internalConvId = null;
            if (op.payload.conversation_id) {
              const sId = await this.driver.session.create(op.payload.conversation_id, internalEntityId, null);
              internalConvId = await this.driver.conversation.create(sId || op.payload.conversation_id, 30);
            }

            // Generate embeddings locally if they are missing from the cloud payload
            let factEmbeddings = op.payload.fact_embeddings;
            if ((!factEmbeddings || factEmbeddings.length === 0) && this.embedder && op.payload.facts?.length > 0) {
              factEmbeddings = this.embedder(op.payload.facts);
            }

            await this.driver.entityFact.create(internalEntityId, op.payload.facts, factEmbeddings, internalConvId);
            break;
          }

          case 'knowledge_graph.create': {
            const eId = await this.driver.entity.create(op.payload.entity_id);
            await this.driver.knowledgeGraph.create(eId || op.payload.entity_id, op.payload.semantic_triples);
            break;
          }

          case 'process_attribute.create': {
            const pId = await this.driver.process.create(op.payload.process_id);
            await this.driver.processAttribute.create(
              pId || op.payload.process_id, 
              Array.isArray(op.payload.attributes) ? op.payload.attributes : Object.values(op.payload.attributes)
            );
            break;
          }

          case 'conversation.update': {
            const sId = await this.driver.session.create(op.payload.conversation_id, null, null);
            const convId = await this.driver.conversation.create(sId || op.payload.conversation_id, 30);
            await this.driver.conversation.update(convId || op.payload.conversation_id, op.payload.summary);
            break;
          }
        }
        written++;
      } catch (e) {
        console.error(`[Memori] Failed to process Async WriteOp ${op.op_type}:`, e);
      }
    }
    return { written_ops: written };
  }
}
