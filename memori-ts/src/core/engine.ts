import { MemoriEngine } from '/Users/rpkruse/src/memori/python-sdk/rust-core/bindings/node/index.js';
import { StorageBridge, WriteBatch } from '../types/storage.js';
import { RetrievalRequest, RecallObject } from '../types/api.js';
import { AugmentationInput } from '../types/integrations.js';

export class NativeEngine {
  private inner?: MemoriEngine;
  private _hasStorage: boolean = false;

  constructor(storageBridge?: StorageBridge, modelName?: string) {
    if (storageBridge) {
      this._hasStorage = true;
      this.inner = new MemoriEngine(
        modelName || null,
        (id: number, reqJson: string) => {
          try {
            const req = JSON.parse(reqJson) as { entity_id: string; limit: number };
            const result = storageBridge.fetchEmbeddings(req.entity_id, req.limit);
            if (result instanceof Promise) {
              result
                .then((res) => {
                  this.inner!.resolveCallback(id, JSON.stringify(res));
                })
                .catch((err: unknown) => {
                  console.error('[Memori] Bridge Error in fetchEmbeddings:', err);
                  this.inner!.resolveCallback(id, '[]');
                });
            } else {
              this.inner!.resolveCallback(id, JSON.stringify(result));
            }
          } catch (e: unknown) {
            console.error('[Memori] Bridge Sync Error (fetchEmbeddings):', e);
            this.inner!.resolveCallback(id, '[]');
          }
        },
        (id: number, reqJson: string) => {
          try {
            const req = JSON.parse(reqJson) as { ids: (number | string)[] };
            const result = storageBridge.fetchFactsByIds(req.ids);
            if (result instanceof Promise) {
              result
                .then((res) => {
                  this.inner!.resolveCallback(id, JSON.stringify(res));
                })
                .catch((err: unknown) => {
                  console.error('[Memori] Bridge Error in fetchFactsByIds:', err);
                  this.inner!.resolveCallback(id, '[]');
                });
            } else {
              this.inner!.resolveCallback(id, JSON.stringify(result));
            }
          } catch (e: unknown) {
            console.error('[Memori] Bridge Sync Error (fetchFactsByIds):', e);
            this.inner!.resolveCallback(id, '[]');
          }
        },
        (id: number, reqJson: string) => {
          try {
            const req = JSON.parse(reqJson) as WriteBatch;
            const result = storageBridge.writeBatch(req);
            if (result instanceof Promise) {
              result
                .then((res) => {
                  this.inner!.resolveCallback(id, JSON.stringify(res));
                })
                .catch((err: unknown) => {
                  console.error('[Memori] Bridge Error in writeBatch:', err);
                  this.inner!.resolveCallback(id, JSON.stringify({ written_ops: 0 }));
                });
            } else {
              this.inner!.resolveCallback(id, JSON.stringify(result));
            }
          } catch (e: unknown) {
            console.error('[Memori] Bridge Sync Error (writeBatch):', e);
            this.inner!.resolveCallback(id, JSON.stringify({ written_ops: 0 }));
          }
        }
      );
    } else {
      // Fallback Engine without Storage
      this.inner = new MemoriEngine(
        modelName || null,
        (id: number) => {
          this.inner!.resolveCallback(id, '[]');
        },
        (id: number) => {
          this.inner!.resolveCallback(id, '[]');
        },
        (id: number) => {
          this.inner!.resolveCallback(id, JSON.stringify({ written_ops: 0 }));
        }
      );
    }
  }

  public get hasStorage(): boolean {
    return this._hasStorage;
  }

  public async retrieve(request: RetrievalRequest): Promise<RecallObject[]> {
    if (!this.inner) throw new Error('Native engine not initialized.');

    // Map TS snake_case into N-API camelCase
    const napiReq = {
      entityId: request.entity_id,
      queryText: request.query_text,
      denseLimit: request.dense_limit,
      limit: request.limit,
    };

    // Fast call into Rust without JSON stringify
    const napiResults = await this.inner.retrieve(napiReq);

    // Map N-API camelCase back to Memori's expected TS snake_case
    return napiResults.map((r: any) => ({
      id: r.id,
      content: r.content,
      rank_score: r.rankScore,
      similarity: r.similarity,
      date_created: r.dateCreated,
      summaries: r.summaries?.map((s: any) => ({
        content: s.content,
        date_created: s.dateCreated,
        entity_fact_id: s.entityFactId,
        fact_id: s.factId,
      })),
    }));
  }

  public async recall(request: RetrievalRequest): Promise<string> {
    if (!this.inner) throw new Error('Native engine not initialized.');

    const napiReq = {
      entityId: request.entity_id,
      queryText: request.query_text,
      denseLimit: request.dense_limit,
      limit: request.limit,
    };

    return await this.inner.recall(napiReq);
  }

  public embedTexts(texts: string[]): Float32Array[] {
    if (!this.inner || texts.length === 0) return [];
    try {
      return this.inner.embedTexts(texts);
    } catch (e: unknown) {
      console.error('[Memori] Bridge Sync Error (embedTexts):', e);
      return [];
    }
  }

  public submitAugmentation(input: AugmentationInput): string {
    if (!this.inner) throw new Error('Native engine not initialized.');

    // Map TS snake_case into N-API camelCase, converting nulls to undefined
    const napiInput = {
      entityId: input.entity_id,
      processId: input.process_id ?? undefined,
      conversationId: input.conversation_id ?? undefined,
      conversationMessages: input.conversation_messages ?? undefined,
      systemPrompt: input.system_prompt ?? undefined,
      llmProvider: input.llm_provider ?? undefined,
      llmModel: input.llm_model ?? undefined,
      llmProviderSdkVersion: input.llm_provider_sdk_version ?? undefined,
      framework: input.framework ?? undefined,
      platformProvider: input.platform_provider ?? undefined,
      storageDialect: input.storage_dialect ?? undefined,
      storageCockroachdb: input.storage_cockroachdb ?? undefined,
      sdkVersion: input.sdk_version ?? undefined,
      useMockResponse: input.use_mock_response ?? undefined,
      sessionId: input.session_id ?? undefined,
      factId: input.fact_id ?? undefined,
      content: input.content ?? undefined,
    };

    return this.inner.submitAugmentation(napiInput);
  }

  public async waitForAugmentation(timeoutMs?: number): Promise<boolean> {
    if (!this.inner) return false;
    return await this.inner.waitForAugmentation(timeoutMs);
  }
}