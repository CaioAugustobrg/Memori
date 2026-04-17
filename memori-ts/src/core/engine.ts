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
    const resJson = await this.inner.retrieve(JSON.stringify(request));
    return JSON.parse(resJson) as RecallObject[];
  }

  public async recall(request: RetrievalRequest): Promise<string> {
    if (!this.inner) throw new Error('Native engine not initialized.');
    return await this.inner.recall(JSON.stringify(request));
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
    return this.inner.submitAugmentation(JSON.stringify(input));
  }

  public async waitForAugmentation(timeoutMs?: number): Promise<boolean> {
    if (!this.inner) return false;
    return await this.inner.waitForAugmentation(timeoutMs);
  }
}
