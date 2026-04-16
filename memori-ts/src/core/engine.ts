import { MemoriEngine } from '/Users/rpkruse/src/memori/python-sdk/rust-core/bindings/node/index.js';
import { StorageBridge } from '../types/storage.js';
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
        (reqJson: string) => {
          try {
            const req = JSON.parse(reqJson);
            const res = storageBridge.fetchEmbeddings(req.entity_id, req.limit);
            return JSON.stringify(res || []);
          } catch (e) {
            console.warn('[Memori] fetchEmbeddings callback failed:', e);
            return '[]';
          }
        },
        (reqJson: string) => {
          try {
            const req = JSON.parse(reqJson);
            const res = storageBridge.fetchFactsByIds(req.ids);
            return JSON.stringify(res || []);
          } catch (e) {
            console.warn('[Memori] fetchFactsByIds callback failed:', e);
            return '[]';
          }
        },
        (reqJson: string) => {
          try {
            const req = JSON.parse(reqJson);
            const res = storageBridge.writeBatch(req);
            return JSON.stringify(res || { written_ops: 0 });
          } catch (e) {
            console.warn('[Memori] writeBatch callback failed:', e);
            return JSON.stringify({ written_ops: 0 });
          }
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
    return JSON.parse(resJson);
  }

  public async recall(request: RetrievalRequest): Promise<string> {
    if (!this.inner) throw new Error('Native engine not initialized.');
    return await this.inner.recall(JSON.stringify(request));
  }

  /**
   * Synchronously embeds a batch of texts using the engine's loaded fastembed model.
   * Safe to call from the Rust engine's writeBatch callback thread.
   */
  public embedTexts(texts: string[]): number[][] {
    if (!this.inner || texts.length === 0) return [];
    try {
      return JSON.parse(this.inner.embedTexts(JSON.stringify(texts)));
    } catch {
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
