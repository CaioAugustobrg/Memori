import { CallContext, LLMRequest, Message, Role } from '@memorilabs/axon';
import { Api } from '../core/network.js';
import { Config } from '../core/config.js';
import { SessionManager } from '../core/session.js';
import { NativeEngine } from '../core/engine.js';
import {
  extractFacts,
  extractHistory,
  extractLastUserMessage,
  formatSummariesFromFacts,
  stringifyContent,
} from '../utils/utils.js';
import { CloudRecallResponse, ParsedFact } from '../types/api.js';

export class RecallEngine {
  constructor(
    private readonly api: Api,
    private readonly engine: NativeEngine,
    private readonly config: Config,
    private readonly session: SessionManager
  ) {}

  /**
   * Manually triggers a memory retrieval.
   * Routes to the local Rust engine if storage is active, otherwise hits the Cloud API.
   */
  public async recall(query: string): Promise<ParsedFact[]> {
    if (this.engine.hasStorage) {
      if (!this.config.entityId) return [];
      try {
        // CRITICAL: Added 'await' because retrieve is now an async Promise
        const results = await this.engine.retrieve({
          entity_id: this.config.entityId,
          query_text: query,
          dense_limit: 100,
          limit: 10,
        });

        return results.map((r) => ({
          content: r.content,
          score: r.rank_score ?? r.similarity ?? 0,
          dateCreated: r.date_created,
          summaries: r.summaries?.map((s) => ({
            content: s.content,
            dateCreated: s.date_created,
          })),
        }));
      } catch (e) {
        console.warn('Local Manual Recall failed:', e);
        return [];
      }
    }

    const payload = {
      attribution: {
        entity: { id: this.config.entityId },
        process: { id: this.config.processId },
      },
      query,
      session: { id: this.session.id },
    };

    try {
      const response = await this.api.post<CloudRecallResponse>('cloud/recall', payload);
      return extractFacts(response);
    } catch (e) {
      console.warn('Memori Manual Recall failed:', e);
      return [];
    }
  }

  /**
   * The Axon 'before' hook that injects memories into the LLM system prompt.
   */
  public async handleRecall(req: LLMRequest, _ctx: CallContext): Promise<LLMRequest> {
    const sessionId = this.session.id;
    if (!sessionId) return req;

    const userQuery = extractLastUserMessage(req.messages);
    if (!userQuery) return req;

    let facts: ParsedFact[] = [];
    let historyMessages: Message[] = [];

    if (this.engine.hasStorage) {
      if (!this.config.entityId) return req;
      try {
        // CRITICAL: Added 'await' here to prevent thread deadlock
        const rawFacts = await this.engine.retrieve({
          entity_id: this.config.entityId,
          query_text: userQuery,
          dense_limit: 100,
          limit: 10,
        });

        facts = rawFacts.map((r) => ({
          content: r.content,
          score: r.rank_score ?? r.similarity ?? 0,
          dateCreated: r.date_created,
          summaries: r.summaries?.map((s) => ({
            content: s.content,
            dateCreated: s.date_created,
          })),
        }));
      } catch (e) {
        console.warn('Local Recall Hook failed:', e);
        return req;
      }
    } else {
      const payload = {
        attribution: {
          entity: { id: this.config.entityId },
          process: { id: this.config.processId },
        },
        query: userQuery,
        session: { id: sessionId },
      };

      let response: CloudRecallResponse;
      try {
        response = await this.api.post<CloudRecallResponse>('cloud/recall', payload);
      } catch (e) {
        console.warn('Memori Recall failed:', e);
        return req;
      }

      facts = extractFacts(response);
      const historyRaw = extractHistory(response);

      historyMessages = (historyRaw as Array<{ role: Role; content?: unknown; text?: string }>)
        .filter((m) => m.role !== 'system')
        .map((m) => ({
          role: m.role,
          content: stringifyContent(m.content ?? m.text),
        }));
    }

    const relevantFacts = facts
      .filter((f) => f.score >= this.config.recallRelevanceThreshold)
      .map((f) => {
        const dateSuffix = f.dateCreated ? `. Stated at ${f.dateCreated}` : '';
        return `- ${f.content}${dateSuffix}`;
      });

    const relevantSummaries = formatSummariesFromFacts(
      facts.filter((f) => f.score >= this.config.recallRelevanceThreshold)
    );

    let messages = [...req.messages];

    if (historyMessages.length > 0) {
      messages = [...historyMessages, ...messages];
    }

    if (relevantFacts.length > 0) {
      let contextBody = `Relevant context about the user:\n${relevantFacts.join('\n')}`;
      if (relevantSummaries.length > 0) {
        contextBody += `\n\n## Summaries\n\n${relevantSummaries.join('\n\n')}`;
      }

      const recallContext = `\n\n<memori_context>\nOnly use the relevant context if it is relevant to the user's query. ${contextBody}\n</memori_context>`;

      const systemIdx = messages.findIndex((m) => m.role === 'system');
      if (systemIdx >= 0) {
        messages[systemIdx] = {
          ...messages[systemIdx],
          content: messages[systemIdx].content + recallContext,
        };
      } else {
        messages.unshift({ role: 'system', content: recallContext });
      }
    }

    return { ...req, messages };
  }
}
