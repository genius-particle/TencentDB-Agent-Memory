/**
 * PostgresMemoryStore — IMemoryStore on Postgres + pgvector.
 *
 * Open-service path: dense `vector` + client-side jieba/BM25 `sparsevec`.
 * Lexical `ftsSearch` is PostgreSQL `simple` tsvector + token ILIKE
 * (not BM25 sparsevec recall, and not pg_jieba).
 * No new TCVDB behavior.
 *
 * Capability flags are honest: profiles/pagination/audit/clear/deferredEmbedding
 * and knowledge/prompts/generationRefs are implemented; entities is not (metadata store).
 *
 * Fault-tolerance matches IMemoryStore: most methods return empty/false on
 * error rather than throwing (except clearMemoryContent team+agent guard).
 */

import type { Pool, PoolClient } from "pg";
import type { MemoryRecord } from "../record/l1-writer.js";
import type { EmbeddingProviderInfo } from "./embedding.js";
import type { BM25LocalEncoder } from "./bm25-local.js";
import type {
  IMemoryStore,
  StoreCapabilities,
  StoreLogger,
  StoreInitResult,
  L0Record,
  L1SearchResult,
  L1FtsResult,
  L0SearchResult,
  L0FtsResult,
  L0QueryRow,
  L0SessionGroup,
  L1RecordRow,
  L1QueryFilter,
  L0CountFilter,
  L0PaginatedFilter,
  L0PaginatedResult,
  L1CountFilter,
  L1PaginatedFilter,
  L1PaginatedResult,
  IsolationFilter,
  MemoryContentClearFilter,
  MemoryContentClearResult,
  AuditEntry,
  AuditQueryFilter,
  ProfileRecord,
  ProfileSyncRecord,
  ProfileCountFilter,
  KnowledgeEntity,
  KnowledgeType,
  KnowledgeListResult,
  BatchDeleteResult,
} from "./types.js";
import type {
  MemoryPromptRecord,
  MemoryPromptListFilter,
} from "../memory-prompt/types.js";
import {
  buildMemoryGenerationRefId,
  type MemoryGenerationLayer,
  type MemoryGenerationRefRecord,
} from "../memory-generation-log/types.js";
import { DEFAULT_ISOLATION_ID, rowMatchesIsolation } from "./types.js";
import {
  DEFAULT_SPARSE_DIMENSIONS,
  ftsIlikePatterns,
  ftsQueryToText,
  toSparsevecLiteral,
  toVectorLiteral,
} from "./sparsevec.js";

const TAG = "[memory-tdai][postgres]";

export interface PostgresMemoryStoreOptions {
  connectionString: string;
  /** Isolated schema per StorePool instanceId. */
  schema: string;
  dimensions?: number;
  logger?: StoreLogger;
  bm25Encoder?: BM25LocalEncoder;
  sparseDimensions?: number;
}

function iso(value: string | Date | null | undefined): string {
  if (!value) return "";
  if (typeof value === "string") return value;
  return value.toISOString();
}

function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function str(value: unknown, fallback = ""): string {
  return value == null ? fallback : String(value);
}

export class PostgresMemoryStore implements IMemoryStore {
  readonly supportsDeferredEmbedding = true;

  private pool: Pool | null = null;
  private readonly connectionString: string;
  private readonly schema: string;
  private readonly dimensions: number;
  private readonly sparseDimensions: number;
  private readonly logger?: StoreLogger;
  private readonly bm25Encoder?: BM25LocalEncoder;
  private degraded = false;
  private pgModule: typeof import("pg") | null = null;

  constructor(opts: PostgresMemoryStoreOptions) {
    this.connectionString = opts.connectionString;
    this.schema = opts.schema;
    this.dimensions = opts.dimensions ?? 0;
    this.sparseDimensions = opts.sparseDimensions ?? DEFAULT_SPARSE_DIMENSIONS;
    this.logger = opts.logger;
    this.bm25Encoder = opts.bm25Encoder;
  }

  private qIdent(name: string): string {
    return `"${name.replace(/"/g, "\"\"")}"`;
  }

  private async getPool(): Promise<Pool> {
    if (this.pool) return this.pool;
    if (!this.pgModule) {
      this.pgModule = await import("pg");
    }
    const { Pool } = this.pgModule;
    this.pool = new Pool({ connectionString: this.connectionString, max: 8 });
    return this.pool;
  }

  private async withClient<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const pool = await this.getPool();
    const client = await pool.connect();
    try {
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${this.qIdent(this.schema)}`);
      await client.query(`SET search_path TO ${this.qIdent(this.schema)}, public`);
      return await fn(client);
    } finally {
      client.release();
    }
  }

  async init(_providerInfo?: EmbeddingProviderInfo): Promise<StoreInitResult> {
    try {
      await this.withClient(async (client) => {
        try {
          await client.query("CREATE EXTENSION IF NOT EXISTS vector");
        } catch (err) {
          this.logger?.warn?.(
            `${TAG} CREATE EXTENSION vector failed (need pgvector): ${err instanceof Error ? err.message : String(err)}`,
          );
          throw err;
        }

        const sparseType = `sparsevec(${this.sparseDimensions})`;
        const denseType = this.dimensions > 0 ? `vector(${this.dimensions})` : "vector";

        await client.query(`
          CREATE TABLE IF NOT EXISTS l0_conversations (
            record_id TEXT PRIMARY KEY,
            session_key TEXT NOT NULL,
            session_id TEXT NOT NULL DEFAULT 'default',
            team_id TEXT NOT NULL DEFAULT 'default',
            task_id TEXT NOT NULL DEFAULT '',
            user_id TEXT NOT NULL DEFAULT 'default',
            agent_id TEXT NOT NULL DEFAULT 'default',
            role TEXT NOT NULL DEFAULT '',
            message_text TEXT NOT NULL,
            recorded_at TEXT NOT NULL DEFAULT '',
            timestamp BIGINT NOT NULL DEFAULT 0,
            embedding ${denseType},
            sparse_embedding ${sparseType}
          )
        `);

        await client.query(`
          CREATE TABLE IF NOT EXISTS l1_records (
            record_id TEXT PRIMARY KEY,
            content TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT '',
            priority INTEGER NOT NULL DEFAULT 50,
            scene_name TEXT NOT NULL DEFAULT '',
            session_key TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT 'default',
            team_id TEXT NOT NULL DEFAULT 'default',
            task_id TEXT NOT NULL DEFAULT '',
            user_id TEXT NOT NULL DEFAULT 'default',
            agent_id TEXT NOT NULL DEFAULT 'default',
            version INTEGER NOT NULL DEFAULT 0,
            timestamp_str TEXT NOT NULL DEFAULT '',
            timestamp_start TEXT NOT NULL DEFAULT '',
            timestamp_end TEXT NOT NULL DEFAULT '',
            created_time TEXT NOT NULL DEFAULT '',
            updated_time TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding ${denseType},
            sparse_embedding ${sparseType}
          )
        `);

        await client.query(`
          CREATE TABLE IF NOT EXISTS profiles (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL CHECK (type IN ('l2','l3')),
            filename TEXT NOT NULL,
            content TEXT NOT NULL,
            content_md5 TEXT NOT NULL,
            team_id TEXT NOT NULL DEFAULT '',
            agent_id TEXT NOT NULL DEFAULT '',
            user_id TEXT NOT NULL DEFAULT '',
            version INTEGER NOT NULL DEFAULT 0,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
          )
        `);

        await client.query(`
          CREATE TABLE IF NOT EXISTS memory_audit (
            audit_id TEXT PRIMARY KEY,
            record_id TEXT NOT NULL,
            layer TEXT NOT NULL CHECK (layer IN ('L1','L2','L3')),
            action TEXT NOT NULL CHECK (action IN ('update','delete')),
            team_id TEXT,
            agent_id TEXT,
            user_id TEXT,
            task_id TEXT,
            version INTEGER NOT NULL,
            updated_at_ms BIGINT NOT NULL,
            request_id TEXT
          )
        `);

        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l0_session ON l0_conversations(session_key)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l0_session_id ON l0_conversations(session_id)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l0_team_agent ON l0_conversations(team_id, agent_id)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l0_recorded ON l0_conversations(recorded_at)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l1_session ON l1_records(session_id, updated_time)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_l1_team_agent ON l1_records(team_id, agent_id, updated_time)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_profiles_team_agent ON profiles(team_id, agent_id)");
        await client.query("CREATE INDEX IF NOT EXISTS idx_pg_audit_record ON memory_audit(record_id, updated_at_ms)");

        await client.query(`
          CREATE TABLE IF NOT EXISTS entity_knowledge (
            knowledge_id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            service_url TEXT NOT NULL,
            name TEXT NOT NULL,
            summary TEXT,
            team_id TEXT NOT NULL,
            agent_id TEXT NOT NULL DEFAULT '',
            user_id TEXT,
            repo_url TEXT,
            branch TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
          )
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_pg_entity_knowledge_team
            ON entity_knowledge(team_id)
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_pg_entity_knowledge_team_type
            ON entity_knowledge(team_id, type)
        `);

        await client.query(`
          CREATE TABLE IF NOT EXISTS memory_prompts (
            memory_prompt_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            layer TEXT NOT NULL CHECK (layer IN ('l1','l2','l3')),
            prompt TEXT NOT NULL,
            version INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active','deleting')),
            created_by TEXT,
            updated_by TEXT,
            created_at_ms BIGINT NOT NULL,
            updated_at_ms BIGINT NOT NULL
          )
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_pg_memory_prompts_layer_updated
            ON memory_prompts(layer, updated_at_ms)
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_pg_memory_prompts_status
            ON memory_prompts(status)
        `);

        await client.query(`
          CREATE TABLE IF NOT EXISTS memory_generation_refs (
            generation_ref_id TEXT PRIMARY KEY,
            layer TEXT NOT NULL CHECK (layer IN ('l1','l2','l3')),
            memory_id TEXT NOT NULL,
            generation_id TEXT NOT NULL,
            generation_log_id TEXT NOT NULL,
            generation_log_key TEXT NOT NULL,
            memory_prompt_id TEXT NOT NULL,
            memory_prompt_version INTEGER NOT NULL,
            memory_prompt_source TEXT NOT NULL,
            created_at_ms BIGINT NOT NULL
          )
        `);
        await client.query(`
          CREATE INDEX IF NOT EXISTS idx_pg_memory_generation_refs_memory
            ON memory_generation_refs(layer, memory_id)
        `);
      });
      this.degraded = false;
      this.logger?.debug?.(`${TAG} Initialized schema=${this.schema} dimensions=${this.dimensions}`);
      return { needsReindex: false };
    } catch (err) {
      this.degraded = true;
      this.logger?.error?.(
        `${TAG} Init failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return { needsReindex: false, reason: err instanceof Error ? err.message : String(err) };
    }
  }

  isDegraded(): boolean {
    return this.degraded;
  }

  getCapabilities(): StoreCapabilities {
    const hasBm25 = !!this.bm25Encoder;
    const vectorSearch = this.dimensions > 0 && !this.degraded;
    return {
      vectorSearch,
      // Lexical FTS is tsvector('simple') + token ILIKE, not BM25 sparsevec recall.
      ftsSearch: !this.degraded,
      nativeHybridSearch: vectorSearch && hasBm25,
      sparseVectors: hasBm25,
      profiles: true,
      entities: false,
      audit: true,
      prompts: true,
      generationRefs: true,
      knowledge: true,
      pagination: true,
      clearMemoryContent: true,
      deferredEmbedding: true,
    };
  }

  close(): void {
    const pool = this.pool;
    this.pool = null;
    void pool?.end().catch(() => undefined);
  }

  isFtsAvailable(): boolean {
    return !this.degraded;
  }

  private encodeSparse(text: string, forQuery: boolean): string | null {
    if (!this.bm25Encoder || !text) return null;
    try {
      const encoded = forQuery
        ? this.bm25Encoder.encodeQueries([text])
        : this.bm25Encoder.encodeTexts([text]);
      if (!encoded.length) return null;
      return toSparsevecLiteral(encoded[0], this.sparseDimensions);
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} BM25 encode failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return null;
    }
  }

  // ── L1 write ─────────────────────────────────────────────

  async upsertL1(record: MemoryRecord, embedding?: Float32Array): Promise<boolean> {
    if (this.degraded) return false;
    try {
      const tsStr = record.timestamps[0] ?? "";
      const tsStart = record.timestamps.length > 0
        ? record.timestamps.reduce((a, b) => (a < b ? a : b)) : tsStr;
      const tsEnd = record.timestamps.length > 0
        ? record.timestamps.reduce((a, b) => (a > b ? a : b)) : tsStr;
      const skipVec = !embedding || embedding.every((v) => v === 0);
      const sparse = this.encodeSparse(record.content, false);
      await this.withClient(async (client) => {
        await client.query(
          `INSERT INTO l1_records (
            record_id, content, type, priority, scene_name, session_key, session_id,
            team_id, task_id, version, timestamp_str, timestamp_start, timestamp_end,
            created_time, updated_time, metadata_json, user_id, agent_id,
            embedding, sparse_embedding
          ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
            $19::vector, $20::sparsevec
          )
          ON CONFLICT (record_id) DO UPDATE SET
            content=EXCLUDED.content, type=EXCLUDED.type, priority=EXCLUDED.priority,
            scene_name=EXCLUDED.scene_name, session_key=EXCLUDED.session_key,
            session_id=EXCLUDED.session_id, team_id=EXCLUDED.team_id, task_id=EXCLUDED.task_id,
            version=EXCLUDED.version, timestamp_str=EXCLUDED.timestamp_str,
            timestamp_start=EXCLUDED.timestamp_start, timestamp_end=EXCLUDED.timestamp_end,
            updated_time=EXCLUDED.updated_time, metadata_json=EXCLUDED.metadata_json,
            user_id=EXCLUDED.user_id, agent_id=EXCLUDED.agent_id,
            embedding=COALESCE(EXCLUDED.embedding, l1_records.embedding),
            sparse_embedding=COALESCE(EXCLUDED.sparse_embedding, l1_records.sparse_embedding)`,
          [
            record.id,
            record.content,
            record.type,
            record.priority,
            record.scene_name,
            record.sessionKey,
            record.sessionId || DEFAULT_ISOLATION_ID,
            record.teamId || DEFAULT_ISOLATION_ID,
            record.taskId || "",
            record.version ?? 0,
            tsStr,
            tsStart,
            tsEnd,
            record.createdAt || "",
            record.updatedAt || "",
            JSON.stringify(record.metadata ?? {}),
            record.userId || DEFAULT_ISOLATION_ID,
            record.agentId || DEFAULT_ISOLATION_ID,
            skipVec ? null : toVectorLiteral(embedding!),
            sparse,
          ],
        );
      });
      return true;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-upsert] FAILED id=${record.id}: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async deleteL1(recordId: string, filter?: IsolationFilter): Promise<boolean> {
    if (this.degraded) return false;
    try {
      const n = await this.withClient(async (client) => {
        if (filter) {
          const got = await client.query(`SELECT team_id, user_id, agent_id, session_id, task_id, session_key FROM l1_records WHERE record_id = $1`, [recordId]);
          const row = got.rows[0];
          if (!row || !rowMatchesIsolation(row, filter)) return 0;
        }
        const res = await client.query(`DELETE FROM l1_records WHERE record_id = $1`, [recordId]);
        return res.rowCount ?? 0;
      });
      return n > 0;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-delete] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async deleteL1Batch(recordIds: string[], filter?: IsolationFilter): Promise<boolean> {
    if (this.degraded) return false;
    if (recordIds.length === 0) return true;
    try {
      for (const id of recordIds) {
        await this.deleteL1(id, filter);
      }
      return true;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-deleteBatch] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async deleteL1Expired(cutoffIso: string): Promise<number> {
    if (this.degraded) return 0;
    try {
      return await this.withClient(async (client) => {
        const expired = await client.query(
          `SELECT COUNT(*)::int AS cnt FROM l1_records WHERE updated_time != '' AND updated_time < $1`,
          [cutoffIso],
        );
        const expiredCount = num(expired.rows[0]?.cnt);
        if (expiredCount <= 0) return 0;
        const total = num((await client.query(`SELECT COUNT(*)::int AS cnt FROM l1_records`)).rows[0]?.cnt);
        if (total > 0 && expiredCount / total > 0.8) {
          this.logger?.warn?.(
            `${TAG} [L1-deleteExpired] BLOCKED: would delete ${expiredCount}/${total} (>80%) cutoff=${cutoffIso}`,
          );
          return 0;
        }
        const res = await client.query(
          `DELETE FROM l1_records WHERE updated_time != '' AND updated_time < $1`,
          [cutoffIso],
        );
        return res.rowCount ?? expiredCount;
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-deleteExpired] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return 0;
    }
  }

  async countL1(filter?: L1CountFilter): Promise<number> {
    if (this.degraded) return 0;
    try {
      const { clause, params } = this.l1CountWhere(filter);
      return await this.withClient(async (client) => {
        const sql = `SELECT COUNT(*)::int AS cnt FROM l1_records${clause ? ` WHERE ${clause}` : ""}`;
        const res = await client.query(sql, params);
        return num(res.rows[0]?.cnt);
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} countL1 failed: ${err instanceof Error ? err.message : String(err)}`);
      return 0;
    }
  }

  async queryL1Records(filter?: L1QueryFilter): Promise<L1RecordRow[]> {
    if (this.degraded) return [];
    try {
      const parts: string[] = [];
      const params: unknown[] = [];
      const add = (sql: string, value: unknown) => {
        params.push(value);
        parts.push(sql.replace("?", `$${params.length}`));
      };
      if (filter?.recordIds?.length) {
        params.push(filter.recordIds);
        parts.push(`record_id = ANY($${params.length}::text[])`);
      }
      if (filter?.sessionId) add("session_id = ?", filter.sessionId);
      else if (filter?.sessionKey) add("session_key = ?", filter.sessionKey);
      if (filter?.taskId !== undefined) add("task_id = ?", filter.taskId);
      if (filter?.teamId !== undefined) add("team_id = ?", filter.teamId);
      if (filter?.userId !== undefined) add("user_id = ?", filter.userId);
      if (filter?.agentId !== undefined) add("agent_id = ?", filter.agentId);
      if (filter?.updatedAfter) add("updated_time > ?", filter.updatedAfter);
      const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, content, type, priority, scene_name, session_key, session_id,
                  team_id, task_id, user_id, agent_id, version, timestamp_str, timestamp_start,
                  timestamp_end, created_time, updated_time, metadata_json
           FROM l1_records ${where} ORDER BY updated_time ASC`,
          params,
        );
        return res.rows.map((r) => this.mapL1Row(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-query] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async getAllL1Texts(): Promise<Array<{ record_id: string; content: string; updated_time: string }>> {
    if (this.degraded) return [];
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(`SELECT record_id, content, updated_time FROM l1_records`);
        return res.rows.map((r) => ({
          record_id: str(r.record_id),
          content: str(r.content),
          updated_time: iso(r.updated_time),
        }));
      });
    } catch {
      return [];
    }
  }

  async searchL1Vector(
    queryEmbedding: Float32Array,
    topK = 5,
    _queryText?: string,
    filter?: IsolationFilter,
  ): Promise<L1SearchResult[]> {
    if (this.degraded || this.dimensions <= 0) return [];
    try {
      const lit = toVectorLiteral(queryEmbedding);
      const retrieve = filter ? Math.max(topK * 5, topK) : topK;
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, content, type, priority, scene_name, session_key, session_id,
                  team_id, task_id, user_id, agent_id, version, timestamp_str, timestamp_start,
                  timestamp_end, metadata_json,
                  1 - (embedding <=> $1::vector) AS score
           FROM l1_records
           WHERE embedding IS NOT NULL
           ORDER BY embedding <=> $1::vector
           LIMIT $2`,
          [lit, retrieve],
        );
        return res.rows
          .filter((r) => rowMatchesIsolation(r, filter))
          .slice(0, topK)
          .map((r) => this.mapL1Search(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-vector] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async searchL1Fts(ftsQuery: string, limit = 10, filter?: IsolationFilter): Promise<L1FtsResult[]> {
    if (this.degraded || !ftsQuery) return [];
    const text = ftsQueryToText(ftsQuery);
    const patterns = ftsIlikePatterns(text);
    if (!text || patterns.length === 0) return [];
    const retrieve = filter ? Math.max(limit * 5, limit) : limit;
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, content, type, priority, scene_name, session_key, session_id,
                  team_id, task_id, user_id, agent_id, version, timestamp_str, timestamp_start,
                  timestamp_end, metadata_json,
                  ts_rank_cd(
                    to_tsvector('simple', coalesce(content, '')),
                    plainto_tsquery('simple', $1)
                  ) AS score
           FROM l1_records
           WHERE to_tsvector('simple', coalesce(content, '')) @@ plainto_tsquery('simple', $1)
              OR content ILIKE ANY($2::text[])
           ORDER BY score DESC, updated_time DESC
           LIMIT $3`,
          [text, patterns, retrieve],
        );
        return res.rows
          .filter((r) => rowMatchesIsolation(r, filter))
          .slice(0, limit)
          .map((r) => this.mapL1Search(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L1-fts] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async searchL1Hybrid(params: {
    query?: string;
    queryEmbedding?: Float32Array;
    sparseVector?: Array<[number, number]>;
    topK?: number;
    filter?: IsolationFilter;
  }): Promise<L1SearchResult[]> {
    const topK = params.topK ?? 10;
    const queryText = params.query ?? "";
    if (params.queryEmbedding && this.dimensions > 0 && !this.bm25Encoder) {
      return this.searchL1Vector(params.queryEmbedding, topK, queryText, params.filter);
    }
    if (queryText) return this.searchL1Fts(queryText, topK, params.filter);
    if (params.queryEmbedding) return this.searchL1Vector(params.queryEmbedding, topK, undefined, params.filter);
    return [];
  }

  // ── L0 write ─────────────────────────────────────────────

  async upsertL0(record: L0Record, embedding?: Float32Array): Promise<boolean> {
    if (this.degraded) return false;
    try {
      const skipVec = !embedding || embedding.every((v) => v === 0);
      const sparse = this.encodeSparse(record.messageText, false);
      await this.withClient(async (client) => {
        await client.query(
          `INSERT INTO l0_conversations (
            record_id, session_key, session_id, team_id, task_id, role, message_text,
            recorded_at, timestamp, user_id, agent_id, embedding, sparse_embedding
          ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, $12::vector, $13::sparsevec
          )
          ON CONFLICT (record_id) DO UPDATE SET
            session_key=EXCLUDED.session_key, session_id=EXCLUDED.session_id,
            team_id=EXCLUDED.team_id, task_id=EXCLUDED.task_id, role=EXCLUDED.role,
            message_text=EXCLUDED.message_text, recorded_at=EXCLUDED.recorded_at,
            timestamp=EXCLUDED.timestamp, user_id=EXCLUDED.user_id, agent_id=EXCLUDED.agent_id,
            embedding=COALESCE(EXCLUDED.embedding, l0_conversations.embedding),
            sparse_embedding=COALESCE(EXCLUDED.sparse_embedding, l0_conversations.sparse_embedding)`,
          [
            record.id,
            record.sessionKey,
            record.sessionId || DEFAULT_ISOLATION_ID,
            record.teamId || DEFAULT_ISOLATION_ID,
            record.taskId || "",
            record.role,
            record.messageText,
            record.recordedAt || "",
            record.timestamp || 0,
            record.userId || DEFAULT_ISOLATION_ID,
            record.agentId || DEFAULT_ISOLATION_ID,
            skipVec ? null : toVectorLiteral(embedding!),
            sparse,
          ],
        );
      });
      return true;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-upsert] FAILED id=${record.id}: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async updateL0Embedding(recordId: string, embedding: Float32Array): Promise<boolean> {
    if (this.degraded || !embedding || embedding.every((v) => v === 0)) return false;
    try {
      const n = await this.withClient(async (client) => {
        const res = await client.query(
          `UPDATE l0_conversations SET embedding = $2::vector WHERE record_id = $1`,
          [recordId, toVectorLiteral(embedding)],
        );
        return res.rowCount ?? 0;
      });
      return n > 0;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-updateEmbedding] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async deleteL0(recordId: string, filter?: IsolationFilter): Promise<boolean> {
    if (this.degraded) return false;
    try {
      const n = await this.withClient(async (client) => {
        if (filter) {
          const got = await client.query(
            `SELECT team_id, user_id, agent_id, session_id, task_id, session_key FROM l0_conversations WHERE record_id = $1`,
            [recordId],
          );
          const row = got.rows[0];
          if (!row || !rowMatchesIsolation(row, filter)) return 0;
        }
        const res = await client.query(`DELETE FROM l0_conversations WHERE record_id = $1`, [recordId]);
        return res.rowCount ?? 0;
      });
      return n > 0;
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-delete] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  async deleteL0Expired(cutoffIso: string): Promise<number> {
    if (this.degraded) return 0;
    try {
      return await this.withClient(async (client) => {
        const expired = await client.query(
          `SELECT COUNT(*)::int AS cnt FROM l0_conversations WHERE recorded_at != '' AND recorded_at < $1`,
          [cutoffIso],
        );
        const expiredCount = num(expired.rows[0]?.cnt);
        if (expiredCount <= 0) return 0;
        const total = num((await client.query(`SELECT COUNT(*)::int AS cnt FROM l0_conversations`)).rows[0]?.cnt);
        if (total > 0 && expiredCount / total > 0.8) {
          this.logger?.warn?.(
            `${TAG} [L0-deleteExpired] BLOCKED: would delete ${expiredCount}/${total} (>80%) cutoff=${cutoffIso}`,
          );
          return 0;
        }
        const res = await client.query(
          `DELETE FROM l0_conversations WHERE recorded_at != '' AND recorded_at < $1`,
          [cutoffIso],
        );
        return res.rowCount ?? expiredCount;
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-deleteExpired] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return 0;
    }
  }

  async countL0(filter?: L0CountFilter): Promise<number> {
    if (this.degraded) return 0;
    try {
      const { clause, params } = this.l0Where(filter);
      return await this.withClient(async (client) => {
        const sql = `SELECT COUNT(*)::int AS cnt FROM l0_conversations${clause ? ` WHERE ${clause}` : ""}`;
        const res = await client.query(sql, params);
        return num(res.rows[0]?.cnt);
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} countL0 failed: ${err instanceof Error ? err.message : String(err)}`);
      return 0;
    }
  }

  async queryL0ForL1(sessionKey: string, afterRecordedAtMs?: number, limit = 50): Promise<L0QueryRow[]> {
    if (this.degraded) return [];
    try {
      return await this.withClient(async (client) => {
        if (afterRecordedAtMs && afterRecordedAtMs > 0) {
          const afterIso = new Date(afterRecordedAtMs).toISOString();
          const res = await client.query(
            `SELECT record_id, session_key, session_id, team_id, task_id, user_id, agent_id,
                    role, message_text, recorded_at, timestamp
             FROM l0_conversations
             WHERE session_key = $1 AND recorded_at > $2
             ORDER BY recorded_at ASC
             LIMIT $3`,
            [sessionKey, afterIso, limit],
          );
          return res.rows.map((r) => this.mapL0Query(r));
        }
        const res = await client.query(
          `SELECT record_id, session_key, session_id, team_id, task_id, user_id, agent_id,
                  role, message_text, recorded_at, timestamp
           FROM l0_conversations
           WHERE session_key = $1
           ORDER BY recorded_at ASC
           LIMIT $2`,
          [sessionKey, limit],
        );
        return res.rows.map((r) => this.mapL0Query(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-query] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async queryL0GroupedBySessionId(
    sessionKey: string,
    afterRecordedAtMs?: number,
    limit = 50,
  ): Promise<L0SessionGroup[]> {
    const rows = await this.queryL0ForL1(sessionKey, afterRecordedAtMs, limit);
    const groupMap = new Map<string, L0SessionGroup>();
    for (const row of rows) {
      const sid = row.session_id || "";
      const teamId = row.team_id || undefined;
      const taskId = row.task_id || undefined;
      const userId = row.user_id || "";
      const agentId = row.agent_id || "";
      const groupKey = `${teamId ?? ""}\u0000${userId}\u0000${agentId}\u0000${sid}\u0000${taskId ?? ""}`;
      let group = groupMap.get(groupKey);
      if (!group) {
        group = { sessionId: sid, teamId, taskId, userId, agentId, messages: [] };
        groupMap.set(groupKey, group);
      }
      group.messages.push({
        id: row.record_id,
        role: row.role,
        content: row.message_text,
        timestamp: row.timestamp,
        recordedAtMs: row.recorded_at ? Date.parse(row.recorded_at) || 0 : 0,
      });
    }
    return [...groupMap.values()].filter((g) => g.messages.length > 0);
  }

  async getAllL0Texts(): Promise<Array<{ record_id: string; message_text: string; recorded_at: string }>> {
    if (this.degraded) return [];
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(`SELECT record_id, message_text, recorded_at FROM l0_conversations`);
        return res.rows.map((r) => ({
          record_id: str(r.record_id),
          message_text: str(r.message_text),
          recorded_at: iso(r.recorded_at),
        }));
      });
    } catch {
      return [];
    }
  }

  async searchL0Vector(
    queryEmbedding: Float32Array,
    topK = 5,
    _queryText?: string,
    filter?: IsolationFilter,
  ): Promise<L0SearchResult[]> {
    if (this.degraded || this.dimensions <= 0) return [];
    try {
      const lit = toVectorLiteral(queryEmbedding);
      const retrieve = filter ? Math.max(topK * 5, topK) : topK;
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, session_key, session_id, team_id, task_id, user_id, agent_id,
                  role, message_text, recorded_at, timestamp,
                  1 - (embedding <=> $1::vector) AS score
           FROM l0_conversations
           WHERE embedding IS NOT NULL
           ORDER BY embedding <=> $1::vector
           LIMIT $2`,
          [lit, retrieve],
        );
        return res.rows
          .filter((r) => rowMatchesIsolation(r, filter))
          .slice(0, topK)
          .map((r) => this.mapL0Search(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-vector] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async searchL0Fts(ftsQuery: string, limit = 10, filter?: IsolationFilter): Promise<L0FtsResult[]> {
    if (this.degraded || !ftsQuery) return [];
    const text = ftsQueryToText(ftsQuery);
    const patterns = ftsIlikePatterns(text);
    if (!text || patterns.length === 0) return [];
    const retrieve = filter ? Math.max(limit * 5, limit) : limit;
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, session_key, session_id, team_id, task_id, user_id, agent_id,
                  role, message_text, recorded_at, timestamp,
                  ts_rank_cd(
                    to_tsvector('simple', coalesce(message_text, '')),
                    plainto_tsquery('simple', $1)
                  ) AS score
           FROM l0_conversations
           WHERE to_tsvector('simple', coalesce(message_text, '')) @@ plainto_tsquery('simple', $1)
              OR message_text ILIKE ANY($2::text[])
           ORDER BY score DESC, recorded_at DESC
           LIMIT $3`,
          [text, patterns, retrieve],
        );
        return res.rows
          .filter((r) => rowMatchesIsolation(r, filter))
          .slice(0, limit)
          .map((r) => this.mapL0Search(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [L0-fts] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async searchL0Hybrid(params: {
    query?: string;
    queryEmbedding?: Float32Array;
    sparseVector?: Array<[number, number]>;
    topK?: number;
    filter?: IsolationFilter;
  }): Promise<L0SearchResult[]> {
    const topK = params.topK ?? 10;
    const queryText = params.query ?? "";
    if (queryText) return this.searchL0Fts(queryText, topK, params.filter);
    if (params.queryEmbedding) return this.searchL0Vector(params.queryEmbedding, topK, undefined, params.filter);
    return [];
  }

  async reindexAll(
    embedFn: (text: string) => Promise<Float32Array>,
    onProgress?: (done: number, total: number, layer: "L1" | "L0") => void,
  ): Promise<{ l1Count: number; l0Count: number }> {
    if (this.degraded) return { l1Count: 0, l0Count: 0 };
    try {
      const l1Rows = await this.getAllL1Texts();
      let l1Count = 0;
      for (const row of l1Rows) {
        try {
          const embedding = await embedFn(row.content);
          if (embedding.length > 0) {
            await this.withClient(async (client) => {
              await client.query(
                `UPDATE l1_records SET embedding = $2::vector WHERE record_id = $1`,
                [row.record_id, toVectorLiteral(embedding)],
              );
            });
            l1Count++;
          }
        } catch { /* skip row */ }
        onProgress?.(l1Count, l1Rows.length, "L1");
      }
      const l0Rows = await this.getAllL0Texts();
      let l0Count = 0;
      for (const row of l0Rows) {
        try {
          const embedding = await embedFn(row.message_text);
          if (embedding.length > 0) {
            await this.updateL0Embedding(row.record_id, embedding);
            l0Count++;
          }
        } catch { /* skip row */ }
        onProgress?.(l0Count, l0Rows.length, "L0");
      }
      return { l1Count, l0Count };
    } catch (err) {
      this.logger?.warn?.(`${TAG} reindexAll failed: ${err instanceof Error ? err.message : String(err)}`);
      return { l1Count: 0, l0Count: 0 };
    }
  }

  async queryL0Paginated(filter: L0PaginatedFilter): Promise<L0PaginatedResult> {
    if (this.degraded) return { rows: [], total: 0 };
    try {
      const { clause, params } = this.l0Where(filter);
      const where = clause ? `WHERE ${clause}` : "";
      const limit = Math.max(filter.limit, 0);
      const offset = Math.max(filter.offset, 0);
      return await this.withClient(async (client) => {
        const totalRes = await client.query(
          `SELECT COUNT(*)::int AS cnt FROM l0_conversations ${where}`,
          params,
        );
        const rowsRes = await client.query(
          `SELECT record_id, session_key, session_id, team_id, task_id, user_id, agent_id,
                  role, message_text, recorded_at, timestamp
           FROM l0_conversations ${where}
           ORDER BY timestamp ASC, record_id ASC
           LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
          [...params, limit, offset],
        );
        return {
          total: num(totalRes.rows[0]?.cnt),
          rows: rowsRes.rows.map((r) => this.mapL0Query(r)),
        };
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} queryL0Paginated failed: ${err instanceof Error ? err.message : String(err)}`);
      return { rows: [], total: 0 };
    }
  }

  async queryL1Paginated(filter: L1PaginatedFilter): Promise<L1PaginatedResult> {
    if (this.degraded) return { rows: [], total: 0 };
    try {
      const { clause, params } = this.l1CountWhere(filter);
      const where = clause ? `WHERE ${clause}` : "";
      const limit = Math.max(filter.limit, 0);
      const offset = Math.max(filter.offset, 0);
      return await this.withClient(async (client) => {
        const totalRes = await client.query(
          `SELECT COUNT(*)::int AS cnt FROM l1_records ${where}`,
          params,
        );
        const rowsRes = await client.query(
          `SELECT record_id, content, type, priority, scene_name, session_key, session_id,
                  team_id, task_id, user_id, agent_id, version, timestamp_str, timestamp_start,
                  timestamp_end, created_time, updated_time, metadata_json
           FROM l1_records ${where}
           ORDER BY updated_time ASC, record_id ASC
           LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
          [...params, limit, offset],
        );
        return {
          total: num(totalRes.rows[0]?.cnt),
          rows: rowsRes.rows.map((r) => this.mapL1Row(r)),
        };
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} queryL1Paginated failed: ${err instanceof Error ? err.message : String(err)}`);
      return { rows: [], total: 0 };
    }
  }

  async deleteL0BySession(sessionId: string, filter?: IsolationFilter): Promise<number> {
    if (this.degraded || !sessionId) return 0;
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(
          `SELECT record_id, team_id, user_id, agent_id, session_id, task_id, session_key
           FROM l0_conversations WHERE session_id = $1 OR session_key = $1`,
          [sessionId],
        );
        let deleted = 0;
        for (const row of res.rows) {
          if (filter && !rowMatchesIsolation(row, filter)) continue;
          const del = await client.query(`DELETE FROM l0_conversations WHERE record_id = $1`, [row.record_id]);
          deleted += del.rowCount ?? 0;
        }
        return deleted;
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} deleteL0BySession failed: ${err instanceof Error ? err.message : String(err)}`);
      return 0;
    }
  }

  async clearMemoryContent(filter: MemoryContentClearFilter): Promise<MemoryContentClearResult> {
    const teamId = (filter?.teamId ?? "").trim();
    const agentId = (filter?.agentId ?? "").trim();
    if (!teamId || !agentId) {
      throw new Error("clearMemoryContent requires non-empty teamId and agentId");
    }
    const empty: MemoryContentClearResult = { l0Deleted: 0, l1Deleted: 0, profilesDeleted: 0 };
    if (this.degraded) return empty;
    const userId = filter.userId?.trim() || undefined;
    try {
      return await this.withClient(async (client) => {
        const params = userId ? [teamId, agentId, userId] : [teamId, agentId];
        const where = userId ? "team_id = $1 AND agent_id = $2 AND user_id = $3" : "team_id = $1 AND agent_id = $2";
        const l0 = await client.query(`DELETE FROM l0_conversations WHERE ${where}`, params);
        const l1 = await client.query(`DELETE FROM l1_records WHERE ${where}`, params);
        // profiles are team+agent scoped (do not narrow by user), matching TCVDB.
        const profiles = await client.query(
          `DELETE FROM profiles WHERE team_id = $1 AND agent_id = $2`,
          [teamId, agentId],
        );
        return {
          l0Deleted: l0.rowCount ?? 0,
          l1Deleted: l1.rowCount ?? 0,
          profilesDeleted: profiles.rowCount ?? 0,
        };
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} clearMemoryContent failed: ${err instanceof Error ? err.message : String(err)}`);
      return empty;
    }
  }

  // ── Profiles (open service path) ─────────────────────────

  async pullProfiles(): Promise<ProfileRecord[]> {
    if (this.degraded) return [];
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(`SELECT * FROM profiles`);
        return res.rows.map((r) => this.mapProfile(r));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [profiles-pull] FAILED: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  async queryProfilesByIds(ids: string[]): Promise<ProfileRecord[]> {
    if (this.degraded || ids.length === 0) return [];
    try {
      return await this.withClient(async (client) => {
        const res = await client.query(`SELECT * FROM profiles WHERE id = ANY($1::text[])`, [ids]);
        return res.rows.map((r) => this.mapProfile(r));
      });
    } catch {
      return [];
    }
  }

  async countProfiles(filter?: ProfileCountFilter): Promise<number> {
    if (this.degraded) return 0;
    try {
      return await this.withClient(async (client) => {
        const parts: string[] = [];
        const params: unknown[] = [];
        if (filter?.type) { params.push(filter.type); parts.push(`type = $${params.length}`); }
        if (filter?.teamId !== undefined) { params.push(filter.teamId); parts.push(`team_id = $${params.length}`); }
        if (filter?.userId !== undefined) { params.push(filter.userId); parts.push(`user_id = $${params.length}`); }
        if (filter?.agentId !== undefined) { params.push(filter.agentId); parts.push(`agent_id = $${params.length}`); }
        if (filter?.pathPrefix) { params.push(`${filter.pathPrefix}%`); parts.push(`filename LIKE $${params.length}`); }
        const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
        const res = await client.query(`SELECT COUNT(*)::int AS cnt FROM profiles ${where}`, params);
        return num(res.rows[0]?.cnt);
      });
    } catch {
      return 0;
    }
  }

  async syncProfiles(records: ProfileSyncRecord[]): Promise<void> {
    if (this.degraded || records.length === 0) return;
    try {
      await this.withClient(async (client) => {
        const now = Date.now();
        for (const record of records) {
          const current = await client.query(
            `SELECT content_md5, version, created_at_ms FROM profiles WHERE id = $1`,
            [record.id],
          );
          const row = current.rows[0];
          if (!row) {
            await client.query(
              `INSERT INTO profiles (
                id, type, filename, content, content_md5, team_id, agent_id, user_id,
                version, created_at_ms, updated_at_ms
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
              [
                record.id, record.type, record.filename, record.content, record.contentMd5,
                record.teamId ?? "", record.agentId ?? "", record.userId ?? "",
                record.version ?? 0, record.createdAtMs > 0 ? record.createdAtMs : now, now,
              ],
            );
            continue;
          }
          if (str(row.content_md5) === record.contentMd5) continue;
          const currentVersion = num(row.version);
          if ((record.baselineVersion ?? 0) !== currentVersion) {
            this.logger?.warn?.(
              `${TAG} [profiles-sync] Conflict for ${record.filename}: remote ${currentVersion} != baseline ${record.baselineVersion ?? 0}`,
            );
            continue;
          }
          await client.query(
            `UPDATE profiles SET type=$2, filename=$3, content=$4, content_md5=$5,
                    team_id=$6, agent_id=$7, user_id=$8, version=$9, updated_at_ms=$10
             WHERE id = $1`,
            [
              record.id, record.type, record.filename, record.content, record.contentMd5,
              record.teamId ?? "", record.agentId ?? "", record.userId ?? "",
              currentVersion + 1, now,
            ],
          );
        }
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [profiles-sync] FAILED: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  async deleteProfiles(recordIds: string[]): Promise<void> {
    if (this.degraded || recordIds.length === 0) return;
    try {
      await this.withClient(async (client) => {
        await client.query(`DELETE FROM profiles WHERE id = ANY($1::text[])`, [recordIds]);
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} [profiles-delete] FAILED: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  async appendAudit(entry: AuditEntry): Promise<void> {
    if (this.degraded) return;
    try {
      await this.withClient(async (client) => {
        await client.query(
          `INSERT INTO memory_audit (
            audit_id, record_id, layer, action, team_id, agent_id, user_id, task_id,
            version, updated_at_ms, request_id
          ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
          ON CONFLICT (audit_id) DO UPDATE SET
            record_id=EXCLUDED.record_id, layer=EXCLUDED.layer, action=EXCLUDED.action,
            team_id=EXCLUDED.team_id, agent_id=EXCLUDED.agent_id, user_id=EXCLUDED.user_id,
            task_id=EXCLUDED.task_id, version=EXCLUDED.version,
            updated_at_ms=EXCLUDED.updated_at_ms, request_id=EXCLUDED.request_id`,
          [
            entry.audit_id, entry.record_id, entry.layer, entry.action,
            entry.team_id ?? null, entry.agent_id ?? null, entry.user_id ?? null, entry.task_id ?? null,
            entry.version, entry.updated_at_ms, entry.request_id ?? null,
          ],
        );
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} appendAudit failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  async queryAudit(filter: AuditQueryFilter): Promise<AuditEntry[]> {
    if (this.degraded) return [];
    try {
      return await this.withClient(async (client) => {
        const parts: string[] = [];
        const params: unknown[] = [];
        const add = (col: string, value: unknown) => {
          params.push(value);
          parts.push(`${col} = $${params.length}`);
        };
        if (filter.record_id !== undefined) add("record_id", filter.record_id);
        if (filter.layer !== undefined) add("layer", filter.layer);
        if (filter.action !== undefined) add("action", filter.action);
        if (filter.team_id !== undefined) add("team_id", filter.team_id);
        if (filter.agent_id !== undefined) add("agent_id", filter.agent_id);
        if (filter.user_id !== undefined) add("user_id", filter.user_id);
        if (filter.task_id !== undefined) add("task_id", filter.task_id);
        if (filter.since_ms !== undefined) {
          params.push(filter.since_ms);
          parts.push(`updated_at_ms >= $${params.length}`);
        }
        if (filter.until_ms !== undefined) {
          params.push(filter.until_ms);
          parts.push(`updated_at_ms <= $${params.length}`);
        }
        const where = parts.length ? `WHERE ${parts.join(" AND ")}` : "";
        const limit = Math.min(Math.max(filter.limit ?? 100, 1), 1000);
        const offset = Math.max(filter.offset ?? 0, 0);
        const res = await client.query(
          `SELECT * FROM memory_audit ${where} ORDER BY updated_at_ms DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
          [...params, limit, offset],
        );
        return res.rows.map((r) => ({
          audit_id: str(r.audit_id),
          record_id: str(r.record_id),
          layer: r.layer === "L2" || r.layer === "L3" ? r.layer : "L1",
          action: r.action === "delete" ? "delete" : "update",
          team_id: r.team_id ?? undefined,
          agent_id: r.agent_id ?? undefined,
          user_id: r.user_id ?? undefined,
          task_id: r.task_id ?? undefined,
          version: num(r.version),
          updated_at_ms: num(r.updated_at_ms),
          request_id: r.request_id ?? undefined,
        }));
      });
    } catch (err) {
      this.logger?.warn?.(`${TAG} queryAudit failed: ${err instanceof Error ? err.message : String(err)}`);
      return [];
    }
  }

  // ── mappers / where helpers ──────────────────────────────

  private l0Where(filter?: L0CountFilter): { clause: string; params: unknown[] } {
    const parts: string[] = [];
    const params: unknown[] = [];
    if (!filter) return { clause: "", params };
    if (filter.sessionId) {
      params.push(filter.sessionId, filter.sessionId);
      parts.push(`(session_key = $${params.length - 1} OR session_id = $${params.length})`);
    }
    if (filter.teamId !== undefined) { params.push(filter.teamId); parts.push(`team_id = $${params.length}`); }
    if (filter.userId !== undefined) { params.push(filter.userId); parts.push(`user_id = $${params.length}`); }
    if (filter.agentId !== undefined) { params.push(filter.agentId); parts.push(`agent_id = $${params.length}`); }
    if (filter.taskId !== undefined) { params.push(filter.taskId); parts.push(`task_id = $${params.length}`); }
    if (filter.timeStartMs !== undefined) { params.push(filter.timeStartMs); parts.push(`timestamp >= $${params.length}`); }
    if (filter.timeEndMs !== undefined) { params.push(filter.timeEndMs); parts.push(`timestamp <= $${params.length}`); }
    return { clause: parts.join(" AND "), params };
  }

  private l1CountWhere(filter?: L1CountFilter): { clause: string; params: unknown[] } {
    const parts: string[] = [];
    const params: unknown[] = [];
    if (!filter) return { clause: "", params };
    if (filter.type) { params.push(filter.type); parts.push(`type = $${params.length}`); }
    if (filter.sessionId) { params.push(filter.sessionId); parts.push(`session_id = $${params.length}`); }
    if (filter.teamId !== undefined) { params.push(filter.teamId); parts.push(`team_id = $${params.length}`); }
    if (filter.userId !== undefined) { params.push(filter.userId); parts.push(`user_id = $${params.length}`); }
    if (filter.agentId !== undefined) { params.push(filter.agentId); parts.push(`agent_id = $${params.length}`); }
    if (filter.taskId !== undefined) { params.push(filter.taskId); parts.push(`task_id = $${params.length}`); }
    if (filter.timeStart) { params.push(filter.timeStart); parts.push(`updated_time >= $${params.length}`); }
    if (filter.timeEnd) { params.push(filter.timeEnd); parts.push(`updated_time <= $${params.length}`); }
    return { clause: parts.join(" AND "), params };
  }

  private mapL1Row(r: Record<string, unknown>): L1RecordRow {
    return {
      record_id: str(r.record_id),
      content: str(r.content),
      type: str(r.type),
      priority: num(r.priority),
      scene_name: str(r.scene_name),
      session_key: str(r.session_key),
      session_id: str(r.session_id),
      team_id: str(r.team_id),
      task_id: str(r.task_id),
      user_id: str(r.user_id),
      agent_id: str(r.agent_id),
      version: num(r.version),
      timestamp_str: str(r.timestamp_str),
      timestamp_start: str(r.timestamp_start),
      timestamp_end: str(r.timestamp_end),
      created_time: iso(r.created_time as string),
      updated_time: iso(r.updated_time as string),
      metadata_json: str(r.metadata_json, "{}"),
    };
  }

  private mapL1Search(r: Record<string, unknown>): L1SearchResult {
    const row = this.mapL1Row(r);
    return {
      ...row,
      score: num(r.score),
    };
  }

  private mapL0Query(r: Record<string, unknown>): L0QueryRow {
    return {
      record_id: str(r.record_id),
      session_key: str(r.session_key),
      session_id: str(r.session_id),
      team_id: str(r.team_id),
      task_id: str(r.task_id),
      user_id: str(r.user_id),
      agent_id: str(r.agent_id),
      role: str(r.role),
      message_text: str(r.message_text),
      recorded_at: iso(r.recorded_at as string),
      timestamp: num(r.timestamp),
    };
  }

  private mapL0Search(r: Record<string, unknown>): L0SearchResult {
    const row = this.mapL0Query(r);
    return { ...row, score: num(r.score) };
  }

  private mapProfile(r: Record<string, unknown>): ProfileRecord {
    return {
      id: str(r.id),
      type: r.type === "l3" ? "l3" : "l2",
      filename: str(r.filename),
      content: str(r.content),
      contentMd5: str(r.content_md5),
      teamId: str(r.team_id) || undefined,
      agentId: str(r.agent_id) || undefined,
      userId: str(r.user_id) || undefined,
      version: num(r.version),
      createdAtMs: num(r.created_at_ms),
      updatedAtMs: num(r.updated_at_ms),
    };
  }

  // ── Knowledge entity ─────────────────────────────────────

  private knowledgeFromRow(row: Record<string, unknown>): KnowledgeEntity {
    return {
      knowledge_id: str(row.knowledge_id),
      type: (str(row.type) as KnowledgeType) || "wiki",
      service_url: str(row.service_url),
      name: str(row.name),
      summary: row.summary == null ? null : String(row.summary),
      team_id: str(row.team_id),
      agent_id: str(row.agent_id),
      user_id: row.user_id == null ? null : String(row.user_id),
      repo_url: row.repo_url == null ? undefined : String(row.repo_url),
      branch: row.branch == null ? undefined : String(row.branch),
      created_at: str(row.created_at),
      updated_at: str(row.updated_at),
    };
  }

  async createKnowledge(input: Omit<KnowledgeEntity, "created_at" | "updated_at">): Promise<KnowledgeEntity> {
    if (this.degraded) throw new Error("PostgresMemoryStore degraded");
    const now = new Date().toISOString();
    await this.withClient(async (client) => {
      const existing = await client.query(
        "SELECT created_at FROM entity_knowledge WHERE knowledge_id=$1",
        [input.knowledge_id],
      );
      const createdAt = existing.rows[0]?.created_at
        ? String(existing.rows[0].created_at)
        : now;
      await client.query(
        `INSERT INTO entity_knowledge (
          knowledge_id, type, service_url, name, summary, team_id, agent_id,
          user_id, repo_url, branch, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (knowledge_id) DO UPDATE SET
          type=EXCLUDED.type,
          service_url=EXCLUDED.service_url,
          name=EXCLUDED.name,
          summary=EXCLUDED.summary,
          team_id=EXCLUDED.team_id,
          agent_id=EXCLUDED.agent_id,
          user_id=EXCLUDED.user_id,
          repo_url=EXCLUDED.repo_url,
          branch=EXCLUDED.branch,
          updated_at=EXCLUDED.updated_at`,
        [
          input.knowledge_id,
          input.type,
          input.service_url,
          input.name,
          input.summary ?? null,
          input.team_id,
          input.agent_id ?? "",
          input.user_id ?? null,
          input.repo_url ?? null,
          input.branch ?? null,
          createdAt,
          now,
        ],
      );
    });
    const got = await this.getKnowledge(input.knowledge_id);
    if (!got) throw new Error("createKnowledge failed");
    return got;
  }

  async getKnowledge(knowledgeId: string): Promise<KnowledgeEntity | null> {
    if (this.degraded) return null;
    try {
      return await this.withClient(async (client) => {
        const r = await client.query(
          "SELECT * FROM entity_knowledge WHERE knowledge_id=$1",
          [knowledgeId],
        );
        const row = r.rows[0];
        return row ? this.knowledgeFromRow(row) : null;
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} getKnowledge failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return null;
    }
  }

  async updateKnowledge(
    knowledgeId: string,
    patch: Partial<Pick<KnowledgeEntity, "name" | "summary" | "service_url" | "repo_url" | "branch">>,
  ): Promise<KnowledgeEntity | null> {
    const current = await this.getKnowledge(knowledgeId);
    if (!current) return null;
    return this.createKnowledge({
      knowledge_id: current.knowledge_id,
      type: current.type,
      service_url: patch.service_url ?? current.service_url,
      name: patch.name ?? current.name,
      summary: patch.summary !== undefined ? patch.summary : current.summary,
      team_id: current.team_id,
      agent_id: current.agent_id ?? "",
      user_id: current.user_id,
      repo_url: patch.repo_url !== undefined ? patch.repo_url : current.repo_url,
      branch: patch.branch !== undefined ? patch.branch : current.branch,
    });
  }

  async deleteKnowledge(knowledgeIds: string[], teamId?: string): Promise<BatchDeleteResult> {
    const result: BatchDeleteResult = { deleted_ids: [], failed: [] };
    if (this.degraded) {
      for (const id of knowledgeIds) result.failed.push({ id, reason: "degraded" });
      return result;
    }
    for (const id of knowledgeIds) {
      const row = await this.getKnowledge(id);
      if (!row) {
        result.failed.push({ id, reason: "not_found" });
        continue;
      }
      if (teamId && row.team_id !== teamId) {
        result.failed.push({ id, reason: "team_mismatch" });
        continue;
      }
      try {
        await this.withClient(async (client) => {
          await client.query("DELETE FROM entity_knowledge WHERE knowledge_id=$1", [id]);
        });
        result.deleted_ids.push(id);
      } catch {
        result.failed.push({ id, reason: "delete_failed" });
      }
    }
    return result;
  }

  async listKnowledge(input: {
    team_id: string;
    type?: KnowledgeType;
    knowledge_ids?: string[];
    limit?: number;
    offset?: number;
  }): Promise<KnowledgeListResult> {
    if (this.degraded) return { items: [], total: 0 };
    const limit = Math.min(Math.max(input.limit ?? 20, 1), 1000);
    const offset = Math.max(input.offset ?? 0, 0);
    const ids = input.knowledge_ids;
    if (ids && ids.length === 0) return { items: [], total: 0 };

    try {
      return await this.withClient(async (client) => {
        const where: string[] = ["team_id=$1"];
        const args: unknown[] = [input.team_id];
        let idx = 2;
        if (input.type) {
          where.push(`type=$${idx++}`);
          args.push(input.type);
        }
        if (ids && ids.length > 0) {
          where.push(`knowledge_id = ANY($${idx++}::text[])`);
          args.push(ids);
        }
        const whereSql = where.join(" AND ");
        const countR = await client.query(
          `SELECT COUNT(*)::int AS total FROM entity_knowledge WHERE ${whereSql}`,
          args,
        );
        const rowsR = await client.query(
          `SELECT * FROM entity_knowledge WHERE ${whereSql}
           ORDER BY updated_at DESC LIMIT $${idx} OFFSET $${idx + 1}`,
          [...args, limit, offset],
        );
        return {
          items: rowsR.rows.map((r) => this.knowledgeFromRow(r)),
          total: Number(countR.rows[0]?.total ?? 0),
        };
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} listKnowledge failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return { items: [], total: 0 };
    }
  }

  // ── Memory prompts ───────────────────────────────────────

  async createMemoryPrompt(record: MemoryPromptRecord): Promise<MemoryPromptRecord> {
    if (this.degraded) throw new Error("PostgresMemoryStore degraded");
    await this.withClient(async (client) => {
      await client.query(
        `INSERT INTO memory_prompts (
          memory_prompt_id, name, layer, prompt, version, status,
          created_by, updated_by, created_at_ms, updated_at_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          record.memory_prompt_id,
          record.name,
          record.layer,
          record.prompt,
          record.version,
          record.status,
          record.created_by ?? null,
          record.updated_by ?? null,
          record.created_at_ms,
          record.updated_at_ms,
        ],
      );
    });
    return record;
  }

  async getMemoryPrompts(ids: string[]): Promise<MemoryPromptRecord[]> {
    if (this.degraded || ids.length === 0) return [];
    try {
      return await this.withClient(async (client) => {
        const r = await client.query(
          "SELECT * FROM memory_prompts WHERE memory_prompt_id = ANY($1::text[])",
          [ids],
        );
        return r.rows as MemoryPromptRecord[];
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} getMemoryPrompts failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return [];
    }
  }

  async listMemoryPrompts(filter: MemoryPromptListFilter): Promise<MemoryPromptRecord[]> {
    if (this.degraded) return [];
    const conds = ["status = 'active'"];
    const args: unknown[] = [];
    let idx = 1;
    if (filter.layer) {
      conds.push(`layer = $${idx++}`);
      args.push(filter.layer);
    }
    const order = filter.timeOrder === "asc" ? "ASC" : "DESC";
    const limit = Math.min(Math.max(filter.limit ?? 20, 1), 100);
    const offset = Math.max(filter.offset ?? 0, 0);
    try {
      return await this.withClient(async (client) => {
        const r = await client.query(
          `SELECT * FROM memory_prompts
           WHERE ${conds.join(" AND ")}
           ORDER BY updated_at_ms ${order}, memory_prompt_id ${order}
           LIMIT $${idx} OFFSET $${idx + 1}`,
          [...args, limit, offset],
        );
        return r.rows as MemoryPromptRecord[];
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} listMemoryPrompts failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return [];
    }
  }

  async updateMemoryPrompt(
    id: string,
    patch: { name?: string; prompt?: string; updated_by?: string; updated_at_ms: number },
  ): Promise<MemoryPromptRecord | null> {
    const current = (await this.getMemoryPrompts([id]))[0];
    if (!current || current.status !== "active") return null;
    const sameName = patch.name === undefined || patch.name === current.name;
    const samePrompt = patch.prompt === undefined || patch.prompt === current.prompt;
    if (sameName && samePrompt) return current;

    try {
      await this.withClient(async (client) => {
        const sets = ["version = version + 1", "updated_at_ms = $1", "updated_by = $2"];
        const args: unknown[] = [patch.updated_at_ms, patch.updated_by ?? null];
        let idx = 3;
        if (patch.name !== undefined) {
          sets.push(`name = $${idx++}`);
          args.push(patch.name);
        }
        if (patch.prompt !== undefined) {
          sets.push(`prompt = $${idx++}`);
          args.push(patch.prompt);
        }
        args.push(id);
        await client.query(
          `UPDATE memory_prompts SET ${sets.join(", ")}
           WHERE memory_prompt_id = $${idx} AND status = 'active'`,
          args,
        );
      });
      return (await this.getMemoryPrompts([id]))[0] ?? null;
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} updateMemoryPrompt failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return null;
    }
  }

  // ── Memory generation refs ───────────────────────────────

  async upsertMemoryGenerationRefs(records: MemoryGenerationRefRecord[]): Promise<void> {
    if (this.degraded || records.length === 0) return;
    try {
      await this.withClient(async (client) => {
        await client.query("BEGIN");
        try {
          for (const record of records) {
            await client.query(
              `INSERT INTO memory_generation_refs (
                generation_ref_id, layer, memory_id, generation_id, generation_log_id,
                generation_log_key, memory_prompt_id, memory_prompt_version,
                memory_prompt_source, created_at_ms
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
              ON CONFLICT (generation_ref_id) DO UPDATE SET
                memory_id=EXCLUDED.memory_id,
                generation_id=EXCLUDED.generation_id,
                generation_log_id=EXCLUDED.generation_log_id,
                generation_log_key=EXCLUDED.generation_log_key,
                memory_prompt_id=EXCLUDED.memory_prompt_id,
                memory_prompt_version=EXCLUDED.memory_prompt_version,
                memory_prompt_source=EXCLUDED.memory_prompt_source,
                created_at_ms=EXCLUDED.created_at_ms`,
              [
                record.generation_ref_id,
                record.layer,
                record.memory_id,
                record.generation_id,
                record.generation_log_id,
                record.generation_log_key,
                record.memory_prompt_id,
                record.memory_prompt_version,
                record.memory_prompt_source,
                record.created_at_ms,
              ],
            );
          }
          await client.query("COMMIT");
        } catch (err) {
          await client.query("ROLLBACK");
          throw err;
        }
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} upsertMemoryGenerationRefs failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  async getMemoryGenerationRef(
    layer: MemoryGenerationLayer,
    memoryId: string,
  ): Promise<MemoryGenerationRefRecord | null> {
    if (this.degraded) return null;
    const id = buildMemoryGenerationRefId(layer, memoryId);
    try {
      return await this.withClient(async (client) => {
        const r = await client.query(
          `SELECT * FROM memory_generation_refs
           WHERE generation_ref_id=$1 AND layer=$2 AND memory_id=$3`,
          [id, layer, memoryId],
        );
        return (r.rows[0] as MemoryGenerationRefRecord | undefined) ?? null;
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} getMemoryGenerationRef failed: ${err instanceof Error ? err.message : String(err)}`,
      );
      return null;
    }
  }
}
