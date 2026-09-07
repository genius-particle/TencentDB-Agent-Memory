/**
 * PostgresSkillStore — ISkillStore on Postgres + pgvector (open-service path).
 *
 * Skill rows live in the same per-instance schema as PostgresMemoryStore
 * (`mem_{instanceId}`). Dense = pgvector cosine; lexical = tsvector('simple')
 * + token ILIKE; hybrid merges dense + lexical when BM25 encoder is present.
 */

import type { Pool, PoolClient } from "pg";
import { randomBase62 } from "../../utils/short-id.js";
import type { BM25LocalEncoder } from "./bm25-local.js";
import type { StoreLogger } from "./types.js";
import {
  DEFAULT_SPARSE_DIMENSIONS,
  ftsIlikePatterns,
  toSparsevecLiteral,
  toVectorLiteral,
} from "./sparsevec.js";
import { FTS_CONTENT_MAX } from "../skill/skill-store-ddl.js";
import { SkillStoreError } from "../skill/skill-store.js";
import type {
  ISkillStore,
  ExpiredVersionMeta,
  SkillStoreCapabilities,
  SkillSearchResult,
} from "../skill/skill-store.interface.js";
import type {
  AppendVersionInput,
  ListSkillsOptions,
  SearchSkillsOptions,
  Skill,
  SkillManifestEntry,
  SkillStatus,
} from "../skill/types.js";

const TAG = "[memory-tdai][postgres-skill-store]";

export interface PostgresSkillStoreOptions {
  connectionString: string;
  schema: string;
  dimensions?: number;
  sparseDimensions?: number;
  bm25Encoder?: BM25LocalEncoder;
  logger?: StoreLogger;
  now?: () => number;
  ulid?: () => string;
}

function defaultUlid(): string {
  return randomBase62(12);
}

interface SkillRowRaw {
  row_id: string;
  skill_id: string;
  version: number;
  is_head: number;
  user_id: string;
  owner_agent_id: string;
  team_id: string;
  task_id: string;
  name: string;
  description: string;
  content: string;
  content_hash: string;
  manifest_json: string;
  storage_dir: string;
  status: string;
  metadata_json: string;
  created_at_ms: number;
  updated_at_ms: number;
}

function toSkill(raw: SkillRowRaw): Skill {
  let manifest: SkillManifestEntry[];
  try {
    manifest = JSON.parse(raw.manifest_json);
    if (!Array.isArray(manifest)) manifest = [];
  } catch {
    manifest = [];
  }
  return {
    row_id: raw.row_id,
    skill_id: raw.skill_id,
    version: raw.version,
    is_head: raw.is_head === 1,
    user_id: raw.user_id,
    owner_agent_id: raw.owner_agent_id,
    team_id: raw.team_id,
    task_id: raw.task_id,
    name: raw.name,
    description: raw.description,
    content: raw.content,
    content_hash: raw.content_hash,
    manifest,
    storage_dir: raw.storage_dir,
    status: raw.status as SkillStatus,
    metadata_json: raw.metadata_json,
    created_at_ms: raw.created_at_ms,
    updated_at_ms: raw.updated_at_ms,
  };
}

function ftsDocument(name: string, description: string, content: string): string {
  const trimmed = content.length > FTS_CONTENT_MAX ? content.slice(0, FTS_CONTENT_MAX) : content;
  return `${name} ${description} ${trimmed}`;
}

export class PostgresSkillStore implements ISkillStore {
  private pool: Pool | null = null;
  private readonly connectionString: string;
  private readonly schema: string;
  private readonly dimensions: number;
  private readonly sparseDimensions: number;
  private readonly bm25Encoder?: BM25LocalEncoder;
  private readonly logger?: StoreLogger;
  private readonly now: () => number;
  private readonly ulid: () => string;
  private degraded = false;
  private initPromise?: Promise<void>;
  private pgModule: typeof import("pg") | null = null;

  constructor(opts: PostgresSkillStoreOptions) {
    this.connectionString = opts.connectionString;
    this.schema = opts.schema;
    this.dimensions = Math.max(0, Math.floor(opts.dimensions ?? 0));
    this.sparseDimensions = opts.sparseDimensions ?? DEFAULT_SPARSE_DIMENSIONS;
    this.bm25Encoder = opts.bm25Encoder;
    this.logger = opts.logger;
    this.now = opts.now ?? (() => Date.now());
    this.ulid = opts.ulid ?? defaultUlid;
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

  init(): void {
    if (this.initPromise) return;
    this.initPromise = this.initAsync().catch((err) => {
      this.degraded = true;
      this.logger?.error?.(
        `${TAG} Init failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    });
  }

  async ensureReady(): Promise<void> {
    this.init();
    await this.initPromise;
  }

  private async initAsync(): Promise<void> {
    await this.withClient(async (client) => {
      try {
        await client.query("CREATE EXTENSION IF NOT EXISTS vector");
      } catch (err) {
        this.logger?.warn?.(
          `${TAG} CREATE EXTENSION vector failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        throw err;
      }

      const sparseType = `sparsevec(${this.sparseDimensions})`;
      const denseType = this.dimensions > 0 ? `vector(${this.dimensions})` : "vector";

      await client.query(`
        CREATE TABLE IF NOT EXISTS skills (
          row_id TEXT PRIMARY KEY,
          skill_id TEXT NOT NULL,
          version INTEGER NOT NULL,
          is_head INTEGER NOT NULL DEFAULT 1,
          user_id TEXT NOT NULL,
          owner_agent_id TEXT NOT NULL,
          team_id TEXT NOT NULL,
          task_id TEXT NOT NULL DEFAULT '',
          name TEXT NOT NULL,
          description TEXT NOT NULL DEFAULT '',
          content TEXT NOT NULL,
          content_hash TEXT NOT NULL,
          manifest_json TEXT NOT NULL DEFAULT '[]',
          storage_dir TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at_ms BIGINT NOT NULL,
          updated_at_ms BIGINT NOT NULL,
          embedding ${denseType},
          sparse_embedding ${sparseType},
          fts tsvector,
          UNIQUE(skill_id, version)
        )
      `);

      await client.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_skills_team_agent_name_head
          ON skills(team_id, owner_agent_id, name)
          WHERE is_head=1 AND status='active'
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_team_head
          ON skills(team_id, is_head, status)
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_owner_head
          ON skills(owner_agent_id, is_head, status)
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_user
          ON skills(user_id, is_head)
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_skill_version
          ON skills(skill_id, version DESC)
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_task_audit
          ON skills(task_id, created_at_ms DESC)
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_skills_fts ON skills USING gin(fts)
      `);
    });
    this.degraded = false;
    this.logger?.debug?.(`${TAG} Initialized schema=${this.schema} dimensions=${this.dimensions}`);
  }

  isDegraded(): boolean {
    return this.degraded;
  }

  getCapabilities(): SkillStoreCapabilities {
    const vectorSearch = this.dimensions > 0 && !this.degraded;
    const hasBm25 = !!this.bm25Encoder && !this.degraded;
    return {
      vectorSearch,
      ftsSearch: !this.degraded,
      nativeHybridSearch: vectorSearch && hasBm25,
      sparseVectors: hasBm25,
    };
  }

  close(): void {
    this.degraded = true;
    const pool = this.pool;
    this.pool = null;
    void pool?.end().catch(() => undefined);
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

  async appendVersion(input: AppendVersionInput): Promise<Skill> {
    await this.ensureReady();
    if (this.degraded) throw new Error("PostgresSkillStore degraded");

    const tid = input.team_id ?? "default";
    const head = await this.getHead(input.skill_id, tid);

    if (!head) {
      const oid = input.owner_agent_id ?? "default";
      const dup = await this.withClient(async (client) => {
        const r = await client.query(
          `SELECT * FROM skills
           WHERE team_id=$1 AND owner_agent_id=$2 AND name=$3
             AND is_head=1 AND status='active'
           LIMIT 1`,
          [tid, oid, input.name],
        );
        return r.rows[0] as SkillRowRaw | undefined;
      });
      if (dup) {
        throw new SkillStoreError(
          "SKILL_NAME_DUPLICATE",
          `name '${input.name}' already exists for agent in team`,
        );
      }
    } else if (head.name !== input.name) {
      throw new SkillStoreError("SKILL_NAME_DUPLICATE", "name change is not allowed across versions");
    }

    const newVersion = head ? head.version + 1 : 1;
    const ownerForRow = head ? head.owner_agent_id : (input.owner_agent_id ?? "default");
    const userIdForRow = input.user_id ?? "default";
    const ts = this.now();
    const newRowId = this.ulid();
    const ftsContent = ftsDocument(input.name, input.description, input.content);
    const sparse = this.encodeSparse(ftsContent, false);

    await this.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        if (head) {
          await client.query(
            "UPDATE skills SET is_head=0 WHERE skill_id=$1 AND version=$2",
            [head.skill_id, head.version],
          );
        }
        await client.query(
          `INSERT INTO skills (
            row_id, skill_id, version, is_head,
            user_id, owner_agent_id, team_id, task_id,
            name, description, content, content_hash, manifest_json, storage_dir,
            status, metadata_json, created_at_ms, updated_at_ms,
            sparse_embedding, fts
          ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
            $19::sparsevec,
            to_tsvector('simple', $20)
          )`,
          [
            newRowId,
            input.skill_id,
            newVersion,
            1,
            userIdForRow,
            ownerForRow,
            tid,
            input.task_id ?? "default",
            input.name,
            input.description,
            input.content,
            input.content_hash,
            JSON.stringify(input.manifest ?? []),
            input.storage_dir,
            "active",
            input.metadata_json ?? "{}",
            ts,
            ts,
            sparse,
            ftsContent,
          ],
        );
        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      }
    });

    const inserted = await this.withClient(async (client) => {
      const r = await client.query("SELECT * FROM skills WHERE row_id=$1", [newRowId]);
      return r.rows[0] as SkillRowRaw;
    });
    return toSkill(inserted);
  }

  async archiveHead(skillId: string, teamId?: string): Promise<{ archived: boolean }> {
    await this.ensureReady();
    if (this.degraded) return { archived: false };

    const ts = this.now();
    const updated = await this.withClient(async (client) => {
      const where = teamId
        ? "skill_id=$2 AND team_id=$3 AND is_head=1"
        : "skill_id=$2 AND is_head=1";
      const args = teamId ? [ts, skillId, teamId] : [ts, skillId];
      const r = await client.query(
        `UPDATE skills SET status='archived', updated_at_ms=$1 WHERE ${where}`,
        args,
      );
      return r.rowCount ?? 0;
    });
    if (updated > 0) return { archived: true };

    const exists = await this.withClient(async (client) => {
      const where = teamId
        ? "skill_id=$1 AND team_id=$2 AND is_head=1 AND status='archived'"
        : "skill_id=$1 AND is_head=1 AND status='archived'";
      const args = teamId ? [skillId, teamId] : [skillId];
      const r = await client.query(`SELECT 1 FROM skills WHERE ${where} LIMIT 1`, args);
      return (r.rowCount ?? 0) > 0;
    });
    return { archived: exists };
  }

  async getHead(skillId: string, teamId?: string): Promise<Skill | null> {
    await this.ensureReady();
    if (this.degraded) return null;
    const row = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND team_id=$2 AND is_head=1 AND status='active' LIMIT 1",
          [skillId, teamId],
        )
        : await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND is_head=1 AND status='active' LIMIT 1",
          [skillId],
        );
      return r.rows[0] as SkillRowRaw | undefined;
    });
    return row ? toSkill(row) : null;
  }

  async getHeadIncludingArchived(skillId: string, teamId?: string): Promise<Skill | null> {
    await this.ensureReady();
    if (this.degraded) return null;
    const row = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND team_id=$2 AND is_head=1 LIMIT 1",
          [skillId, teamId],
        )
        : await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND is_head=1 LIMIT 1",
          [skillId],
        );
      return r.rows[0] as SkillRowRaw | undefined;
    });
    return row ? toSkill(row) : null;
  }

  async getByVersion(skillId: string, version: number, teamId?: string): Promise<Skill | null> {
    await this.ensureReady();
    if (this.degraded) return null;
    const row = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND version=$2 AND team_id=$3 LIMIT 1",
          [skillId, version, teamId],
        )
        : await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND version=$2 LIMIT 1",
          [skillId, version],
        );
      return r.rows[0] as SkillRowRaw | undefined;
    });
    return row ? toSkill(row) : null;
  }

  async listVersions(
    skillId: string,
    teamId?: string,
    opts: { limit?: number; offset?: number } = {},
  ): Promise<Skill[]> {
    await this.ensureReady();
    if (this.degraded) return [];
    const limit = Math.min(Math.max(opts.limit ?? 50, 1), 1000);
    const offset = Math.max(opts.offset ?? 0, 0);
    const rows = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 AND team_id=$2 ORDER BY version DESC LIMIT $3 OFFSET $4",
          [skillId, teamId, limit, offset],
        )
        : await client.query(
          "SELECT * FROM skills WHERE skill_id=$1 ORDER BY version DESC LIMIT $2 OFFSET $3",
          [skillId, limit, offset],
        );
      return r.rows as SkillRowRaw[];
    });
    return rows.map(toSkill);
  }

  async countVersions(skillId: string, teamId?: string): Promise<number> {
    await this.ensureReady();
    if (this.degraded) return 0;
    const count = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query(
          "SELECT COUNT(*)::int AS c FROM skills WHERE skill_id=$1 AND team_id=$2",
          [skillId, teamId],
        )
        : await client.query("SELECT COUNT(*)::int AS c FROM skills WHERE skill_id=$1", [skillId]);
      return Number(r.rows[0]?.c ?? 0);
    });
    return count;
  }

  async listSkills(opts: ListSkillsOptions): Promise<{ items: Skill[]; total: number }> {
    await this.ensureReady();
    if (this.degraded) return { items: [], total: 0 };

    const status = opts.status?.length ? opts.status : (["active"] as SkillStatus[]);
    const limit = Math.min(Math.max(opts.limit ?? 50, 1), 1000);
    const offset = Math.max(opts.offset ?? 0, 0);

    const where: string[] = ["is_head=1"];
    const args: unknown[] = [];
    let idx = 1;

    if (opts.team_id) {
      where.push(`team_id=$${idx++}`);
      args.push(opts.team_id);
    }
    if (opts.owner_agent_id) {
      where.push(`owner_agent_id=$${idx++}`);
      args.push(opts.owner_agent_id);
    }
    if (opts.user_id) {
      where.push(`user_id=$${idx++}`);
      args.push(opts.user_id);
    }
    if (opts.task_id) {
      where.push(`task_id=$${idx++}`);
      args.push(opts.task_id);
    }
    where.push(`status IN (${status.map(() => `$${idx++}`).join(",")})`);
    args.push(...status);
    if (opts.name_prefix) {
      where.push(`name LIKE $${idx++}`);
      args.push(`${opts.name_prefix}%`);
    }

    const whereSql = where.join(" AND ");
    return this.withClient(async (client) => {
      const totalR = await client.query(`SELECT COUNT(*)::int AS c FROM skills WHERE ${whereSql}`, args);
      const rowsR = await client.query(
        `SELECT * FROM skills WHERE ${whereSql} ORDER BY updated_at_ms DESC LIMIT $${idx} OFFSET $${idx + 1}`,
        [...args, limit, offset],
      );
      return {
        items: (rowsR.rows as SkillRowRaw[]).map(toSkill),
        total: Number(totalR.rows[0]?.c ?? 0),
      };
    });
  }

  async searchSkills(opts: SearchSkillsOptions): Promise<SkillSearchResult[]> {
    await this.ensureReady();
    if (this.degraded) return [];

    const topK = Math.min(Math.max(opts.topK ?? 10, 1), 50);
    const query = (opts.query ?? "").trim();
    if (!query) return [];

    const requestedMode = opts.mode ?? "bm25";
    const caps = this.getCapabilities();
    const wantsVec = requestedMode === "embedding" || requestedMode === "hybrid";
    const canVec = wantsVec && caps.vectorSearch && !!opts.queryEmbedding;

    if (wantsVec && !canVec) {
      this.logger?.warn?.(
        `${TAG} search mode='${requestedMode}' downgraded to lexical ` +
          `(vector=${caps.vectorSearch}, has_embedding=${!!opts.queryEmbedding})`,
      );
    }

    if (canVec && requestedMode === "embedding") {
      return this.searchVector(opts, topK);
    }
    if (canVec && requestedMode === "hybrid") {
      const [lex, vec] = await Promise.all([
        this.searchLexical(opts, topK * 2),
        this.searchVector(opts, topK * 2),
      ]);
      return this.mergeRrf(lex, vec, topK);
    }
    return this.searchLexical(opts, topK);
  }

  private buildFilterClauses(
    opts: SearchSkillsOptions,
    startIdx: number,
  ): { sql: string; args: unknown[]; nextIdx: number } {
    const parts: string[] = [];
    const args: unknown[] = [];
    let idx = startIdx;
    if (opts.team_id) {
      parts.push(`team_id=$${idx++}`);
      args.push(opts.team_id);
    }
    if (opts.agent_id) {
      parts.push(`owner_agent_id=$${idx++}`);
      args.push(opts.agent_id);
    }
    if (opts.task_id) {
      parts.push(`task_id=$${idx++}`);
      args.push(opts.task_id);
    }
    if (opts.user_id) {
      parts.push(`user_id=$${idx++}`);
      args.push(opts.user_id);
    }
    return { sql: parts.length ? ` AND ${parts.join(" AND ")}` : "", args, nextIdx: idx };
  }

  private async searchLexical(opts: SearchSkillsOptions, topK: number): Promise<SkillSearchResult[]> {
    const query = (opts.query ?? "").trim();
    const patterns = ftsIlikePatterns(query);
    const filter = this.buildFilterClauses(opts, 4);

    const rows = await this.withClient(async (client) => {
      const r = await client.query(
        `SELECT *,
                ts_rank_cd(fts, plainto_tsquery('simple', $1)) AS score
         FROM skills
         WHERE is_head=1 AND status='active'
           AND (
             fts @@ plainto_tsquery('simple', $1)
             OR name ILIKE ANY($2::text[])
             OR description ILIKE ANY($2::text[])
             OR content ILIKE ANY($2::text[])
           )${filter.sql}
         ORDER BY score DESC, updated_at_ms DESC
         LIMIT $3`,
        [query, patterns, topK, ...filter.args],
      );
      return r.rows as Array<SkillRowRaw & { score: number }>;
    });

    return rows.map((r) => ({
      skill: toSkill(r),
      score: Number(r.score ?? 0),
      snippet: r.content.slice(0, 200),
    }));
  }

  private async searchVector(opts: SearchSkillsOptions, topK: number): Promise<SkillSearchResult[]> {
    if (!opts.queryEmbedding || this.dimensions <= 0) return [];
    const vec = toVectorLiteral(opts.queryEmbedding);
    const filter = this.buildFilterClauses(opts, 3);

    const rows = await this.withClient(async (client) => {
      const r = await client.query(
        `SELECT *,
                1 - (embedding <=> $1::vector) AS score
         FROM skills
         WHERE is_head=1 AND status='active'
           AND embedding IS NOT NULL${filter.sql}
         ORDER BY embedding <=> $1::vector
         LIMIT $2`,
        [vec, topK, ...filter.args],
      );
      return r.rows as Array<SkillRowRaw & { score: number }>;
    });

    return rows.map((r) => ({
      skill: toSkill(r),
      score: Number(r.score ?? 0),
      snippet: r.content.slice(0, 200),
    }));
  }

  private mergeRrf(
    a: SkillSearchResult[],
    b: SkillSearchResult[],
    topK: number,
  ): SkillSearchResult[] {
    const k = 60;
    const scores = new Map<string, { skill: Skill; score: number; snippet?: string }>();
    const bump = (items: SkillSearchResult[]) => {
      items.forEach((item, rank) => {
        const id = item.skill.skill_id;
        const prev = scores.get(id);
        const add = 1 / (k + rank + 1);
        if (prev) {
          prev.score += add;
          if (!prev.snippet && item.snippet) prev.snippet = item.snippet;
        } else {
          scores.set(id, { skill: item.skill, score: add, snippet: item.snippet });
        }
      });
    };
    bump(a);
    bump(b);
    return [...scores.values()]
      .sort((x, y) => y.score - x.score)
      .slice(0, topK)
      .map((x) => ({ skill: x.skill, score: x.score, snippet: x.snippet }));
  }

  async findExpiredVersions(cutoffMs: number): Promise<ExpiredVersionMeta[]> {
    await this.ensureReady();
    if (this.degraded) return [];
    const rows = await this.withClient(async (client) => {
      const r = await client.query(
        `SELECT skill_id, version, is_head, status, storage_dir, created_at_ms
         FROM skills
         WHERE is_head=0 AND status='active' AND created_at_ms < $1
         ORDER BY skill_id ASC, version ASC`,
        [cutoffMs],
      );
      return r.rows as Array<{
        skill_id: string;
        version: number;
        is_head: number;
        status: string;
        storage_dir: string;
        created_at_ms: number;
      }>;
    });
    return rows.map((r) => ({
      skill_id: r.skill_id,
      version: r.version,
      is_head: r.is_head === 1,
      status: r.status as SkillStatus,
      storage_dir: r.storage_dir,
      created_at_ms: Number(r.created_at_ms),
    }));
  }

  async deleteVersion(skillId: string, version: number): Promise<boolean> {
    await this.ensureReady();
    if (this.degraded) return false;
    const deleted = await this.withClient(async (client) => {
      const r = await client.query(
        "DELETE FROM skills WHERE skill_id=$1 AND version=$2 AND is_head=0",
        [skillId, version],
      );
      return (r.rowCount ?? 0) > 0;
    });
    return deleted;
  }

  async deleteAllVersions(skillId: string, teamId?: string): Promise<number> {
    await this.ensureReady();
    if (this.degraded) return 0;
    const deleted = await this.withClient(async (client) => {
      const r = teamId
        ? await client.query("DELETE FROM skills WHERE skill_id=$1 AND team_id=$2", [skillId, teamId])
        : await client.query("DELETE FROM skills WHERE skill_id=$1", [skillId]);
      return r.rowCount ?? 0;
    });
    return deleted;
  }

  /** Optional embedding maintenance (duck-typed like SqliteSkillStore). */
  async upsertEmbedding(skillId: string, embedding: Float32Array): Promise<void> {
    await this.ensureReady();
    if (this.degraded || this.dimensions <= 0) return;
    if (embedding.length !== this.dimensions) {
      this.logger?.warn?.(
        `${TAG} embedding dim mismatch: ${embedding.length} vs ${this.dimensions}`,
      );
      return;
    }
    try {
      await this.withClient(async (client) => {
        await client.query(
          `UPDATE skills SET embedding=$1::vector
           WHERE skill_id=$2 AND is_head=1 AND status='active'`,
          [toVectorLiteral(embedding), skillId],
        );
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} upsertEmbedding failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  async deleteEmbedding(skillId: string): Promise<void> {
    await this.ensureReady();
    if (this.degraded) return;
    try {
      await this.withClient(async (client) => {
        await client.query(
          "UPDATE skills SET embedding=NULL WHERE skill_id=$1 AND is_head=1",
          [skillId],
        );
      });
    } catch (err) {
      this.logger?.warn?.(
        `${TAG} deleteEmbedding failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}
