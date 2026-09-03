/**
 * Postgres 实现的 IMetadataStore（OPEN 路径）。
 *
 * 与 SQLite 适配器共用同一套 SQL 语义；每实例一个 schema（`tdai_metadata_*`），
 * 可与 pgvector memory store（`mem_*`）共享同一 Postgres 实例。
 *
 * 后端 id 为 `postgres`，不复用未实现的 `mysql`。
 */

import { AsyncLocalStorage } from "node:async_hooks";
import type { Pool, PoolClient } from "pg";
import { mapTeamMemberWithProfile } from "./team-member-view.js";
import { generateId, generateRelationId, ID_PREFIX } from "../utils/id-generator.js";
import {
  isPgRelationIdCollision,
  isPgUniqueViolation,
  runWithGeneratedRelationIdAsync,
  RELATION_ID_RETRY_LIMIT,
} from "./relation-id-insert.js";
import { generateUserKey } from "../utils/crypto.js";
import { isUserKeyExpired } from "../utils/user-key.js";
import type {
  UserEntity,
  UserKeyEntity,
  TeamEntity,
  TeamMemberEntity,
  TeamMemberView,
  AgentEntity,
  TaskEntity,
  TaskAgentEntity,
  ParticipationLogEntity,
  AppendParticipationLogInput,
  ParticipationLogFilter,
  AssetEntity,
  FixedAssetBindingEntity,
  AclEntity,
  CreateUserInput,
  CreateUserKeyInput,
  CreateTeamInput,
  AddTeamMemberInput,
  CreateAgentInput,
  CreateTaskInput,
  CreateAssetInput,
  FixedAssetBindingInput,
  GrantAclInput,
  AgentFilter,
  TaskFilter,
  AssetFilter,
  BatchDeleteResult,
  ListPage,
  PaginationParams,
  InstanceUserListFilter,
  AgentFixedAssetCountRow,
  AssetType,
  ConfigParamEntity,
  UpsertConfigParamInput,
  ListConfigParamsFilter,
} from "../types.js";
import { DEFAULT_PAGINATION } from "../pagination.js";
import { buildChatMemoryAssetId } from "../utils/chat-memory-asset.js";
import { DuplicateUserKeyError, type IMetadataStore } from "./interface.js";

type Row = Record<string, unknown>;
type SqlValue = unknown;

const PK_RETRY_LIMIT = 3;
const pgTx = new AsyncLocalStorage<PoolClient>();

function nowIso(): string {
  return new Date().toISOString();
}

function toPg(sql: string): string {
  let n = 0;
  return sql.replace(/\?/g, () => `$${++n}`);
}

function qIdent(name: string): string {
  return `"${name.replace(/"/g, "\"\"")}"`;
}

function pgHaystack(err: unknown): string {
  if (typeof err !== "object" || err === null) return "";
  const e = err as { constraint?: string; detail?: string };
  return `${e.constraint ?? ""} ${e.detail ?? ""}`;
}

function isPkCollision(err: unknown): boolean {
  if (!isPgUniqueViolation(err)) return false;
  const hay = pgHaystack(err);
  return /(user_id|team_id|agent_id|task_id|asset_id|acl_id|key_id)/i.test(hay) && !/key_value/i.test(hay);
}

function isUserKeyValueCollision(err: unknown): boolean {
  if (!isPgUniqueViolation(err)) return false;
  return /key_value/i.test(pgHaystack(err));
}

function isStorePkCollision(err: unknown): boolean {
  return isPkCollision(err) || isPgRelationIdCollision(err);
}

export interface PostgresMetadataStoreOptions {
  connectionString?: string;
  pool?: Pool;
  schema: string;
  /** false 时 close() 不结束 pool（MetadataStorePool 共享连接）。 */
  ownsPool?: boolean;
}

export class PostgresMetadataStore implements IMetadataStore {
  private pool: Pool | null;
  private readonly connectionString?: string;
  private readonly schema: string;
  private readonly ownsPool: boolean;
  private pgModule: typeof import("pg") | null = null;
  private initialized = false;

  constructor(opts: PostgresMetadataStoreOptions) {
    this.connectionString = opts.connectionString;
    this.schema = opts.schema;
    this.ownsPool = opts.ownsPool ?? !opts.pool;
    this.pool = opts.pool ?? null;
  }

  private async getPool(): Promise<Pool> {
    if (this.pool) return this.pool;
    if (!this.connectionString) {
      throw new Error("TDAI_METADATA_POSTGRES_URL or DATABASE_URL is required when backend=postgres");
    }
    if (!this.pgModule) {
      this.pgModule = await import("pg");
    }
    const { Pool } = this.pgModule;
    this.pool = new Pool({ connectionString: this.connectionString, max: 8 });
    return this.pool;
  }

  private async withClient<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const bound = pgTx.getStore();
    if (bound) return fn(bound);
    const pool = await this.getPool();
    const client = await pool.connect();
    try {
      await client.query(`SET search_path TO ${qIdent(this.schema)}, public`);
      return await fn(client);
    } finally {
      client.release();
    }
  }

  private async tx<T>(fn: () => Promise<T>): Promise<T> {
    return this.withClient(async (client) => {
      await client.query("BEGIN");
      try {
        const result = await pgTx.run(client, fn);
        await client.query("COMMIT");
        return result;
      } catch (e) {
        try {
          await client.query("ROLLBACK");
        } catch {
          /* ignore */
        }
        throw e;
      }
    });
  }

  private async get<T = Row>(sql: string, params: SqlValue[] = []): Promise<T | null> {
    const result = await this.withClient((c) => c.query(toPg(sql), params));
    return (result.rows[0] as T | undefined) ?? null;
  }

  private async all<T = Row>(sql: string, params: SqlValue[] = []): Promise<T[]> {
    const result = await this.withClient((c) => c.query(toPg(sql), params));
    return result.rows as T[];
  }

  private async run(sql: string, params: SqlValue[] = []): Promise<void> {
    await this.withClient((c) => c.query(toPg(sql), params));
  }

  private async selectList<T>(
    countSql: string,
    countParams: SqlValue[],
    dataSql: string,
    dataParams: SqlValue[],
    pagination: PaginationParams | null | undefined,
    mapper: (row: Row) => T | null,
  ): Promise<ListPage<T>> {
    const totalRow = await this.get<{ c: string | number }>(countSql, countParams);
    const total = Number(totalRow?.c ?? 0);
    const p = pagination ?? DEFAULT_PAGINATION;
    const rows = await this.all<Row>(`${dataSql} LIMIT ? OFFSET ?`, [...dataParams, p.limit, p.offset]);
    const items: T[] = [];
    for (const r of rows) {
      const mapped = mapper(r);
      if (mapped) items.push(mapped);
    }
    return { items, total };
  }

  async init(): Promise<void> {
    if (this.initialized) return;
    const pool = await this.getPool();
    const client = await pool.connect();
    try {
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${qIdent(this.schema)}`);
      await client.query(`SET search_path TO ${qIdent(this.schema)}, public`);
      await client.query(SCHEMA_SQL);
    } finally {
      client.release();
    }
    this.initialized = true;
  }

  async close(): Promise<void> {
    if (this.ownsPool && this.pool) {
      await this.pool.end().catch(() => {});
      this.pool = null;
    }
    this.initialized = false;
  }

  /** Tests / purge：删除本实例 schema。 */
  async dropSchema(): Promise<void> {
    const pool = await this.getPool();
    await pool.query(`DROP SCHEMA IF EXISTS ${qIdent(this.schema)} CASCADE`);
    this.initialized = false;
  }

  get schemaName(): string {
    return this.schema;
  }

  // ============================================================
  // User
  // ============================================================
  async createUser(input: CreateUserInput): Promise<UserEntity> {
    const now = nowIso();
    const defaultKeyValue = input.default_key_value ?? generateUserKey();
    for (let attempt = 0; attempt < PK_RETRY_LIMIT; attempt++) {
      const userId = input.user_id ?? generateId(ID_PREFIX.user);
      try {
        await this.tx(async () => {
          await this.run(
            `INSERT INTO meta_users
              (user_id, password, auth_provider, external_id, username,
               display_name, email, raw_profile_json, status, user_type, created_at, updated_at, metadata_json)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [
              userId,
              input.password ?? null,
              input.auth_provider as string,
              input.external_id as string,
              input.username as string,
              input.display_name ?? null,
              input.email ?? null,
              input.raw_profile_json ?? "{}",
              input.status ?? "active",
              input.user_type ?? "normal",
              now,
              now,
              input.metadata_json ?? "{}",
            ],
          );
          await this.insertUserKeyRow({
            user_id: userId,
            key_value: defaultKeyValue,
            is_default: true,
            created_at: now,
          });
        });
        return (await this.getUserById(userId))!;
      } catch (err) {
        if (err instanceof DuplicateUserKeyError) throw err;
        if (isUserKeyValueCollision(err)) {
          throw new DuplicateUserKeyError(defaultKeyValue);
        }
        if (isPkCollision(err) && !input.user_id) continue;
        throw err;
      }
    }
    throw new Error("PK collision after max retries");
  }

  async getUserById(userId: string): Promise<UserEntity | null> {
    return this.mapUser(await this.get("SELECT * FROM meta_users WHERE user_id = ?", [userId]));
  }

  async getUserByKey(userKey: string): Promise<UserEntity | null> {
    const keyRow = this.mapUserKey(
      await this.get("SELECT * FROM meta_user_keys WHERE key_value = ? AND status = 'active'", [userKey]),
    );
    if (!keyRow || isUserKeyExpired(keyRow.expires_at)) return null;
    await this.touchUserKeyUsage(keyRow.key_id);
    return this.getUserById(keyRow.user_id);
  }

  async getDefaultUserKey(userId: string): Promise<UserKeyEntity | null> {
    return this.mapUserKey(
      await this.get(
        "SELECT * FROM meta_user_keys WHERE user_id = ? AND is_default = 1 AND status = 'active' LIMIT 1",
        [userId],
      ),
    );
  }

  async getUserByUsername(authProvider: string, username: string): Promise<UserEntity | null> {
    return this.mapUser(
      await this.get("SELECT * FROM meta_users WHERE auth_provider = ? AND username = ?", [authProvider, username]),
    );
  }

  async getUserByEmail(email: string): Promise<UserEntity | null> {
    return this.mapUser(await this.get("SELECT * FROM meta_users WHERE email = ?", [email]));
  }

  async getUserByExternalId(authProvider: string, externalId: string): Promise<UserEntity | null> {
    return this.mapUser(
      await this.get(
        "SELECT * FROM meta_users WHERE auth_provider = ? AND external_id = ?",
        [authProvider, externalId],
      ),
    );
  }

  async updateUser(userId: string, patch: Partial<UserEntity>): Promise<UserEntity | null> {
    const allowed = ["password", "display_name", "email", "raw_profile_json", "status", "metadata_json", "username"] as const;
    await this.applyUpdate("meta_users", "user_id", userId, allowed, patch);
    return this.getUserById(userId);
  }

  async deleteUsers(userIds: string[]): Promise<BatchDeleteResult> {
    const result = await this.batchDelete("meta_users", "user_id", userIds);
    if (result.deleted_ids.length > 0) {
      const ph = result.deleted_ids.map(() => "?").join(",");
      await this.run(`DELETE FROM meta_user_keys WHERE user_id IN (${ph})`, result.deleted_ids);
      await this.run(`DELETE FROM meta_team_members WHERE user_id IN (${ph})`, result.deleted_ids);
      await this.run(
        `DELETE FROM meta_asset_acl WHERE subject_type = 'user' AND subject_id IN (${ph})`,
        result.deleted_ids,
      );
    }
    return result;
  }

  async listUsersByTeam(
    teamId: string,
    pagination?: PaginationParams | null,
    filter?: InstanceUserListFilter,
  ): Promise<ListPage<UserEntity>> {
    let base =
      "FROM meta_users u JOIN meta_team_members m ON m.user_id = u.user_id WHERE m.team_id = ? AND m.status = 'active'";
    const params: SqlValue[] = [teamId];
    if (filter?.user_ids?.length) {
      base += ` AND u.user_id IN (${filter.user_ids.map(() => "?").join(",")})`;
      params.push(...filter.user_ids);
    }
    if (filter?.username) {
      base += " AND u.username = ?";
      params.push(filter.username);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      params,
      `SELECT u.* ${base} ORDER BY u.created_at DESC`,
      params,
      pagination,
      (r) => this.mapUser(r),
    );
  }

  async listUsers(
    pagination?: PaginationParams | null,
    filter?: InstanceUserListFilter,
  ): Promise<ListPage<UserEntity>> {
    let where = "WHERE 1=1";
    const params: SqlValue[] = [];
    if (filter?.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter?.user_type) {
      where += " AND user_type = ?";
      params.push(filter.user_type);
    }
    if (filter?.user_ids?.length) {
      where += ` AND user_id IN (${filter.user_ids.map(() => "?").join(",")})`;
      params.push(...filter.user_ids);
    }
    if (filter?.username) {
      where += " AND username = ?";
      params.push(filter.username);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_users ${where}`,
      params,
      `SELECT * FROM meta_users ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapUser(r),
    );
  }

  async countUsers(): Promise<number> {
    const row = await this.get<{ c: string | number }>("SELECT COUNT(*) AS c FROM meta_users");
    return Number(row?.c ?? 0);
  }

  async countSystemAdmins(): Promise<number> {
    const row = await this.get<{ c: string | number }>(
      "SELECT COUNT(*) AS c FROM meta_users WHERE user_type = 'system_admin'",
    );
    return Number(row?.c ?? 0);
  }

  async countTeams(): Promise<number> {
    const row = await this.get<{ c: string | number }>("SELECT COUNT(*) AS c FROM meta_teams");
    return Number(row?.c ?? 0);
  }

  // ============================================================
  // UserKey
  // ============================================================
  private async insertUserKeyRow(input: {
    user_id: string;
    key_value: string;
    name?: string | null;
    is_default?: boolean;
    expires_at?: string | null;
    created_at?: string;
    metadata_json?: string;
  }): Promise<UserKeyEntity> {
    const now = input.created_at ?? nowIso();
    for (let attempt = 0; attempt < PK_RETRY_LIMIT; attempt++) {
      const keyId = generateId(ID_PREFIX.userKey);
      try {
        await this.run(
          `INSERT INTO meta_user_keys
            (key_id, user_id, key_value, name, status, is_default, last_used_at, expires_at, created_at, revoked_at, metadata_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
          [
            keyId,
            input.user_id,
            input.key_value,
            input.name ?? null,
            "active",
            input.is_default ? 1 : 0,
            null,
            input.expires_at ?? null,
            now,
            null,
            input.metadata_json ?? "{}",
          ],
        );
        return (await this.getUserKeyById(keyId))!;
      } catch (err) {
        if (isUserKeyValueCollision(err)) {
          throw new DuplicateUserKeyError(input.key_value);
        }
        if (isPkCollision(err)) continue;
        throw err;
      }
    }
    throw new Error("user key PK collision after max retries");
  }

  async createUserKey(input: CreateUserKeyInput): Promise<UserKeyEntity> {
    if (input.is_default) {
      await this.run(
        "UPDATE meta_user_keys SET is_default = 0 WHERE user_id = ? AND status = 'active'",
        [input.user_id],
      );
    }
    const keyValue = input.key_value ?? generateUserKey();
    return this.insertUserKeyRow({
      user_id: input.user_id,
      key_value: keyValue,
      name: input.name,
      is_default: input.is_default ?? false,
      expires_at: input.expires_at,
      metadata_json: input.metadata_json,
    });
  }

  async getUserKeyById(keyId: string): Promise<UserKeyEntity | null> {
    return this.mapUserKey(await this.get("SELECT * FROM meta_user_keys WHERE key_id = ?", [keyId]));
  }

  async listUserKeys(userId: string, pagination?: PaginationParams | null): Promise<ListPage<UserKeyEntity>> {
    const base = "FROM meta_user_keys WHERE user_id = ?";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [userId],
      `SELECT * ${base} ORDER BY created_at DESC`,
      [userId],
      pagination,
      (r) => this.mapUserKey(r),
    );
  }

  async countActiveUserKeys(userId: string): Promise<number> {
    const row = await this.get<{ c: string | number }>(
      "SELECT COUNT(*) AS c FROM meta_user_keys WHERE user_id = ? AND status = 'active'",
      [userId],
    );
    return Number(row?.c ?? 0);
  }

  async revokeUserKey(keyId: string, options?: { promoteNextDefault?: boolean }): Promise<UserKeyEntity | null> {
    const promoteNextDefault = options?.promoteNextDefault ?? true;
    const existing = await this.getUserKeyById(keyId);
    if (!existing) return null;

    if (existing.is_default && promoteNextDefault) {
      await this.run("UPDATE meta_user_keys SET is_default = 0 WHERE key_id = ?", [keyId]);
      const next = await this.get(
        `SELECT * FROM meta_user_keys WHERE user_id = ? AND status = 'active' AND key_id != ? ORDER BY created_at ASC LIMIT 1`,
        [existing.user_id, keyId],
      );
      if (next) {
        await this.run("UPDATE meta_user_keys SET is_default = 1 WHERE key_id = ?", [String((next as Row).key_id)]);
      }
    }

    await this.run("DELETE FROM meta_user_keys WHERE key_id = ?", [keyId]);
    return existing;
  }

  async updateUserKey(
    keyId: string,
    patch: Partial<Pick<UserKeyEntity, "name" | "expires_at" | "is_default" | "metadata_json">>,
  ): Promise<UserKeyEntity | null> {
    const existing = await this.getUserKeyById(keyId);
    if (!existing) return null;
    if (patch.is_default === true) {
      await this.run(
        "UPDATE meta_user_keys SET is_default = 0 WHERE user_id = ? AND status = 'active'",
        [existing.user_id],
      );
    }
    const allowed = ["name", "expires_at", "is_default", "metadata_json"] as const;
    await this.applyUpdate("meta_user_keys", "key_id", keyId, allowed, {
      ...patch,
      is_default: patch.is_default === undefined ? undefined : patch.is_default ? 1 : 0,
    } as Partial<UserKeyEntity>);
    return this.getUserKeyById(keyId);
  }

  async touchUserKeyUsage(keyId: string): Promise<void> {
    await this.run("UPDATE meta_user_keys SET last_used_at = ? WHERE key_id = ?", [nowIso(), keyId]);
  }

  async revokeAllUserKeysForUser(userId: string): Promise<void> {
    await this.run("DELETE FROM meta_user_keys WHERE user_id = ? AND status = 'active'", [userId]);
  }

  // ============================================================
  // Team
  // ============================================================
  async createTeam(input: CreateTeamInput): Promise<TeamEntity> {
    const now = nowIso();
    for (let attempt = 0; attempt < PK_RETRY_LIMIT; attempt++) {
      const teamId = input.team_id ?? generateId(ID_PREFIX.team);
      try {
        return await this.tx(async () => {
          await this.run(
            `INSERT INTO meta_teams
              (team_id, name, description, owner_user_id, status, created_at, updated_at, metadata_json)
             VALUES (?,?,?,?,?,?,?,?)`,
            [
              teamId,
              input.name,
              input.description ?? null,
              input.owner_user_id,
              input.status ?? "active",
              now,
              now,
              input.metadata_json ?? "{}",
            ],
          );
          await this.run(
            `INSERT INTO meta_team_members (id, team_id, user_id, role, joined_at, status)
             VALUES (?,?,?,?,?,?)`,
            [generateRelationId(), teamId, input.owner_user_id, "admin", now, "active"],
          );
          return (await this.getTeamById(teamId))!;
        });
      } catch (err) {
        if (isStorePkCollision(err) && !input.team_id) continue;
        if (isPgRelationIdCollision(err)) continue;
        throw err;
      }
    }
    throw new Error("PK collision after max retries");
  }

  async getTeamById(teamId: string): Promise<TeamEntity | null> {
    return this.mapTeam(await this.get("SELECT * FROM meta_teams WHERE team_id = ?", [teamId]));
  }

  async updateTeam(teamId: string, patch: Partial<TeamEntity>): Promise<TeamEntity | null> {
    const allowed = ["name", "description", "status", "metadata_json"] as const;
    await this.applyUpdate("meta_teams", "team_id", teamId, allowed, patch);
    return this.getTeamById(teamId);
  }

  async deleteTeams(teamIds: string[]): Promise<BatchDeleteResult> {
    const result = await this.batchDelete("meta_teams", "team_id", teamIds);
    if (result.deleted_ids.length > 0) {
      const ph = result.deleted_ids.map(() => "?").join(",");
      await this.run(`DELETE FROM meta_team_members WHERE team_id IN (${ph})`, result.deleted_ids);
      await this.run(`DELETE FROM meta_agents WHERE team_id IN (${ph})`, result.deleted_ids);
      await this.run(`DELETE FROM meta_tasks WHERE team_id IN (${ph})`, result.deleted_ids);
      await this.run(`DELETE FROM meta_assets WHERE team_id IN (${ph})`, result.deleted_ids);
    }
    return result;
  }

  async listTeamsByUser(
    userId: string,
    pagination?: PaginationParams | null,
    filter?: { name?: string },
  ): Promise<ListPage<TeamEntity>> {
    let base =
      "FROM meta_teams t JOIN meta_team_members m ON m.team_id = t.team_id WHERE m.user_id = ? AND m.status = 'active'";
    const params: SqlValue[] = [userId];
    if (filter?.name) {
      base += " AND t.name = ?";
      params.push(filter.name);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      params,
      `SELECT t.* ${base} ORDER BY t.created_at DESC`,
      params,
      pagination,
      (r) => this.mapTeam(r),
    );
  }

  // ============================================================
  // TeamMember
  // ============================================================
  async addTeamMember(input: AddTeamMemberInput): Promise<TeamMemberEntity> {
    const now = nowIso();
    await runWithGeneratedRelationIdAsync(input.id, isPgRelationIdCollision, async (id) => {
      await this.run(
        `INSERT INTO meta_team_members (id, team_id, user_id, role, joined_at, status)
       VALUES (?,?,?,?,?,?)
       ON CONFLICT (team_id, user_id) DO UPDATE SET role = EXCLUDED.role, status = EXCLUDED.status`,
        [id, input.team_id, input.user_id, input.role ?? "member", now, input.status ?? "active"],
      );
    });
    return (await this.getTeamMember(input.team_id, input.user_id))!;
  }

  async removeTeamMember(teamId: string, userId: string): Promise<void> {
    await this.run("DELETE FROM meta_team_members WHERE team_id = ? AND user_id = ?", [teamId, userId]);
  }

  async listTeamMembers(teamId: string, pagination?: PaginationParams | null): Promise<ListPage<TeamMemberEntity>> {
    const base = "FROM meta_team_members WHERE team_id = ? AND status = 'active'";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [teamId],
      `SELECT * ${base} ORDER BY joined_at DESC`,
      [teamId],
      pagination,
      (r) => this.mapTeamMember(r),
    );
  }

  async getTeamMember(teamId: string, userId: string): Promise<TeamMemberEntity | null> {
    return this.mapTeamMember(
      await this.get("SELECT * FROM meta_team_members WHERE team_id = ? AND user_id = ?", [teamId, userId]),
    );
  }

  async listTeamMembersWithProfile(
    teamId: string,
    pagination?: PaginationParams | null,
  ): Promise<ListPage<TeamMemberView>> {
    const base =
      "FROM meta_team_members m LEFT JOIN meta_users u ON u.user_id = m.user_id WHERE m.team_id = ? AND m.status = 'active'";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [teamId],
      `SELECT m.id, m.team_id, m.user_id, m.role, m.joined_at, m.status, COALESCE(u.username, '') AS username ${base} ORDER BY m.joined_at DESC`,
      [teamId],
      pagination,
      (r) => mapTeamMemberWithProfile(r as unknown as TeamMemberEntity & { username?: string }),
    );
  }

  async getTeamMemberWithProfile(teamId: string, userId: string): Promise<TeamMemberView | null> {
    const row = await this.get<TeamMemberEntity & { username?: string }>(
      `SELECT m.id, m.team_id, m.user_id, m.role, m.joined_at, m.status, COALESCE(u.username, '') AS username
       FROM meta_team_members m
       LEFT JOIN meta_users u ON u.user_id = m.user_id
       WHERE m.team_id = ? AND m.user_id = ?`,
      [teamId, userId],
    );
    return row ? mapTeamMemberWithProfile(row) : null;
  }

  // ============================================================
  // Agent
  // ============================================================
  async createAgent(input: CreateAgentInput): Promise<AgentEntity> {
    const now = nowIso();
    for (let attempt = 0; attempt < PK_RETRY_LIMIT; attempt++) {
      const agentId = input.agent_id ?? generateId(ID_PREFIX.agent);
      try {
        await this.run(
          `INSERT INTO meta_agents
            (agent_id, team_id, owner_user_id, name, description, prompt, visibility, status, created_at, updated_at, metadata_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
          [
            agentId,
            input.team_id,
            input.owner_user_id,
            input.name,
            input.description ?? null,
            input.prompt ?? null,
            input.visibility ?? "team",
            input.status ?? "active",
            now,
            now,
            input.metadata_json ?? "{}",
          ],
        );
        return (await this.getAgentById(agentId))!;
      } catch (err) {
        if (isPkCollision(err) && !input.agent_id) continue;
        throw err;
      }
    }
    throw new Error("PK collision after max retries");
  }

  async getAgentById(agentId: string): Promise<AgentEntity | null> {
    return this.mapAgent(await this.get("SELECT * FROM meta_agents WHERE agent_id = ?", [agentId]));
  }

  async updateAgent(agentId: string, patch: Partial<AgentEntity>): Promise<AgentEntity | null> {
    const allowed = ["name", "description", "prompt", "visibility", "status", "metadata_json"] as const;
    await this.applyUpdate("meta_agents", "agent_id", agentId, allowed, patch);
    return this.getAgentById(agentId);
  }

  async deleteAgents(agentIds: string[]): Promise<BatchDeleteResult> {
    const existingAgents = (
      await Promise.all(agentIds.map((agentId) => this.getAgentById(agentId)))
    ).filter((agent): agent is AgentEntity => !!agent);
    const selfMemoryByAgent = new Map(
      existingAgents.map((agent) => [agent.agent_id, buildChatMemoryAssetId(agent.team_id, agent.agent_id)]),
    );

    const result = await this.batchDelete("meta_agents", "agent_id", agentIds);
    if (result.deleted_ids.length > 0) {
      const ph = result.deleted_ids.map(() => "?").join(",");
      await this.run(`DELETE FROM meta_task_agents WHERE agent_id IN (${ph})`, result.deleted_ids);
      await this.run(`DELETE FROM meta_agent_fixed_assets WHERE agent_id IN (${ph})`, result.deleted_ids);
      const selfMemoryAssetIds = result.deleted_ids
        .map((agentId) => selfMemoryByAgent.get(agentId))
        .filter((assetId): assetId is string => !!assetId);
      if (selfMemoryAssetIds.length > 0) {
        await this.deleteAssets(selfMemoryAssetIds);
      }
    }
    return result;
  }

  async listAgentsByTeam(
    teamId: string,
    pagination?: PaginationParams | null,
    filter?: AgentFilter,
  ): Promise<ListPage<AgentEntity>> {
    let where = "WHERE team_id = ?";
    const params: SqlValue[] = [teamId];
    if (filter?.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter?.owner_user_id) {
      where += " AND owner_user_id = ?";
      params.push(filter.owner_user_id);
    }
    if (filter?.name) {
      where += " AND name = ?";
      params.push(filter.name);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_agents ${where}`,
      params,
      `SELECT * FROM meta_agents ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapAgent(r),
    );
  }

  async listAgentsByOwner(
    userId: string,
    pagination?: PaginationParams | null,
    filter?: AgentFilter,
  ): Promise<ListPage<AgentEntity>> {
    let where = "WHERE owner_user_id = ?";
    const params: SqlValue[] = [userId];
    if (filter?.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter?.name) {
      where += " AND name = ?";
      params.push(filter.name);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_agents ${where}`,
      params,
      `SELECT * FROM meta_agents ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapAgent(r),
    );
  }

  // ============================================================
  // Task
  // ============================================================
  async createTask(input: CreateTaskInput): Promise<TaskEntity> {
    const now = nowIso();
    for (let attempt = 0; attempt < PK_RETRY_LIMIT; attempt++) {
      const taskId = input.task_id ?? generateId(ID_PREFIX.task);
      try {
        return await this.tx(async () => {
          await this.run(
            `INSERT INTO meta_tasks
              (task_id, team_id, creator_user_id, title, description, source_type, source_url,
               status, auto_assign_floating_assets, risk_level, created_at, updated_at, metadata_json)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            [
              taskId,
              input.team_id,
              input.creator_user_id,
              input.title,
              input.description ?? null,
              input.source_type ?? "manual",
              input.source_url ?? null,
              input.status ?? "running",
              input.auto_assign_floating_assets ? 1 : 0,
              input.risk_level ?? null,
              now,
              now,
              input.metadata_json ?? "{}",
            ],
          );
          for (const link of input.linked_agents ?? []) {
            await this.run(
              `INSERT INTO meta_task_agents (id, task_id, agent_id, role_in_task, status, created_at)
               VALUES (?,?,?,?,?,?)`,
              [generateRelationId(), taskId, link.agent_id, link.role_in_task ?? null, "active", now],
            );
          }
          return (await this.getTaskById(taskId))!;
        });
      } catch (err) {
        if (isStorePkCollision(err) && !input.task_id) continue;
        if (isPgRelationIdCollision(err)) continue;
        throw err;
      }
    }
    throw new Error("PK collision after max retries");
  }

  async getTaskById(taskId: string): Promise<TaskEntity | null> {
    return this.mapTask(await this.get("SELECT * FROM meta_tasks WHERE task_id = ?", [taskId]));
  }

  async updateTask(taskId: string, patch: Partial<TaskEntity>): Promise<TaskEntity | null> {
    const allowed = ["title", "description", "source_type", "source_url", "status", "auto_assign_floating_assets", "risk_level", "metadata_json"] as const;
    const normalized: Record<string, SqlValue> = {};
    for (const k of allowed) {
      if (k in patch && (patch as Record<string, unknown>)[k] !== undefined) {
        const v = (patch as Record<string, unknown>)[k];
        normalized[k] = k === "auto_assign_floating_assets" ? (v ? 1 : 0) : v;
      }
    }
    await this.applyUpdateRaw("meta_tasks", "task_id", taskId, normalized);
    return this.getTaskById(taskId);
  }

  async deleteTasks(taskIds: string[]): Promise<BatchDeleteResult> {
    const result = await this.batchDelete("meta_tasks", "task_id", taskIds);
    if (result.deleted_ids.length > 0) {
      const ph = result.deleted_ids.map(() => "?").join(",");
      await this.run(`DELETE FROM meta_task_agents WHERE task_id IN (${ph})`, result.deleted_ids);
    }
    return result;
  }

  async listTasksByTeam(
    teamId: string,
    pagination?: PaginationParams | null,
    filter?: TaskFilter,
  ): Promise<ListPage<TaskEntity>> {
    let where = "WHERE team_id = ?";
    const params: SqlValue[] = [teamId];
    if (filter?.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter?.creator_user_id) {
      where += " AND creator_user_id = ?";
      params.push(filter.creator_user_id);
    }
    if (filter?.title) {
      where += " AND title = ?";
      params.push(filter.title);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_tasks ${where}`,
      params,
      `SELECT * FROM meta_tasks ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapTask(r),
    );
  }

  async listTasks(filter: TaskFilter, pagination?: PaginationParams | null): Promise<ListPage<TaskEntity>> {
    let where = "WHERE 1=1";
    const params: SqlValue[] = [];
    if (filter.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter.creator_user_id) {
      where += " AND creator_user_id = ?";
      params.push(filter.creator_user_id);
    }
    if (filter.title) {
      where += " AND title = ?";
      params.push(filter.title);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_tasks ${where}`,
      params,
      `SELECT * FROM meta_tasks ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapTask(r),
    );
  }

  // ============================================================
  // TaskAgent
  // ============================================================
  async linkTaskAgent(taskId: string, agentId: string, roleInTask?: string): Promise<TaskAgentEntity> {
    const now = nowIso();
    await runWithGeneratedRelationIdAsync(undefined, isPgRelationIdCollision, async (id) => {
      await this.run(
        `INSERT INTO meta_task_agents (id, task_id, agent_id, role_in_task, status, created_at)
       VALUES (?,?,?,?,?,?)
       ON CONFLICT (task_id, agent_id) DO UPDATE SET role_in_task = EXCLUDED.role_in_task, status = 'active'`,
        [id, taskId, agentId, roleInTask ?? null, "active", now],
      );
    });
    return this.mapTaskAgent(
      await this.get("SELECT * FROM meta_task_agents WHERE task_id = ? AND agent_id = ?", [taskId, agentId]),
    )!;
  }

  async unlinkTaskAgent(taskId: string, agentId: string): Promise<void> {
    await this.run("DELETE FROM meta_task_agents WHERE task_id = ? AND agent_id = ?", [taskId, agentId]);
  }

  async listTaskAgents(taskId: string, pagination?: PaginationParams | null): Promise<ListPage<TaskAgentEntity>> {
    const base = "FROM meta_task_agents WHERE task_id = ? AND status = 'active'";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [taskId],
      `SELECT * ${base} ORDER BY created_at DESC`,
      [taskId],
      pagination,
      (r) => this.mapTaskAgent(r),
    );
  }

  // ============================================================
  // ParticipationLog
  // ============================================================
  async appendParticipationLog(input: AppendParticipationLogInput): Promise<ParticipationLogEntity> {
    const now = nowIso();
    const createdAt = input.created_at ?? now;
    const entity: ParticipationLogEntity = {
      id: generateRelationId(),
      team_id: input.team_id,
      task_id: input.task_id,
      agent_id: input.agent_id,
      user_id: input.user_id,
      source: input.source ?? "unknown",
      metadata_json: input.metadata_json ?? "{}",
      created_at: createdAt,
      updated_at: createdAt,
    };
    await this.run(
      `INSERT INTO meta_participation_logs
        (id, team_id, task_id, agent_id, user_id, source, metadata_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entity.id,
        entity.team_id,
        entity.task_id,
        entity.agent_id,
        entity.user_id,
        entity.source,
        entity.metadata_json,
        entity.created_at,
        entity.updated_at,
      ],
    );
    return entity;
  }

  async listParticipationLogs(
    filter: ParticipationLogFilter,
    pagination?: PaginationParams | null,
  ): Promise<ListPage<ParticipationLogEntity>> {
    const { sql, params } = this.buildParticipationLogWhere(filter);
    if (filter.dedupe) {
      const dedupeIds = `
        SELECT id FROM (
          SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC, id DESC) AS rn
          FROM meta_participation_logs
          WHERE ${sql}
        ) AS ranked WHERE rn = 1
      `;
      return this.selectList(
        `SELECT COUNT(*) AS c FROM (${dedupeIds}) AS deduped`,
        params,
        `SELECT * FROM meta_participation_logs WHERE id IN (${dedupeIds}) ORDER BY created_at DESC, id DESC`,
        params,
        pagination,
        (r) => this.mapParticipationLog(r),
      );
    }
    const base = `FROM meta_participation_logs WHERE ${sql}`;
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      params,
      `SELECT * ${base} ORDER BY created_at DESC, id DESC`,
      params,
      pagination,
      (r) => this.mapParticipationLog(r),
    );
  }

  private buildParticipationLogWhere(filter: ParticipationLogFilter): { sql: string; params: SqlValue[] } {
    const conditions = ["team_id = ?"];
    const params: SqlValue[] = [filter.team_id];
    if (filter.task_id) {
      conditions.push("task_id = ?");
      params.push(filter.task_id);
    }
    if (filter.agent_id) {
      conditions.push("agent_id = ?");
      params.push(filter.agent_id);
    }
    if (filter.user_id) {
      conditions.push("user_id = ?");
      params.push(filter.user_id);
    }
    if (filter.created_after) {
      conditions.push("created_at >= ?");
      params.push(filter.created_after);
    }
    if (filter.created_before) {
      conditions.push("created_at <= ?");
      params.push(filter.created_before);
    }
    return { sql: conditions.join(" AND "), params };
  }

  // ============================================================
  // Asset
  // ============================================================
  async createAsset(input: CreateAssetInput): Promise<AssetEntity> {
    const now = nowIso();
    const assetId = input.asset_id;
    await this.run(
      `INSERT INTO meta_assets
        (asset_id, team_id, asset_type, name, description, owner_user_id, source_type, source_ref,
         version, visibility, status, confidence, expires_at, last_used_at, usage_count, content_ref,
         created_at, updated_at, metadata_json)
       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        assetId,
        input.team_id,
        input.asset_type,
        input.name,
        input.description ?? null,
        input.owner_user_id,
        input.source_type,
        input.source_ref ?? null,
        1,
        input.visibility ?? "team",
        input.status ?? "draft",
        input.confidence ?? null,
        input.expires_at ?? null,
        null,
        0,
        input.content_ref ?? null,
        now,
        now,
        input.metadata_json ?? "{}",
      ],
    );
    return (await this.getAssetById(assetId))!;
  }

  async getAssetById(assetId: string): Promise<AssetEntity | null> {
    return this.mapAsset(await this.get("SELECT * FROM meta_assets WHERE asset_id = ?", [assetId]));
  }

  async updateAsset(assetId: string, patch: Partial<AssetEntity>): Promise<AssetEntity | null> {
    const allowed = ["name", "description", "visibility", "status", "confidence", "expires_at", "content_ref", "version", "source_ref", "metadata_json"] as const;
    await this.applyUpdate("meta_assets", "asset_id", assetId, allowed, patch);
    return this.getAssetById(assetId);
  }

  async deleteAssets(assetIds: string[]): Promise<BatchDeleteResult> {
    const result: BatchDeleteResult = { deleted_ids: [], failed: [] };
    for (const id of assetIds) {
      const existing = await this.getAssetById(id);
      if (!existing) {
        result.deleted_ids.push(id);
        continue;
      }
      await this.tx(async () => {
        await this.run("DELETE FROM meta_agent_fixed_assets WHERE asset_id = ?", [id]);
        await this.run("DELETE FROM meta_asset_acl WHERE asset_id = ?", [id]);
        await this.run("DELETE FROM meta_assets WHERE asset_id = ?", [id]);
      });
      result.deleted_ids.push(id);
    }
    return result;
  }

  async listAssetsByTeam(
    teamId: string,
    pagination?: PaginationParams | null,
    filter?: AssetFilter,
  ): Promise<ListPage<AssetEntity>> {
    let where = "WHERE team_id = ?";
    const params: SqlValue[] = [teamId];
    if (filter?.asset_type) {
      where += " AND asset_type = ?";
      params.push(filter.asset_type);
    }
    if (filter?.status) {
      where += " AND status = ?";
      params.push(filter.status);
    }
    if (filter?.owner_user_id) {
      where += " AND owner_user_id = ?";
      params.push(filter.owner_user_id);
    }
    if (filter?.visibility) {
      where += " AND visibility = ?";
      params.push(filter.visibility);
    }
    return this.selectList(
      `SELECT COUNT(*) AS c FROM meta_assets ${where}`,
      params,
      `SELECT * FROM meta_assets ${where} ORDER BY created_at DESC`,
      params,
      pagination,
      (r) => this.mapAsset(r),
    );
  }

  async touchAssetUsage(assetId: string): Promise<void> {
    await this.run(
      "UPDATE meta_assets SET usage_count = usage_count + 1, last_used_at = ? WHERE asset_id = ?",
      [nowIso(), assetId],
    );
  }

  // ============================================================
  // AgentFixedAsset
  // ============================================================
  async setAgentFixedAssets(agentId: string, bindings: FixedAssetBindingInput[]): Promise<void> {
    const now = nowIso();
    for (let attempt = 0; attempt < RELATION_ID_RETRY_LIMIT; attempt++) {
      try {
        await this.tx(async () => {
          await this.run("DELETE FROM meta_agent_fixed_assets WHERE agent_id = ?", [agentId]);
          for (const b of bindings) {
            await this.run(
              `INSERT INTO meta_agent_fixed_assets
            (id, agent_id, asset_id, asset_type, injection_mode, priority, created_by, created_at)
           VALUES (?,?,?,?,?,?,?,?)`,
              [
                generateRelationId(),
                agentId,
                b.asset_id,
                b.asset_type,
                b.injection_mode ?? "summary",
                b.priority ?? 50,
                b.created_by,
                now,
              ],
            );
          }
        });
        return;
      } catch (err) {
        if (isPgRelationIdCollision(err)) continue;
        throw err;
      }
    }
    throw new Error("relation id collision after max retries");
  }

  async addAgentFixedAsset(agentId: string, b: FixedAssetBindingInput): Promise<void> {
    await this.run(
      `INSERT INTO meta_agent_fixed_assets
        (id, agent_id, asset_id, asset_type, injection_mode, priority, created_by, created_at)
       VALUES (?,?,?,?,?,?,?,?)
       ON CONFLICT (agent_id, asset_id) DO NOTHING`,
      [
        generateRelationId(),
        agentId,
        b.asset_id,
        b.asset_type,
        b.injection_mode ?? "summary",
        b.priority ?? 50,
        b.created_by,
        nowIso(),
      ],
    );
  }

  async listAgentFixedAssets(
    agentId: string,
    pagination?: PaginationParams | null,
    filter?: { assetTypes?: readonly string[] },
  ): Promise<ListPage<FixedAssetBindingEntity>> {
    const types = filter?.assetTypes ?? [];
    if (types.length > 0) {
      const placeholders = types.map(() => "?").join(",");
      const base = `FROM meta_agent_fixed_assets b
        INNER JOIN meta_assets a ON a.asset_id = b.asset_id
        WHERE b.agent_id = ? AND a.asset_type IN (${placeholders})`;
      const params: SqlValue[] = [agentId, ...types];
      return this.selectList(
        `SELECT COUNT(*) AS c ${base}`,
        params,
        `SELECT b.* ${base} ORDER BY b.priority DESC, b.created_at DESC`,
        params,
        pagination,
        (r) => this.mapFixedAsset(r),
      );
    }
    const base = "FROM meta_agent_fixed_assets WHERE agent_id = ?";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [agentId],
      `SELECT * ${base} ORDER BY priority DESC, created_at DESC`,
      [agentId],
      pagination,
      (r) => this.mapFixedAsset(r),
    );
  }

  async getAgentFixedAsset(agentId: string, assetId: string): Promise<FixedAssetBindingEntity | null> {
    return this.mapFixedAsset(
      await this.get("SELECT * FROM meta_agent_fixed_assets WHERE agent_id = ? AND asset_id = ?", [agentId, assetId]),
    );
  }

  async summarizeAgentFixedAssetsByAgents(
    agentIds: string[],
    options?: { assetId?: string },
  ): Promise<AgentFixedAssetCountRow[]> {
    if (agentIds.length === 0) return [];
    const ph = agentIds.map(() => "?").join(",");
    const params: SqlValue[] = [...agentIds];
    let sql =
      `SELECT agent_id, asset_type, COUNT(DISTINCT asset_id) AS cnt
       FROM meta_agent_fixed_assets
       WHERE agent_id IN (${ph})`;
    if (options?.assetId) {
      sql += ` AND asset_id = ?`;
      params.push(options.assetId);
    }
    sql += ` GROUP BY agent_id, asset_type`;
    const rows = await this.all<{ agent_id: string; asset_type: string; cnt: number | string }>(sql, params);
    return rows.map((r) => ({
      agent_id: r.agent_id,
      asset_type: r.asset_type as AssetType,
      cnt: Number(r.cnt),
    }));
  }

  // ============================================================
  // ACL
  // ============================================================
  async grantAcl(input: GrantAclInput): Promise<AclEntity> {
    const now = nowIso();
    await runWithGeneratedRelationIdAsync(input.id, isPgRelationIdCollision, async (id) => {
      await this.run(
        `INSERT INTO meta_asset_acl
        (id, asset_id, subject_type, subject_id, permission, effect, granted_by, created_at, updated_at)
       VALUES (?,?,?,?,?,?,?,?,?)
       ON CONFLICT (asset_id, subject_type, subject_id, permission)
       DO UPDATE SET effect = EXCLUDED.effect, granted_by = EXCLUDED.granted_by, updated_at = EXCLUDED.updated_at`,
        [
          id,
          input.asset_id,
          input.subject_type,
          input.subject_id,
          input.permission,
          input.effect ?? "allow",
          input.granted_by,
          now,
          now,
        ],
      );
    });
    return this.mapAcl(
      await this.get(
        "SELECT * FROM meta_asset_acl WHERE asset_id = ? AND subject_type = ? AND subject_id = ? AND permission = ?",
        [input.asset_id, input.subject_type, input.subject_id, input.permission],
      ),
    )!;
  }

  async getAclById(id: string): Promise<AclEntity | null> {
    return this.mapAcl(await this.get("SELECT * FROM meta_asset_acl WHERE id = ?", [id]));
  }

  async revokeAcl(id: string): Promise<void> {
    await this.run("DELETE FROM meta_asset_acl WHERE id = ?", [id]);
  }

  async listAclByAsset(assetId: string, pagination?: PaginationParams | null): Promise<ListPage<AclEntity>> {
    const base = "FROM meta_asset_acl WHERE asset_id = ?";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [assetId],
      `SELECT * ${base} ORDER BY created_at DESC`,
      [assetId],
      pagination,
      (r) => this.mapAcl(r),
    );
  }

  async listAclBySubject(
    subjectType: string,
    subjectId: string,
    pagination?: PaginationParams | null,
  ): Promise<ListPage<AclEntity>> {
    const base = "FROM meta_asset_acl WHERE subject_type = ? AND subject_id = ?";
    return this.selectList(
      `SELECT COUNT(*) AS c ${base}`,
      [subjectType, subjectId],
      `SELECT * ${base} ORDER BY created_at DESC`,
      [subjectType, subjectId],
      pagination,
      (r) => this.mapAcl(r),
    );
  }

  // ============================================================
  // ConfigParam
  // ============================================================
  async getConfigParam(
    scope: "global" | "user",
    userId: string | null,
    module: string,
    paramName: string,
  ): Promise<ConfigParamEntity | null> {
    let row: Row | null;
    if (scope === "global") {
      row = await this.get<Row>(
        `SELECT * FROM meta_config_params WHERE scope = 'global' AND module = ? AND param_name = ?`,
        [module, paramName],
      );
    } else {
      row = await this.get<Row>(
        `SELECT * FROM meta_config_params WHERE scope = 'user' AND user_id = ? AND module = ? AND param_name = ?`,
        [userId!, module, paramName],
      );
    }
    return this.mapConfigParam(row);
  }

  async upsertConfigParam(input: UpsertConfigParamInput): Promise<ConfigParamEntity> {
    const now = nowIso();
    await this.tx(async () => {
      if (input.scope === "global") {
        const existing = await this.get<Row>(
          `SELECT id FROM meta_config_params WHERE scope = 'global' AND module = ? AND param_name = ?`,
          [input.module, input.param_name],
        );
        if (existing) {
          await this.run(
            `UPDATE meta_config_params SET param_value = ?, description = ?, updated_at = ? WHERE id = ?`,
            [input.param_value, input.description, now, existing.id],
          );
        } else {
          await this.run(
            `INSERT INTO meta_config_params (scope, user_id, module, param_name, param_value, description, created_at, updated_at)
             VALUES ('global', NULL, ?, ?, ?, ?, ?, ?)`,
            [input.module, input.param_name, input.param_value, input.description, now, now],
          );
        }
      } else {
        const existing = await this.get<Row>(
          `SELECT id FROM meta_config_params WHERE scope = 'user' AND user_id = ? AND module = ? AND param_name = ?`,
          [input.user_id!, input.module, input.param_name],
        );
        if (existing) {
          await this.run(
            `UPDATE meta_config_params SET param_value = ?, description = ?, updated_at = ? WHERE id = ?`,
            [input.param_value, input.description, now, existing.id],
          );
        } else {
          await this.run(
            `INSERT INTO meta_config_params (scope, user_id, module, param_name, param_value, description, created_at, updated_at)
             VALUES ('user', ?, ?, ?, ?, ?, ?, ?)`,
            [input.user_id!, input.module, input.param_name, input.param_value, input.description, now, now],
          );
        }
      }
    });

    return (await this.getConfigParam(
      input.scope,
      input.scope === "user" ? input.user_id! : null,
      input.module,
      input.param_name,
    ))!;
  }

  async listConfigParams(filter: ListConfigParamsFilter): Promise<ConfigParamEntity[]> {
    const conditions: string[] = [`module = ?`];
    const params: SqlValue[] = [filter.module];

    if (filter.scope) {
      conditions.push(`scope = ?`);
      params.push(filter.scope);
    }
    if (filter.userId) {
      conditions.push(`(scope = 'global' OR (scope = 'user' AND user_id = ?))`);
      params.push(filter.userId);
    }
    if (filter.paramNames && filter.paramNames.length > 0) {
      const placeholders = filter.paramNames.map(() => "?").join(", ");
      conditions.push(`param_name IN (${placeholders})`);
      params.push(...filter.paramNames);
    }

    const sql = `SELECT * FROM meta_config_params WHERE ${conditions.join(" AND ")} ORDER BY scope ASC, param_name ASC`;
    const rows = await this.all<Row>(sql, params);
    return rows.map((r) => this.mapConfigParam(r)!);
  }

  // ============================================================
  // Helpers
  // ============================================================
  private async applyUpdate<T>(
    table: string,
    pkCol: string,
    pkVal: string,
    allowed: readonly string[],
    patch: Partial<T>,
  ): Promise<void> {
    const fields: Record<string, SqlValue> = {};
    for (const k of allowed) {
      const v = (patch as Record<string, unknown>)[k];
      if (k in (patch as object) && v !== undefined) {
        fields[k] = v;
      }
    }
    await this.applyUpdateRaw(table, pkCol, pkVal, fields);
  }

  private async applyUpdateRaw(
    table: string,
    pkCol: string,
    pkVal: string,
    fields: Record<string, SqlValue>,
  ): Promise<void> {
    const keys = Object.keys(fields);
    const hasUpdatedAt = ["meta_users", "meta_teams", "meta_agents", "meta_tasks", "meta_assets"].includes(table);
    if (keys.length === 0 && !hasUpdatedAt) return;
    const sets = keys.map((k) => `${k} = ?`);
    const params: SqlValue[] = keys.map((k) => fields[k]);
    if (hasUpdatedAt) {
      sets.push("updated_at = ?");
      params.push(nowIso());
    }
    if (sets.length === 0) return;
    params.push(pkVal);
    await this.run(`UPDATE ${table} SET ${sets.join(", ")} WHERE ${pkCol} = ?`, params);
  }

  private async batchDelete(table: string, pkCol: string, ids: string[]): Promise<BatchDeleteResult> {
    const result: BatchDeleteResult = { deleted_ids: [], failed: [] };
    for (const id of ids) {
      const exists = await this.get(`SELECT ${pkCol} FROM ${table} WHERE ${pkCol} = ?`, [id]);
      if (!exists) {
        result.failed.push({ id, reason: "not_found" });
        continue;
      }
      await this.run(`DELETE FROM ${table} WHERE ${pkCol} = ?`, [id]);
      result.deleted_ids.push(id);
    }
    return result;
  }

  private mapUser(row: Row | null): UserEntity | null {
    if (!row) return null;
    return {
      user_id: String(row.user_id),
      password: row.password != null ? String(row.password) : null,
      auth_provider: String(row.auth_provider),
      external_id: String(row.external_id),
      username: String(row.username),
      display_name: row.display_name != null ? String(row.display_name) : null,
      email: row.email != null ? String(row.email) : null,
      raw_profile_json: String(row.raw_profile_json ?? "{}"),
      status: String(row.status) as UserEntity["status"],
      user_type: String(row.user_type ?? "normal") as UserEntity["user_type"],
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapUserKey(row: Row | null | undefined): UserKeyEntity | null {
    if (!row) return null;
    return {
      key_id: String(row.key_id),
      user_id: String(row.user_id),
      key_value: String(row.key_value),
      name: row.name != null ? String(row.name) : null,
      status: String(row.status) as UserKeyEntity["status"],
      is_default: Number(row.is_default) === 1,
      last_used_at: row.last_used_at != null ? String(row.last_used_at) : null,
      expires_at: row.expires_at != null ? String(row.expires_at) : null,
      created_at: String(row.created_at),
      revoked_at: row.revoked_at != null ? String(row.revoked_at) : null,
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapTeam(row: Row | null): TeamEntity | null {
    if (!row) return null;
    return {
      team_id: String(row.team_id),
      name: String(row.name),
      description: row.description != null ? String(row.description) : null,
      owner_user_id: String(row.owner_user_id),
      status: String(row.status) as TeamEntity["status"],
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapTeamMember(row: Row | null): TeamMemberEntity | null {
    if (!row) return null;
    return {
      id: String(row.id),
      team_id: String(row.team_id),
      user_id: String(row.user_id),
      role: row.role as TeamMemberEntity["role"],
      joined_at: String(row.joined_at),
      status: String(row.status) as TeamMemberEntity["status"],
    };
  }

  private mapAgent(row: Row | null): AgentEntity | null {
    if (!row) return null;
    return {
      agent_id: String(row.agent_id),
      team_id: String(row.team_id),
      owner_user_id: String(row.owner_user_id),
      name: String(row.name),
      description: row.description != null ? String(row.description) : null,
      prompt: row.prompt != null ? String(row.prompt) : null,
      visibility: String(row.visibility ?? "team") as AgentEntity["visibility"],
      status: String(row.status) as AgentEntity["status"],
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapTask(row: Row | null): TaskEntity | null {
    if (!row) return null;
    return {
      task_id: String(row.task_id),
      team_id: String(row.team_id),
      creator_user_id: String(row.creator_user_id),
      title: String(row.title),
      description: row.description != null ? String(row.description) : null,
      source_type: String(row.source_type) as TaskEntity["source_type"],
      source_url: row.source_url != null ? String(row.source_url) : null,
      status: String(row.status) as TaskEntity["status"],
      auto_assign_floating_assets: Number(row.auto_assign_floating_assets) === 1,
      risk_level: row.risk_level != null ? String(row.risk_level) : null,
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapTaskAgent(row: Row | null): TaskAgentEntity | null {
    if (!row) return null;
    return {
      id: String(row.id),
      task_id: String(row.task_id),
      agent_id: String(row.agent_id),
      role_in_task: row.role_in_task != null ? String(row.role_in_task) : null,
      status: String(row.status) as TaskAgentEntity["status"],
      created_at: String(row.created_at),
    };
  }

  private mapParticipationLog(row: Row | null): ParticipationLogEntity | null {
    if (!row) return null;
    return {
      id: String(row.id),
      team_id: String(row.team_id),
      task_id: String(row.task_id),
      agent_id: String(row.agent_id),
      user_id: String(row.user_id),
      source: String(row.source),
      metadata_json: String(row.metadata_json),
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
    };
  }

  private mapAsset(row: Row | null): AssetEntity | null {
    if (!row) return null;
    return {
      asset_id: String(row.asset_id),
      team_id: String(row.team_id),
      asset_type: String(row.asset_type) as AssetEntity["asset_type"],
      name: String(row.name),
      description: row.description != null ? String(row.description) : null,
      owner_user_id: String(row.owner_user_id),
      source_type: String(row.source_type),
      source_ref: row.source_ref != null ? String(row.source_ref) : null,
      version: Number(row.version ?? 1),
      visibility: String(row.visibility) as AssetEntity["visibility"],
      status: String(row.status) as AssetEntity["status"],
      confidence: row.confidence != null ? Number(row.confidence) : null,
      expires_at: row.expires_at != null ? String(row.expires_at) : null,
      last_used_at: row.last_used_at != null ? String(row.last_used_at) : null,
      usage_count: Number(row.usage_count ?? 0),
      content_ref: row.content_ref != null ? String(row.content_ref) : null,
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
      metadata_json: String(row.metadata_json ?? "{}"),
    };
  }

  private mapFixedAsset(row: Row | null): FixedAssetBindingEntity | null {
    if (!row) return null;
    return {
      id: String(row.id),
      agent_id: String(row.agent_id),
      asset_id: String(row.asset_id),
      asset_type: String(row.asset_type) as FixedAssetBindingEntity["asset_type"],
      injection_mode: String(row.injection_mode) as FixedAssetBindingEntity["injection_mode"],
      priority: Number(row.priority ?? 50),
      created_by: String(row.created_by),
      created_at: String(row.created_at),
    };
  }

  private mapAcl(row: Row | null): AclEntity | null {
    if (!row) return null;
    return {
      id: String(row.id),
      asset_id: String(row.asset_id),
      subject_type: String(row.subject_type) as AclEntity["subject_type"],
      subject_id: String(row.subject_id),
      permission: String(row.permission) as AclEntity["permission"],
      effect: String(row.effect) as AclEntity["effect"],
      granted_by: String(row.granted_by),
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
    };
  }

  private mapConfigParam(row: Row | null): ConfigParamEntity | null {
    if (!row) return null;
    return {
      id: Number(row.id),
      scope: String(row.scope) as ConfigParamEntity["scope"],
      user_id: row.user_id != null ? String(row.user_id) : null,
      module: String(row.module),
      param_name: String(row.param_name),
      param_value: String(row.param_value),
      description: String(row.description),
      created_at: String(row.created_at),
      updated_at: String(row.updated_at),
    };
  }
}

const SCHEMA_SQL = `
      CREATE TABLE IF NOT EXISTS meta_users (
        user_id TEXT PRIMARY KEY,
        password TEXT,
        auth_provider TEXT NOT NULL,
        external_id TEXT NOT NULL,
        username TEXT NOT NULL,
        display_name TEXT,
        email TEXT,
        raw_profile_json TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'active',
        user_type TEXT NOT NULL DEFAULT 'normal',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_users_system_admin ON meta_users(user_type) WHERE user_type = 'system_admin';
      CREATE INDEX IF NOT EXISTS idx_meta_users_auth_username ON meta_users(auth_provider, username);
      CREATE INDEX IF NOT EXISTS idx_meta_users_auth_external ON meta_users(auth_provider, external_id);
      CREATE INDEX IF NOT EXISTS idx_meta_users_email ON meta_users(email) WHERE email IS NOT NULL;
      CREATE TABLE IF NOT EXISTS meta_user_keys (
        key_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        key_value TEXT NOT NULL UNIQUE,
        name TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        is_default INTEGER NOT NULL DEFAULT 0,
        last_used_at TEXT,
        expires_at TEXT,
        created_at TEXT NOT NULL,
        revoked_at TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE INDEX IF NOT EXISTS idx_meta_user_keys_user ON meta_user_keys(user_id, status);
      CREATE TABLE IF NOT EXISTS meta_teams (
        team_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        owner_user_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE TABLE IF NOT EXISTS meta_team_members (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        joined_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        UNIQUE(team_id, user_id)
      );
      CREATE TABLE IF NOT EXISTS meta_agents (
        agent_id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        owner_user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        prompt TEXT,
        visibility TEXT NOT NULL DEFAULT 'team',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE INDEX IF NOT EXISTS idx_meta_agents_team_status ON meta_agents(team_id, status, created_at DESC);
      CREATE TABLE IF NOT EXISTS meta_tasks (
        task_id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        creator_user_id TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        source_type TEXT NOT NULL DEFAULT 'manual',
        source_url TEXT,
        status TEXT NOT NULL DEFAULT 'running',
        auto_assign_floating_assets INTEGER NOT NULL DEFAULT 0,
        risk_level TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE INDEX IF NOT EXISTS idx_meta_tasks_team_status ON meta_tasks(team_id, status, created_at DESC);
      CREATE TABLE IF NOT EXISTS meta_task_agents (
        id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        role_in_task TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        UNIQUE(task_id, agent_id)
      );
      CREATE TABLE IF NOT EXISTS meta_participation_logs (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'unknown',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS meta_assets (
        asset_id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        asset_type TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        owner_user_id TEXT NOT NULL,
        source_type TEXT NOT NULL,
        source_ref TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        visibility TEXT NOT NULL DEFAULT 'team',
        status TEXT NOT NULL DEFAULT 'draft',
        confidence DOUBLE PRECISION,
        expires_at TEXT,
        last_used_at TEXT,
        usage_count INTEGER NOT NULL DEFAULT 0,
        content_ref TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}'
      );
      CREATE INDEX IF NOT EXISTS idx_meta_assets_team_status ON meta_assets(team_id, status, created_at DESC);
      CREATE TABLE IF NOT EXISTS meta_agent_fixed_assets (
        id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        asset_type TEXT NOT NULL,
        injection_mode TEXT NOT NULL DEFAULT 'summary',
        priority INTEGER NOT NULL DEFAULT 50,
        created_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(agent_id, asset_id)
      );
      CREATE TABLE IF NOT EXISTS meta_asset_acl (
        id TEXT PRIMARY KEY,
        asset_id TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        permission TEXT NOT NULL,
        effect TEXT NOT NULL DEFAULT 'allow',
        granted_by TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(asset_id, subject_type, subject_id, permission)
      );
      CREATE INDEX IF NOT EXISTS idx_meta_users_created ON meta_users(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_user_keys_user_created ON meta_user_keys(user_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_teams_created ON meta_teams(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_members_user_status ON meta_team_members(user_id, status);
      CREATE INDEX IF NOT EXISTS idx_meta_members_team_status_joined ON meta_team_members(team_id, status, joined_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_agents_owner_status_created ON meta_agents(owner_user_id, status, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_tasks_creator_status_created ON meta_tasks(creator_user_id, status, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_task_agents_task_status_created ON meta_task_agents(task_id, status, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_pl_team_created ON meta_participation_logs(team_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_pl_team_task_agent_created ON meta_participation_logs(team_id, task_id, agent_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_pl_team_user_created ON meta_participation_logs(team_id, user_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_pl_team_dims_created ON meta_participation_logs(team_id, task_id, agent_id, user_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_fixed_agent_prio_created ON meta_agent_fixed_assets(agent_id, priority DESC, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_acl_asset_created ON meta_asset_acl(asset_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_meta_acl_subject_created ON meta_asset_acl(subject_type, subject_id, created_at DESC);

      CREATE TABLE IF NOT EXISTS meta_config_params (
        id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        scope TEXT NOT NULL CHECK (scope IN ('global', 'user')),
        user_id TEXT,
        module TEXT NOT NULL,
        param_name TEXT NOT NULL,
        param_value TEXT NOT NULL,
        description TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        CHECK (
          (scope = 'global' AND user_id IS NULL) OR
          (scope = 'user' AND user_id IS NOT NULL)
        )
      );
      CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_config_params_global
        ON meta_config_params(module, param_name) WHERE scope = 'global';
      CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_config_params_user
        ON meta_config_params(user_id, module, param_name) WHERE scope = 'user';
      CREATE INDEX IF NOT EXISTS idx_meta_config_params_module
        ON meta_config_params(module);
`;
