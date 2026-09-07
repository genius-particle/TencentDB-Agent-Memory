/**
 * RedisStateBackend — fork/open-source Pipeline state backend.
 *
 * Implements IStateBackend for standalone deployments that need durable
 * cross-restart state without the private src/integrations/redis submodule.
 */

import type Redis from "ioredis";
import type {
  CaptureAtomicParams,
  CaptureAtomicResult,
  IStateBackend,
  PipelineSessionState,
  TaskPayload,
  TimerEntry,
} from "./types.js";
import { DEFAULT_PIPELINE_STATE } from "./types.js";
import {
  LUA_ACQUIRE_LOCK,
  LUA_CAPTURE_ATOMIC,
  LUA_CLAIM_EXPIRED_TIMERS,
  LUA_RELEASE_LOCK,
  LUA_RENEW_LOCK,
} from "./redis-lua.js";

export interface RedisStateBackendOptions {
  client: Redis;
  keyPrefix?: string;
  consumerGroup?: string;
}

const TIMER_SHARD_COUNT = 16;
const DEFAULT_CONSUMER_GROUP = "tdai_pipeline";

type TaskPayloadWithMsgId = TaskPayload & { _msgId?: string };

function fnv1a(str: string): number {
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function sessionSlot(instanceId: string, teamId?: string, agentId?: string): string {
  const tid = teamId || "_";
  const aid = agentId || "_";
  if (teamId && agentId) {
    return `{${instanceId}:${tid}:${aid}}`;
  }
  return `{${instanceId}}`;
}

function parseStateHash(raw: Record<string, string>): PipelineSessionState | null {
  if (!raw || Object.keys(raw).length === 0) return null;
  return {
    conversation_count: Number(raw.conversation_count ?? 0),
    last_extraction_time: raw.last_extraction_time ?? "",
    last_extraction_updated_time: raw.last_extraction_updated_time ?? "",
    last_active_time: Number(raw.last_active_time ?? 0),
    l2_pending_l1_count: Number(raw.l2_pending_l1_count ?? 0),
    warmup_threshold: Number(raw.warmup_threshold ?? 0),
    l2_last_extraction_time: raw.l2_last_extraction_time ?? "",
  };
}

function stateToHash(state: PipelineSessionState): Record<string, string> {
  return {
    conversation_count: String(state.conversation_count),
    last_extraction_time: state.last_extraction_time,
    last_extraction_updated_time: state.last_extraction_updated_time,
    last_active_time: String(state.last_active_time),
    l2_pending_l1_count: String(state.l2_pending_l1_count),
    warmup_threshold: String(state.warmup_threshold),
    l2_last_extraction_time: state.l2_last_extraction_time,
  };
}

export class RedisStateBackend implements IStateBackend {
  readonly timerShardCount = TIMER_SHARD_COUNT;

  private readonly client: Redis;
  private readonly prefix: string;
  private readonly consumerGroup: string;
  private readonly streamKey: string;
  private readonly lockPrefix: string;
  private readonly sessionsIndexKey: string;

  private claimTimersSha: string | null = null;
  private captureAtomicSha: string | null = null;
  private acquireLockSha: string | null = null;
  private renewLockSha: string | null = null;
  private releaseLockSha: string | null = null;
  private destroyed = false;

  constructor(opts: RedisStateBackendOptions) {
    this.client = opts.client;
    this.prefix = opts.keyPrefix ?? "tdai_memory_v2";
    this.consumerGroup = opts.consumerGroup ?? DEFAULT_CONSUMER_GROUP;
    this.streamKey = `${this.prefix}:tasks`;
    this.lockPrefix = `${this.prefix}:lock:`;
    this.sessionsIndexKey = `${this.prefix}:sessions`;
  }

  getClient(): Redis {
    return this.client;
  }

  getTimerShardKeyByIndex(shard: number): string {
    return `${this.prefix}:timers:shard_${shard}`;
  }

  private stateKey(instanceId: string, sessionId: string, teamId?: string, agentId?: string): string {
    return `${this.prefix}:sk:${sessionSlot(instanceId, teamId, agentId)}:${sessionId}`;
  }

  private bufferKey(instanceId: string, sessionId: string, teamId?: string, agentId?: string): string {
    return `${this.prefix}:bk:${sessionSlot(instanceId, teamId, agentId)}:${sessionId}`;
  }

  private timerShardFor(instanceId: string, member: string): number {
    return fnv1a(`${instanceId}\x00${member}`) % TIMER_SHARD_COUNT;
  }

  private timerMember(instanceId: string, member: string): string {
    return `${instanceId}\x00${member}`;
  }

  private lockKey(key: string): string {
    return `${this.lockPrefix}${key}`;
  }

  private async loadScript(source: string): Promise<string> {
    return this.client.script("LOAD", source) as Promise<string>;
  }

  async initialize(): Promise<void> {
    try {
      await this.client.xgroup("CREATE", this.streamKey, this.consumerGroup, "$", "MKSTREAM");
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes("BUSYGROUP")) throw err;
    }

    this.claimTimersSha = await this.loadScript(LUA_CLAIM_EXPIRED_TIMERS);
    this.captureAtomicSha = await this.loadScript(LUA_CAPTURE_ATOMIC);
    this.acquireLockSha = await this.loadScript(LUA_ACQUIRE_LOCK);
    this.renewLockSha = await this.loadScript(LUA_RENEW_LOCK);
    this.releaseLockSha = await this.loadScript(LUA_RELEASE_LOCK);
  }

  async destroy(): Promise<void> {
    this.destroyed = true;
  }

  // ═══ Buffer ═══

  async appendBuffer(instanceId: string, sessionId: string, message: string, teamId?: string, agentId?: string): Promise<void> {
    await this.client.rpush(this.bufferKey(instanceId, sessionId, teamId, agentId), message);
    await this.client.sadd(this.sessionsIndexKey, `${instanceId}:${teamId || "_"}:${agentId || "_"}:${sessionId}`);
  }

  async drainBuffer(instanceId: string, sessionId: string, teamId?: string, agentId?: string): Promise<string[]> {
    const key = this.bufferKey(instanceId, sessionId, teamId, agentId);
    const len = await this.client.llen(key);
    if (len === 0) return [];
    const pipe = this.client.pipeline();
    pipe.lrange(key, 0, -1);
    pipe.del(key);
    const results = await pipe.exec();
    const rows = results?.[0]?.[1];
    return Array.isArray(rows) ? (rows as string[]) : [];
  }

  async getBufferLength(instanceId: string, sessionId: string, teamId?: string, agentId?: string): Promise<number> {
    return this.client.llen(this.bufferKey(instanceId, sessionId, teamId, agentId));
  }

  // ═══ Session State ═══

  async getSessionState(instanceId: string, sessionId: string, teamId?: string, agentId?: string): Promise<PipelineSessionState | null> {
    const raw = await this.client.hgetall(this.stateKey(instanceId, sessionId, teamId, agentId));
    return parseStateHash(raw);
  }

  async updateSessionState(
    instanceId: string,
    sessionId: string,
    patch: Partial<PipelineSessionState>,
    teamId?: string,
    agentId?: string,
  ): Promise<void> {
    const key = this.stateKey(instanceId, sessionId, teamId, agentId);
    const current = (await this.getSessionState(instanceId, sessionId, teamId, agentId))
      ?? { ...DEFAULT_PIPELINE_STATE, last_active_time: Date.now() };
    const merged = { ...current, ...patch };
    await this.client.hset(key, stateToHash(merged));
    await this.client.sadd(this.sessionsIndexKey, `${instanceId}:${teamId || "_"}:${agentId || "_"}:${sessionId}`);
  }

  async deleteSessionState(instanceId: string, sessionId: string, teamId?: string, agentId?: string): Promise<void> {
    const slot = `${instanceId}:${teamId || "_"}:${agentId || "_"}:${sessionId}`;
    await this.client.del(this.stateKey(instanceId, sessionId, teamId, agentId));
    await this.client.del(this.bufferKey(instanceId, sessionId, teamId, agentId));
    await this.client.srem(this.sessionsIndexKey, slot);
  }

  async listActiveSessions(instanceId: string): Promise<string[]> {
    const members = await this.client.smembers(this.sessionsIndexKey);
    const prefix = `${instanceId}:`;
    const sessions: string[] = [];
    for (const m of members) {
      if (!m.startsWith(prefix)) continue;
      const parts = m.split(":");
      if (parts.length >= 4) sessions.push(parts.slice(3).join(":"));
    }
    return sessions;
  }

  // ═══ Timer ═══

  async setTimer(instanceId: string, member: string, fireAtMs: number): Promise<void> {
    const shard = this.timerShardFor(instanceId, member);
    const zkey = this.getTimerShardKeyByIndex(shard);
    const zmember = this.timerMember(instanceId, member);
    await this.client.zadd(zkey, fireAtMs, zmember);
  }

  async setTimerIfEarlier(instanceId: string, member: string, fireAtMs: number): Promise<boolean> {
    const shard = this.timerShardFor(instanceId, member);
    const zkey = this.getTimerShardKeyByIndex(shard);
    const zmember = this.timerMember(instanceId, member);
    const existing = await this.client.zscore(zkey, zmember);
    if (existing !== null && fireAtMs >= Number(existing)) return false;
    await this.client.zadd(zkey, fireAtMs, zmember);
    return true;
  }

  async removeTimer(instanceId: string, member: string): Promise<void> {
    const shard = this.timerShardFor(instanceId, member);
    const zkey = this.getTimerShardKeyByIndex(shard);
    await this.client.zrem(zkey, this.timerMember(instanceId, member));
  }

  async getExpiredTimers(instanceId: string, nowMs: number): Promise<TimerEntry[]> {
    const expired: TimerEntry[] = [];
    for (let shard = 0; shard < TIMER_SHARD_COUNT; shard++) {
      const zkey = this.getTimerShardKeyByIndex(shard);
      const members = await this.client.zrangebyscore(zkey, 0, nowMs);
      for (const zmember of members) {
        const sep = zmember.indexOf("\x00");
        const inst = sep >= 0 ? zmember.slice(0, sep) : "";
        if (inst !== instanceId) continue;
        const member = sep >= 0 ? zmember.slice(sep + 1) : zmember;
        await this.client.zrem(zkey, zmember);
        expired.push({ instanceId: inst, member, fireAtMs: nowMs });
      }
    }
    return expired;
  }

  async claimExpiredFromShard(shardKey: string, nowMs: number, batchSize: number): Promise<TimerEntry[]> {
    const sha = this.claimTimersSha!;
    const raw = (await this.client.evalsha(
      sha,
      1,
      shardKey,
      String(nowMs),
      String(batchSize),
    )) as string[];

    return raw.map((zmember) => {
      const sep = zmember.indexOf("\x00");
      const instanceId = sep >= 0 ? zmember.slice(0, sep) : "";
      const member = sep >= 0 ? zmember.slice(sep + 1) : zmember;
      return { instanceId, member, fireAtMs: nowMs };
    });
  }

  // ═══ Task Queue ═══

  async enqueueTask(task: TaskPayload): Promise<void> {
    await this.client.xadd(this.streamKey, "*", "payload", JSON.stringify(task));
  }

  async consumeTask(workerId: string, blockMs?: number): Promise<TaskPayload | null> {
    const deadline = Date.now() + Math.max(0, blockMs ?? 0);
    do {
      const rows = await this.client.xreadgroup(
        "GROUP",
        this.consumerGroup,
        workerId,
        "COUNT",
        1,
        "STREAMS",
        this.streamKey,
        ">",
      );
      const parsed = this.parseStreamRows(rows);
      if (parsed) return parsed;

      if (!blockMs || blockMs <= 0) return null;
      await new Promise((r) => setTimeout(r, Math.min(200, deadline - Date.now())));
    } while (Date.now() < deadline);
    return null;
  }

  async ackTask(taskId: string): Promise<void> {
    await this.client.xack(this.streamKey, this.consumerGroup, taskId);
  }

  async getQueueDepth(): Promise<{ high: number; low: number }> {
    let high = 0;
    let low = 0;
    const tasks = await this.listQueuedTasks();
    for (const t of tasks) {
      if (t.priority === 0) high++;
      else low++;
    }
    return { high, low };
  }

  async listQueuedTasks(): Promise<TaskPayload[]> {
    type GroupInfo = Record<string, string | number>;
    const groups = await this.client.xinfo("GROUPS", this.streamKey) as GroupInfo[];
    let lastDelivered = "0-0";
    for (const g of groups) {
      const name = g.name ?? g["name"];
      if (name === this.consumerGroup) {
        lastDelivered = String(g["last-delivered-id"] ?? "0-0");
        break;
      }
    }
    const start = lastDelivered === "0-0" ? "-" : bumpStreamId(lastDelivered);
    const rows = await this.client.xrange(this.streamKey, start, "+");
    return rows.map(([, fields]) => parseTaskFields(fields)).filter(Boolean) as TaskPayload[];
  }

  async claimStaleTasks(workerId: string, minIdleMs: number, count: number): Promise<TaskPayload[]> {
    const pending = await this.client.xpending(this.streamKey, this.consumerGroup, "-", "+", count) as unknown[];
    if (!Array.isArray(pending) || pending.length === 0) return [];

    const msgIds: string[] = [];
    for (const entry of pending) {
      const row = entry as [string, string, number, number];
      const idleMs = row[3];
      if (idleMs >= minIdleMs) msgIds.push(row[0]);
    }
    if (msgIds.length === 0) return [];

    const claimed = await this.client.xclaim(
      this.streamKey,
      this.consumerGroup,
      workerId,
      minIdleMs,
      ...msgIds,
    ) as [string, string[]][];

    return claimed.map(([id, fields]) => {
      const task = parseTaskFields(fields);
      if (!task) return null;
      (task as TaskPayloadWithMsgId)._msgId = id;
      return task;
    }).filter(Boolean) as TaskPayload[];
  }

  // ═══ Lock ═══

  async acquireLock(key: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const sha = this.acquireLockSha!;
    const result = await this.client.evalsha(sha, 1, this.lockKey(key), ownerId, String(ttlMs));
    return Number(result) === 1;
  }

  async renewLock(key: string, ownerId: string, ttlMs: number): Promise<boolean> {
    const sha = this.renewLockSha!;
    const result = await this.client.evalsha(sha, 1, this.lockKey(key), ownerId, String(ttlMs));
    return Number(result) === 1;
  }

  async releaseLock(key: string, ownerId: string): Promise<void> {
    const sha = this.releaseLockSha!;
    await this.client.evalsha(sha, 1, this.lockKey(key), ownerId);
  }

  // ═══ Atomic Capture ═══

  async captureAtomic(params: CaptureAtomicParams): Promise<CaptureAtomicResult> {
    const {
      instanceId,
      sessionId,
      teamId,
      agentId,
      messageJson,
      threshold,
      fireAtMs,
      timerMember,
      taskPayload,
      nowMs,
      rounds,
    } = params;

    const stateKey = this.stateKey(instanceId, sessionId, teamId, agentId);
    const bufKey = this.bufferKey(instanceId, sessionId, teamId, agentId);
    const shard = this.timerShardFor(instanceId, timerMember);
    const timerKey = this.getTimerShardKeyByIndex(shard);
    const zmember = this.timerMember(instanceId, timerMember);

    const sha = this.captureAtomicSha!;
    const raw = await this.client.evalsha(
      sha,
      4,
      stateKey,
      bufKey,
      timerKey,
      this.streamKey,
      messageJson ?? "",
      String(rounds),
      String(threshold),
      String(nowMs),
      String(fireAtMs),
      zmember,
      JSON.stringify(taskPayload),
      JSON.stringify(DEFAULT_PIPELINE_STATE),
    ) as number[];

    await this.client.sadd(this.sessionsIndexKey, `${instanceId}:${teamId || "_"}:${agentId || "_"}:${sessionId}`);

    return {
      triggered: raw[0] === 1,
      conversationCount: Number(raw[1] ?? 0),
    };
  }

  // ═══ Instance Lifecycle ═══

  async purgeInstance(instanceId: string): Promise<{ sessions: number; timers: number; buffers: number }> {
    let sessions = 0;
    let buffers = 0;
    let timers = 0;

    const members = await this.client.smembers(this.sessionsIndexKey);
    const toRemove: string[] = [];
    for (const m of members) {
      if (!m.startsWith(`${instanceId}:`)) continue;
      const parts = m.split(":");
      if (parts.length < 4) continue;
      const teamId = parts[1] === "_" ? undefined : parts[1];
      const agentId = parts[2] === "_" ? undefined : parts[2];
      const sessionId = parts.slice(3).join(":");
      await this.deleteSessionState(instanceId, sessionId, teamId, agentId);
      sessions++;
      buffers++;
      toRemove.push(m);
    }
    if (toRemove.length) await this.client.srem(this.sessionsIndexKey, ...toRemove);

    for (let shard = 0; shard < TIMER_SHARD_COUNT; shard++) {
      const zkey = this.getTimerShardKeyByIndex(shard);
      const all = await this.client.zrange(zkey, 0, -1);
      for (const zmember of all) {
        if (zmember.startsWith(`${instanceId}\x00`)) {
          await this.client.zrem(zkey, zmember);
          timers++;
        }
      }
    }

    // Remove queued tasks for this instance (best-effort scan)
    const queued = await this.listQueuedTasks();
    // Cannot delete from stream easily; tasks will be ACK'd when processed or stale-claimed

    return { sessions, timers, buffers };
  }

  private parseStreamRows(rows: unknown): TaskPayload | null {
    if (!rows || !Array.isArray(rows) || rows.length === 0) return null;
    const [, messages] = rows[0] as [string, [string, string[]][]];
    if (!messages?.length) return null;
    const [msgId, fields] = messages[0];
    const task = parseTaskFields(fields);
    if (!task) return null;
    (task as TaskPayloadWithMsgId)._msgId = msgId;
    return task;
  }
}

function parseTaskFields(fields: string[]): TaskPayload | null {
  for (let i = 0; i < fields.length; i += 2) {
    if (fields[i] === "payload") {
      try {
        return JSON.parse(fields[i + 1]) as TaskPayload;
      } catch {
        return null;
      }
    }
  }
  return null;
}

/** Increment stream ID for XRANGE start (exclude last-delivered). */
function bumpStreamId(id: string): string {
  const [ms, seq] = id.split("-");
  return `${ms}-${Number(seq) + 1}`;
}

export { RedisStateBackend as default };
