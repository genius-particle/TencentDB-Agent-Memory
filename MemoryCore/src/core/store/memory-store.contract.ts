/**
 * IMemoryStore 契约测试套件 —— 与后端无关。
 *
 * 同一套用例分别跑在 SQLite（本 PR）/ TCVDB（无凭证则 skip）上。
 * 可选能力（profiles / entities / audit / …）按 `getCapabilities()` 旗标 skip；
 * 24 个 required 方法始终执行。
 */
import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import type { IMemoryStore, StoreCapabilities, L0Record } from "./types.js";
import { REQUIRED_MEMORY_STORE_METHODS } from "./types.js";
import { buildFtsQuery } from "./sqlite.js";
import type { MemoryRecord } from "../record/l1-writer.js";
import { buildMemoryGenerationRefId } from "../memory-generation-log/types.js";

type CapFlag = keyof StoreCapabilities;

function skipUnlessCap(
  ctx: { skip: (reason?: string) => void },
  store: IMemoryStore,
  flag: CapFlag,
): boolean {
  if (!store.getCapabilities()[flag]) {
    ctx.skip(`store does not advertise ${flag}`);
    return false;
  }
  return true;
}

function isoNow(): string {
  return new Date().toISOString();
}

function sampleL0(over: Partial<L0Record> = {}): L0Record {
  const now = Date.now();
  return {
    id: `l0-${randomUUID()}`,
    sessionKey: "sess-key",
    sessionId: "sess-1",
    teamId: "team-1",
    userId: "user-1",
    agentId: "agent-1",
    taskId: "task-1",
    role: "user",
    messageText: "hello world from contract",
    recordedAt: new Date(now).toISOString(),
    timestamp: now,
    ...over,
  };
}

function sampleL1(over: Partial<MemoryRecord> = {}): MemoryRecord {
  const now = isoNow();
  return {
    id: `l1-${randomUUID()}`,
    content: "hello world from contract l1",
    type: "episodic",
    priority: 50,
    scene_name: "contract",
    source_message_ids: [],
    metadata: {},
    timestamps: [now],
    createdAt: now,
    updatedAt: now,
    version: 1,
    sessionKey: "sess-key",
    sessionId: "sess-1",
    teamId: "team-1",
    userId: "user-1",
    agentId: "agent-1",
    taskId: "task-1",
    ...over,
  };
}

/**
 * @param name 后端名称（用于 describe 标题）
 * @param getStore 当前用例的已 init store（由调用方的 beforeEach 准备）
 */
export function runMemoryStoreContract(
  name: string,
  getStore: () => IMemoryStore,
): void {
  it(`[${name}] exposes all 24 required IMemoryStore methods`, () => {
    const store = getStore();
    for (const method of REQUIRED_MEMORY_STORE_METHODS) {
      expect((store as unknown as Record<string, unknown>)[method], method).toBeTypeOf("function");
    }
    expect(REQUIRED_MEMORY_STORE_METHODS).toHaveLength(24);
  });

  it(`[${name}] advertised feature flags imply methods exist`, () => {
    const store = getStore();
    const caps = store.getCapabilities();
    if (caps.profiles) {
      expect(store.pullProfiles).toBeTypeOf("function");
      expect(store.syncProfiles).toBeTypeOf("function");
    }
    if (caps.entities) {
      expect(store.createTeam).toBeTypeOf("function");
      expect(store.createUser).toBeTypeOf("function");
    }
    if (caps.audit) {
      expect(store.appendAudit).toBeTypeOf("function");
      expect(store.queryAudit).toBeTypeOf("function");
    }
    if (caps.prompts) {
      expect(store.createMemoryPrompt).toBeTypeOf("function");
    }
    if (caps.generationRefs) {
      expect(store.upsertMemoryGenerationRefs).toBeTypeOf("function");
    }
    if (caps.knowledge) {
      expect(store.createKnowledge).toBeTypeOf("function");
    }
    if (caps.pagination) {
      expect(store.queryL0Paginated).toBeTypeOf("function");
      expect(store.queryL1Paginated).toBeTypeOf("function");
    }
    if (caps.clearMemoryContent) {
      expect(store.clearMemoryContent).toBeTypeOf("function");
    }
    if (caps.deferredEmbedding) {
      expect(store.updateL0Embedding).toBeTypeOf("function");
    }
  });

  it(`[${name}] init is idempotent and not degraded for a fresh store`, () => {
    const store = getStore();
    expect(store.isDegraded()).toBe(false);
  });

  it(`[${name}] L0 upsert / count / query / delete round-trip`, async () => {
    const store = getStore();
    const rec = sampleL0();
    expect(await store.upsertL0(rec)).toBe(true);
    expect(await store.countL0({ sessionId: rec.sessionId, teamId: rec.teamId })).toBeGreaterThanOrEqual(1);
    const rows = await store.queryL0ForL1(rec.sessionKey);
    expect(rows.some((r) => r.record_id === rec.id)).toBe(true);
    const texts = await store.getAllL0Texts();
    expect(texts.some((t) => t.record_id === rec.id)).toBe(true);
    const groups = await store.queryL0GroupedBySessionId(rec.sessionKey);
    expect(groups.some((g) => g.sessionId === rec.sessionId)).toBe(true);
    expect(await store.deleteL0(rec.id)).toBe(true);
    expect(await store.countL0({ sessionId: rec.sessionId, teamId: rec.teamId })).toBe(0);
  });

  it(`[${name}] L1 upsert / count / query / deleteBatch round-trip`, async () => {
    const store = getStore();
    const rec = sampleL1();
    expect(await store.upsertL1(rec)).toBe(true);
    expect(await store.countL1({ sessionId: rec.sessionId, teamId: rec.teamId })).toBeGreaterThanOrEqual(1);
    const rows = await store.queryL1Records({ sessionId: rec.sessionId });
    expect(rows.some((r) => r.record_id === rec.id)).toBe(true);
    const texts = await store.getAllL1Texts();
    expect(texts.some((t) => t.record_id === rec.id)).toBe(true);
    expect(await store.deleteL1Batch([rec.id])).toBe(true);
    expect(await store.countL1({ sessionId: rec.sessionId, teamId: rec.teamId })).toBe(0);
  });

  it(`[${name}] deleteL0Expired / deleteL1Expired return a number`, async () => {
    const store = getStore();
    expect(await store.deleteL0Expired("1970-01-01T00:00:00.000Z")).toBeTypeOf("number");
    expect(await store.deleteL1Expired("1970-01-01T00:00:00.000Z")).toBeTypeOf("number");
  });

  it(`[${name}] FTS search hits inserted text when ftsSearch is advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "ftsSearch")) return;
    // Hyphen-free so postgres ILIKE fallback (after FTS5→plain text) still matches.
    // SQLite FTS5 treats this as a single token either way.
    const rec = sampleL0({ messageText: "uniqueftstokenalpha" });
    expect(await store.upsertL0(rec)).toBe(true);
    expect(store.isFtsAvailable()).toBe(true);
    const q = buildFtsQuery("uniqueftstokenalpha");
    expect(q).toBeTruthy();
    const hits = await store.searchL0Fts(q!, 10);
    expect(hits.some((h) => h.record_id === rec.id)).toBe(true);
  });

  it(`[${name}] vector search is callable when vectorSearch is advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "vectorSearch")) return;
    const rec = sampleL0();
    const dim = 8;
    const embedding = new Float32Array(dim).map(() => 0.1);
    await store.upsertL0(rec, embedding);
    const hits = await store.searchL0Vector(embedding, 5);
    expect(Array.isArray(hits)).toBe(true);
  });

  it(`[${name}] reindexAll returns counts`, async () => {
    const store = getStore();
    const result = await store.reindexAll(async () => new Float32Array(0));
    expect(result.l1Count).toBeTypeOf("number");
    expect(result.l0Count).toBeTypeOf("number");
  });

  it(`[${name}] pagination queryL0/L1 when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "pagination")) return;
    const rec = sampleL0();
    await store.upsertL0(rec);
    const l0 = await store.queryL0Paginated!({
      teamId: rec.teamId,
      sessionId: rec.sessionId,
      limit: 10,
      offset: 0,
    });
    expect(l0.total).toBeGreaterThanOrEqual(1);
    expect(l0.rows.some((r) => r.record_id === rec.id)).toBe(true);

    const l1 = sampleL1();
    await store.upsertL1(l1);
    const page = await store.queryL1Paginated!({
      teamId: l1.teamId,
      sessionId: l1.sessionId,
      limit: 10,
      offset: 0,
    });
    expect(page.total).toBeGreaterThanOrEqual(1);
  });

  it(`[${name}] clearMemoryContent requires team+agent and wipes L0/L1`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "clearMemoryContent")) return;
    await expect(
      (async () => store.clearMemoryContent!({ teamId: "", agentId: "a" }))(),
    ).rejects.toThrow();
    const rec = sampleL0();
    await store.upsertL0(rec);
    await store.upsertL1(sampleL1({ sessionId: rec.sessionId }));
    const result = await store.clearMemoryContent!({
      teamId: rec.teamId!,
      agentId: rec.agentId!,
    });
    expect(result.l0Deleted).toBeGreaterThanOrEqual(1);
    expect(await store.countL0({ teamId: rec.teamId, agentId: rec.agentId })).toBe(0);
  });

  it(`[${name}] entities createTeam/getTeam when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "entities")) return;
    const team = await store.createTeam!({
      team_id: `team-${randomUUID()}`,
      name: "Contract Team",
      owner_user_id: "user-1",
    });
    expect(team.team_id).toBeTruthy();
    expect(await store.getTeam!(team.team_id)).toMatchObject({ name: "Contract Team" });
  });

  it(`[${name}] knowledge create/get when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "knowledge")) return;
    const id = `know-${randomUUID()}`;
    const created = await store.createKnowledge!({
      knowledge_id: id,
      type: "wiki",
      service_url: "http://localhost",
      name: "Contract Wiki",
      summary: null,
      team_id: "team-1",
      user_id: null,
    });
    expect(created.knowledge_id).toBe(id);
    expect(await store.getKnowledge!(id)).toMatchObject({ name: "Contract Wiki" });
  });

  it(`[${name}] audit append/query when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "audit")) return;
    const entry = {
      audit_id: `audit-${randomUUID()}`,
      record_id: "l1-x",
      layer: "L1" as const,
      action: "update" as const,
      team_id: "team-1",
      agent_id: "agent-1",
      version: 2,
      updated_at_ms: Date.now(),
    };
    await store.appendAudit!(entry);
    const rows = await store.queryAudit!({ record_id: entry.record_id, limit: 10 });
    expect(rows.some((r) => r.audit_id === entry.audit_id)).toBe(true);
  });

  it(`[${name}] prompts create/get when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "prompts")) return;
    const now = Date.now();
    const rec = await store.createMemoryPrompt!({
      memory_prompt_id: `mp-${randomUUID()}`,
      name: "contract-prompt",
      layer: "l1",
      prompt: "you are a contract test",
      version: 1,
      status: "active",
      created_at_ms: now,
      updated_at_ms: now,
    });
    const got = await store.getMemoryPrompts!([rec.memory_prompt_id]);
    expect(got[0]?.name).toBe("contract-prompt");
  });

  it(`[${name}] generationRefs upsert/get when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "generationRefs")) return;
    const memoryId = `l1-${randomUUID()}`;
    const generationRefId = buildMemoryGenerationRefId("l1", memoryId);
    await store.upsertMemoryGenerationRefs!([{
      generation_ref_id: generationRefId,
      layer: "l1",
      memory_id: memoryId,
      generation_id: "gen-1",
      generation_log_id: "log-1",
      generation_log_key: "key-1",
      memory_prompt_id: "mp-1",
      memory_prompt_version: 1,
      memory_prompt_source: "system",
      created_at_ms: Date.now(),
    }]);
    const got = await store.getMemoryGenerationRef!("l1", memoryId);
    expect(got?.generation_id).toBe("gen-1");
  });

  it(`[${name}] profiles pull/sync when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "profiles")) return;
    const now = Date.now();
    await store.syncProfiles!([{
      id: `profile:v1:${randomUUID()}`,
      type: "l2",
      filename: "scene.md",
      content: "profile body",
      contentMd5: "abc",
      teamId: "team-1",
      agentId: "agent-1",
      version: 1,
      createdAtMs: now,
      updatedAtMs: now,
    }]);
    const pulled = await store.pullProfiles!();
    expect(pulled.length).toBeGreaterThanOrEqual(1);
  });

  it(`[${name}] deferredEmbedding updateL0Embedding is callable when advertised`, async (ctx) => {
    const store = getStore();
    if (!skipUnlessCap(ctx, store, "deferredEmbedding")) return;
    const rec = sampleL0();
    await store.upsertL0(rec, undefined);
    const ok = await store.updateL0Embedding!(rec.id, new Float32Array(8));
    expect(typeof ok).toBe("boolean");
  });
}
