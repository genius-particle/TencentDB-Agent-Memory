import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { parseConfig } from "../../config.js";
import { executeMemorySearch } from "../tools/memory-search.js";
import { defaultStoreBackendRegistry } from "./registry.js";
import { ensureBuiltinStoreBackends } from "./backends.js";
import { hasPostgresEnv } from "./postgres-env.js";
import { createBM25Encoder } from "./bm25-local.js";
import { buildFtsQuery } from "./sqlite.js";
import { RRF_K } from "./search-utils.js";
import type { IMemoryStore, MemoryRecord } from "./types.js";
import type { EmbeddingService } from "./embedding.js";
import { PostgresMemoryStore } from "./postgres.js";

const hasPostgres = hasPostgresEnv();
const VECTOR_DIMS = 8;

function unitVector(dim: number, index: number): Float32Array {
  const v = new Float32Array(dim);
  v[index % dim] = 1;
  return v;
}

function mockEmbeddingService(signalIndex: number): EmbeddingService {
  return {
    embed: async () => unitVector(VECTOR_DIMS, signalIndex),
    embedBatch: async (texts: string[]) => texts.map(() => unitVector(VECTOR_DIMS, signalIndex)),
    getDimensions: () => VECTOR_DIMS,
    getProviderInfo: () => ({ provider: "mock", model: "mock" }),
    isReady: () => true,
    startWarmup: () => {},
  };
}

async function upsertL1(
  store: IMemoryStore,
  recordId: string,
  content: string,
  embedding?: Float32Array,
): Promise<void> {
  const now = new Date().toISOString();
  await store.upsertL1(
    {
      id: recordId,
      content,
      type: "instruction",
      priority: 50,
      scene_name: "",
      source_message_ids: [],
      metadata: {},
      timestamps: [now],
      createdAt: now,
      updatedAt: now,
      version: 1,
      sessionKey: "sess-1",
      sessionId: "sess-1",
      teamId: "team-1",
      userId: "user-1",
      agentId: "agent-1",
      taskId: "",
    } satisfies MemoryRecord,
    embedding,
  );
}

describe("PostgresMemoryStore capabilities (no database)", () => {
  it("never advertises nativeHybridSearch even with bm25 + vector dims", () => {
    const cfg = parseConfig({
      storeBackend: "postgres",
      bm25: { enabled: true, language: "en" },
      embedding: { provider: "openai_compatible", dimensions: VECTOR_DIMS, apiKey: "test" },
    });
    const bm25Encoder = createBM25Encoder(cfg.bm25);
    const store = new PostgresMemoryStore({
      connectionString: "postgres://unused/unused",
      schema: "mem_test_caps",
      dimensions: VECTOR_DIMS,
      bm25Encoder,
    });
    const caps = store.getCapabilities();
    expect(caps.vectorSearch).toBe(true);
    expect(caps.sparseVectors).toBe(true);
    expect(caps.nativeHybridSearch).toBe(false);
  });
});

describe.skipIf(!hasPostgres)("Postgres Chinese hybrid search", () => {
  let store: IMemoryStore;
  let schema: string;

  beforeEach(async () => {
    ensureBuiltinStoreBackends();
    const cfg = parseConfig({
      storeBackend: "postgres",
      bm25: { enabled: true, language: "en" },
      embedding: { provider: "openai_compatible", dimensions: VECTOR_DIMS, apiKey: "test" },
    });
    const bm25Encoder = createBM25Encoder(cfg.bm25);
    schema = `mem_search_${process.pid}_${Date.now().toString(36)}`;
    const created = defaultStoreBackendRegistry.create("postgres", {
      memoryCfg: cfg,
      dataDir: ".",
      instanceId: "search",
      postgresSchema: schema,
      bm25Encoder,
    });
    store = created.store;
    await store.init();
    expect(store.isDegraded()).toBe(false);
    expect(store.getCapabilities().nativeHybridSearch).toBe(false);
  });

  afterEach(() => {
    store?.close();
  });

  it("executeMemorySearch hits Chinese L1 via vector path when ILIKE phrase misses", async () => {
    const content =
      "用户团队在 2026年9月7日 确定了数据库迁移规范：生产环境统一使用 golang-migrate";
    const recordId = `l1-${randomUUID()}`;
    const embedding = unitVector(VECTOR_DIMS, 2);
    await upsertL1(store, recordId, content, embedding);

    const embeddingService = mockEmbeddingService(2);
    const result = await executeMemorySearch({
      query: "数据库迁移用什么工具",
      limit: 5,
      vectorStore: store,
      embeddingService,
    });

    expect(result.total).toBeGreaterThan(0);
    expect(result.results.some((r) => r.id === recordId)).toBe(true);
    expect(["embedding", "hybrid"]).toContain(result.strategy);
  });

  it("searchL1Hybrid RRF-merges when FTS and vector both hit the same record", async () => {
    if (!store.searchL1Hybrid || !store.searchL1Fts || !store.searchL1Vector) {
      throw new Error("searchL1Hybrid/searchL1Fts/searchL1Vector required");
    }

    const ftsToken = `zxqv-rrf-merge-${randomUUID()}`;
    const recordId = `l1-${randomUUID()}`;
    const query = ftsToken;
    const queryEmbedding = unitVector(VECTOR_DIMS, 1);

    await upsertL1(store, recordId, `${ftsToken} golang-migrate tool`, queryEmbedding);

    const ftsQuery = buildFtsQuery(query);
    expect(ftsQuery).toBeTruthy();

    const [ftsOnly, vecOnly] = await Promise.all([
      store.searchL1Fts(ftsQuery!, 5),
      store.searchL1Vector(queryEmbedding, 5),
    ]);
    expect(ftsOnly[0]?.record_id).toBe(recordId);
    expect(vecOnly[0]?.record_id).toBe(recordId);

    const hits = await store.searchL1Hybrid({
      query,
      queryEmbedding,
      topK: 5,
    });

    const merged = hits.find((h) => h.record_id === recordId);
    expect(merged).toBeDefined();
    expect(hits[0].record_id).toBe(recordId);
    // Rank 0 in both FTS and vector lists → RRF score 2/(K+1).
    expect(merged!.score).toBeCloseTo(2 / (RRF_K + 1), 5);
  });

  it("searchL1Hybrid uses jieba FTS tokens for Chinese queries", async () => {
    if (!store.searchL1Hybrid || !store.searchL1Fts) {
      throw new Error("searchL1Hybrid/searchL1Fts required");
    }

    const recordId = `l1-${randomUUID()}`;
    const content = "数据库迁移规范 golang-migrate";
    const query = "数据库迁移用什么工具";
    const embedding = unitVector(VECTOR_DIMS, 1);
    await upsertL1(store, recordId, content, embedding);

    const ftsQuery = buildFtsQuery(query);
    expect(ftsQuery).toBeTruthy();
    const ftsOnly = await store.searchL1Fts(ftsQuery!, 5);
    expect(ftsOnly.some((h) => h.record_id === recordId)).toBe(true);

    const hits = await store.searchL1Hybrid({
      query,
      queryEmbedding: unitVector(VECTOR_DIMS, 1),
      topK: 5,
    });
    expect(hits[0]?.record_id).toBe(recordId);
    expect(hits[0].score).toBeCloseTo(2 / (RRF_K + 1), 5);
  });
});

