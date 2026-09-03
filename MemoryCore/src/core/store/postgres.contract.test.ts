import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { parseConfig } from "../../config.js";
import { runMemoryStoreContract } from "./memory-store.contract.js";
import { defaultStoreBackendRegistry } from "./registry.js";
import { ensureBuiltinStoreBackends } from "./backends.js";
import { hasPostgresEnv, postgresSchemaForInstance } from "./postgres-env.js";
import { createBM25Encoder } from "./bm25-local.js";
import type { IMemoryStore } from "./types.js";

/**
 * Postgres IMemoryStore contract. Skips unless DATABASE_URL or PG* is set
 * (same pattern as TCVDB-without-creds). `npm test` stays green in CI
 * without a Postgres instance.
 */
const hasPostgres = hasPostgresEnv();

describe.skipIf(!hasPostgres)("IMemoryStore contract: postgres", () => {
  let store: IMemoryStore;

  beforeEach(async () => {
    ensureBuiltinStoreBackends();
    const cfg = parseConfig({
      storeBackend: "postgres",
      bm25: { enabled: true, language: "zh" },
      embedding: { provider: "none" },
    });
    const bm25Encoder = createBM25Encoder(cfg.bm25);
    // Unique schema per test so sparse FTS (LIMIT 10) is not crowded by
    // leftover rows from earlier cases in this file.
    const schema = `mem_c_${process.pid}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    const created = defaultStoreBackendRegistry.create("postgres", {
      memoryCfg: cfg,
      dataDir: ".",
      instanceId: "contract",
      postgresSchema: schema,
      bm25Encoder,
    });
    store = created.store;
    await store.init();
    expect(store.isDegraded()).toBe(false);
  });

  afterEach(() => {
    store?.close();
  });

  runMemoryStoreContract("postgres", () => store);

  it("advertises honest postgres feature flags", () => {
    const caps = store.getCapabilities();
    expect(caps.profiles).toBe(true);
    expect(caps.pagination).toBe(true);
    expect(caps.audit).toBe(true);
    expect(caps.clearMemoryContent).toBe(true);
    expect(caps.deferredEmbedding).toBe(true);
    expect(caps.entities).toBe(false);
    expect(caps.knowledge).toBe(false);
    expect(caps.prompts).toBe(false);
    expect(caps.generationRefs).toBe(false);
    expect(caps.sparseVectors).toBe(true);
    expect(caps.ftsSearch).toBe(true);
    expect(caps.nativeHybridSearch).toBe(false);
    expect(caps.vectorSearch).toBe(false);
  });
});

describe("postgres helpers (no database required)", () => {
  it("postgresSchemaForInstance sanitizes identifiers", () => {
    expect(postgresSchemaForInstance("default")).toBe("mem_default");
    expect(postgresSchemaForInstance("inst-9")).toBe("mem_inst_9");
    expect(postgresSchemaForInstance("9abc")).toBe("mem_9abc");
  });
});
