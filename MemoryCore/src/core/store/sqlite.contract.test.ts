import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { parseConfig } from "../../config.js";
import { runMemoryStoreContract } from "./memory-store.contract.js";
import {
  defaultStoreBackendRegistry,
} from "./registry.js";
import { ensureBuiltinStoreBackends } from "./backends.js";
import type { IMemoryStore } from "./types.js";

describe("IMemoryStore contract: sqlite", () => {
  let dir: string;
  let store: IMemoryStore;

  beforeEach(async () => {
    ensureBuiltinStoreBackends();
    dir = mkdtempSync(path.join(tmpdir(), "memstore-sqlite-"));
    const cfg = parseConfig({
      storeBackend: "sqlite",
      bm25: { enabled: false },
      embedding: { provider: "none" },
    });
    const created = defaultStoreBackendRegistry.create("sqlite", {
      memoryCfg: cfg,
      dataDir: dir,
      instanceId: "default",
    });
    store = created.store;
    await store.init();
    expect(store.isDegraded()).toBe(false);
  });

  afterEach(() => {
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  });

  runMemoryStoreContract("sqlite", () => store);

  it("advertises the static feature flags SQLite actually implements", () => {
    const caps = store.getCapabilities();
    expect(caps.nativeHybridSearch).toBe(false);
    expect(caps.sparseVectors).toBe(false);
    expect(caps.profiles).toBe(false);
    expect(caps.entities).toBe(true);
    expect(caps.audit).toBe(true);
    expect(caps.prompts).toBe(true);
    expect(caps.generationRefs).toBe(true);
    expect(caps.knowledge).toBe(true);
    expect(caps.pagination).toBe(true);
    expect(caps.clearMemoryContent).toBe(true);
    expect(caps.deferredEmbedding).toBe(true);
    expect(caps.ftsSearch).toBe(store.isFtsAvailable());
    // dimensions=0 → vec0 deferred
    expect(caps.vectorSearch).toBe(false);
  });

  it("preserves same-DB Skill wiring via getRawDb()", () => {
    const raw = store as unknown as { getRawDb?: () => unknown };
    expect(typeof raw.getRawDb).toBe("function");
    expect(raw.getRawDb?.()).toBeTruthy();
  });
});
