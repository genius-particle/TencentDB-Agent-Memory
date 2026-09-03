import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { parseConfig } from "../../config.js";
import { createStoreBundle } from "./factory.js";
import { StorePool } from "./store-pool.js";
import {
  StoreBackendRegistry,
  defaultStoreBackendRegistry,
  resolvePooledStoreBackend,
} from "./registry.js";
import { ensureBuiltinStoreBackends } from "./backends.js";
import { getSqlitePath } from "./sqlite-backend.js";

const silentLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

function testConfig(over: Record<string, unknown> = {}) {
  return parseConfig({
    storeBackend: "sqlite",
    bm25: { enabled: false },
    embedding: { provider: "none" },
    ...over,
  });
}

describe("StoreBackendRegistry", () => {
  let dir: string;

  beforeEach(() => {
    ensureBuiltinStoreBackends();
    dir = mkdtempSync(path.join(tmpdir(), "memstore-registry-"));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
    vi.restoreAllMocks();
  });

  it("registers sqlite, tcvdb, and postgres as first-class backends (no silent default flip)", () => {
    expect(defaultStoreBackendRegistry.has("sqlite")).toBe(true);
    expect(defaultStoreBackendRegistry.has("tcvdb")).toBe(true);
    expect(defaultStoreBackendRegistry.has("postgres")).toBe(true);
    expect(defaultStoreBackendRegistry.list().sort()).toEqual(["postgres", "sqlite", "tcvdb"]);
  });

  it("createStoreBundle and StorePool resolve sqlite through the same registry", async () => {
    const spy = vi.spyOn(defaultStoreBackendRegistry, "create");
    const cfg = testConfig();

    const bundle = createStoreBundle(cfg, { dataDir: dir, logger: silentLogger });
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls.some((c) => c[0] === "sqlite")).toBe(true);
    bundle.store.close();

    spy.mockClear();
    const pool = new StorePool({
      mode: "sqlite",
      memoryCfg: cfg,
      dataDir: dir,
      logger: silentLogger,
    });
    pool.setGraceCloseDelay(0);
    const pooled = await pool.getStore("default", null);
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls.some((c) => c[0] === "sqlite")).toBe(true);
    expect(getSqlitePath(dir, "default")).toBe(path.join(dir, "vectors.db"));
    await pool.closeAll();
    void pooled;
  });

  it("fails if createStoreBundle bypasses the registry", () => {
    const empty = new StoreBackendRegistry();
    expect(() =>
      createStoreBundle(testConfig(), { dataDir: dir, logger: silentLogger, registry: empty }),
    ).toThrow(/Unknown store backend/);
  });

  it("fails if StorePool bypasses the registry", async () => {
    const empty = new StoreBackendRegistry();
    const pool = new StorePool({
      mode: "sqlite",
      memoryCfg: testConfig(),
      dataDir: dir,
      logger: silentLogger,
      registry: empty,
    });
    pool.setGraceCloseDelay(0);
    await expect(pool.getStore("default", null)).rejects.toThrow(/Unknown store backend/);
    await pool.closeAll();
  });

  it("mode===tcvdb && !vdbConfig silently falls back to sqlite (documented, not an error)", async () => {
    expect(resolvePooledStoreBackend("tcvdb", null)).toBe("sqlite");
    expect(resolvePooledStoreBackend("tcvdb", undefined)).toBe("sqlite");
    expect(resolvePooledStoreBackend("sqlite", null)).toBe("sqlite");
    expect(resolvePooledStoreBackend("tcvdb", {
      url: "http://vdb",
      user: "root",
      apiKey: "k",
      database: "db",
    })).toBe("tcvdb");
    expect(resolvePooledStoreBackend("postgres", null)).toBe("postgres");
    expect(resolvePooledStoreBackend("postgres", {
      url: "http://vdb",
      user: "root",
      apiKey: "k",
      database: "db",
    })).toBe("postgres");

    const spy = vi.spyOn(defaultStoreBackendRegistry, "create");
    const pool = new StorePool({
      mode: "tcvdb",
      memoryCfg: testConfig({ storeBackend: "tcvdb" }),
      dataDir: dir,
      logger: silentLogger,
    });
    pool.setGraceCloseDelay(0);
    const pooled = await pool.getStore("inst-1", null);
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls.some((c) => c[0] === "sqlite")).toBe(true);
    expect(spy.mock.calls.every((c) => c[0] !== "tcvdb")).toBe(true);
    // sqlite-only escape hatch — not a TCVDB store
    expect(typeof (pooled.store as { getRawDb?: () => unknown }).getRawDb).toBe("function");
    expect(pooled.store.getCapabilities().profiles).toBe(false);
    await pool.closeAll();
  });

  it("plugin createStoreBundle still fail-hard when tcvdb creds are missing (not a silent flip)", () => {
    expect(() =>
      createStoreBundle(testConfig({ storeBackend: "tcvdb" }), {
        dataDir: dir,
        logger: silentLogger,
      }),
    ).toThrow(/TCVDB backend requires tcvdb.url and tcvdb.apiKey/);
  });

  it("plugin createStoreBundle fail-hard when postgres is selected without DATABASE_URL", () => {
    const prev = process.env.DATABASE_URL;
    const prevHost = process.env.PGHOST;
    const prevDb = process.env.PGDATABASE;
    delete process.env.DATABASE_URL;
    delete process.env.PGHOST;
    delete process.env.PGDATABASE;
    try {
      expect(() =>
        createStoreBundle(testConfig({ storeBackend: "postgres" }), {
          dataDir: dir,
          logger: silentLogger,
        }),
      ).toThrow(/Postgres backend requires/);
    } finally {
      if (prev !== undefined) process.env.DATABASE_URL = prev;
      if (prevHost !== undefined) process.env.PGHOST = prevHost;
      if (prevDb !== undefined) process.env.PGDATABASE = prevDb;
    }
  });

  it("getSqlitePath keeps default vs per-instance layout", () => {
    expect(getSqlitePath("/data", "default")).toBe(path.join("/data", "vectors.db"));
    expect(getSqlitePath("/data", "inst-9")).toBe(path.join("/data", "instances", "inst-9", "vectors.db"));
  });
});
