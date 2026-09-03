/**
 * StoreBackendRegistry — single resolution path for IMemoryStore backends.
 *
 * Gateway StorePool and plugin createStoreBundle MUST construct stores through
 * this registry. Tests fail if either call site bypasses it.
 *
 * Built-in ids: "sqlite" | "tcvdb" | "postgres". Open-source backends are
 * first-class registrations; TCVDB remains a first-class vendor backend,
 * not a silent default flip. Selecting postgres does not change live
 * deployMode=service defaults (still tcvdb unless STORE_MODE=postgres).
 */

import type { MemoryTdaiConfig } from "../../config.js";
import type { IEmbeddingService, IMemoryStore, StoreLogger } from "./types.js";
import type { BM25LocalEncoder } from "./bm25-local.js";
import type { VdbConfig } from "../instance-config-provider.js";
import type { StoreConfigSnapshot } from "../../utils/manifest.js";

export type StoreBackendId = "sqlite" | "tcvdb" | "postgres";

export type StoreMode = StoreBackendId;

export interface StoreBackendCreateContext {
  memoryCfg: MemoryTdaiConfig;
  logger?: StoreLogger;
  /** SQLite data directory (plugin dataDir / gateway data.baseDir). */
  dataDir: string;
  /**
   * Instance id for per-instance sqlite paths.
   * Plugin `createStoreBundle` uses `"default"` → `{dataDir}/vectors.db`.
   */
  instanceId?: string;
  /** Override sqlite file path. If omitted, derived via getSqlitePath. */
  dbPath?: string;
  /**
   * Per-instance TCVDB config (StorePool / service mode).
   * Plugin path omits this and reads `memoryCfg.tcvdb`.
   */
  vdbConfig?: VdbConfig | null;
  /** Shared BM25 encoder (StorePool). If omitted, factory may create from config. */
  bm25Encoder?: BM25LocalEncoder;
  /** Postgres connection string (DATABASE_URL / memory.postgres.url). */
  postgresUrl?: string;
  /** Postgres schema override. Default: mem_{instanceId}. */
  postgresSchema?: string;
}

export interface CreatedStoreBackend {
  store: IMemoryStore;
  embedding?: IEmbeddingService;
  bm25Encoder?: BM25LocalEncoder;
  storeSnapshot: StoreConfigSnapshot;
}

export interface StoreBackendFactory {
  id: StoreBackendId;
  create(ctx: StoreBackendCreateContext): CreatedStoreBackend;
}

/**
 * Resolve which backend StorePool should construct.
 *
 * Documented silent fallback (do NOT turn into an error this phase):
 *   mode === "tcvdb" && !vdbConfig  →  "sqlite"
 *
 * postgres is explicit — never a silent fallback from tcvdb/sqlite.
 */
export function resolvePooledStoreBackend(
  mode: StoreMode,
  vdbConfig: VdbConfig | null | undefined,
): StoreBackendId {
  if (mode === "postgres") return "postgres";
  return mode === "tcvdb" && vdbConfig ? "tcvdb" : "sqlite";
}

export class StoreBackendRegistry {
  private readonly factories = new Map<string, StoreBackendFactory>();

  register(factory: StoreBackendFactory): void {
    this.factories.set(factory.id, factory);
  }

  has(id: string): boolean {
    return this.factories.has(id);
  }

  get(id: string): StoreBackendFactory | undefined {
    return this.factories.get(id);
  }

  list(): StoreBackendId[] {
    return [...this.factories.keys()] as StoreBackendId[];
  }

  create(id: string, ctx: StoreBackendCreateContext): CreatedStoreBackend {
    const factory = this.factories.get(id);
    if (!factory) {
      const registered = [...this.factories.keys()].join(", ") || "(none)";
      throw new Error(
        `[store-registry] Unknown store backend: ${id}. Registered: ${registered}`,
      );
    }
    return factory.create(ctx);
  }
}

/** Process-wide registry used by StorePool and createStoreBundle. */
export const defaultStoreBackendRegistry = new StoreBackendRegistry();
