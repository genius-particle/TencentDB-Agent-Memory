/**
 * Store Factory — creates the appropriate storage backend and embedding service
 * based on plugin configuration.
 *
 * Supports:
 * - "sqlite" (default): local SQLite + sqlite-vec + FTS5
 * - "tcvdb": Tencent Cloud VectorDB (server-side embedding + hybridSearch)
 *
 * Both backends ship with core and are resolved through StoreBackendRegistry
 * (same registry as Gateway StorePool). TCVDB is a vendor-provided store that
 * has always been part of the open-source surface of this plugin.
 */

import type { MemoryTdaiConfig } from "../../config.js";
import type { IMemoryStore, IEmbeddingService, StoreLogger } from "./types.js";
import { NoopEmbeddingService } from "./embedding.js";
import { createBM25Encoder } from "./bm25-local.js";
import type { BM25LocalEncoder } from "./bm25-local.js";
import {
  defaultStoreBackendRegistry,
  type StoreBackendRegistry,
} from "./registry.js";
import { ensureBuiltinStoreBackends } from "./backends.js";

// Re-export for convenience
export type { IMemoryStore, IEmbeddingService, StoreLogger, BM25LocalEncoder };

const TAG = "[memory-tdai][factory]";

export interface StoreBundle {
  store: IMemoryStore;
  embedding: IEmbeddingService;
  bm25Encoder?: BM25LocalEncoder;
  /** Snapshot of current store config for manifest writing. */
  storeSnapshot: import("../../utils/manifest.js").StoreConfigSnapshot;
}

export interface CreateStoreBundleOptions {
  dataDir: string;
  logger?: StoreLogger;
  /** Injected in tests. Production uses {@link defaultStoreBackendRegistry}. */
  registry?: StoreBackendRegistry;
}

/**
 * Create the storage backend, embedding service, and optional BM25 encoder
 * based on plugin configuration.
 *
 * @param config       Fully resolved plugin config.
 * @param options.dataDir    Plugin data directory.
 * @param options.logger     Logger instance.
 */
export function createStoreBundle(
  config: MemoryTdaiConfig,
  options: CreateStoreBundleOptions,
): StoreBundle {
  const { logger } = options;
  const registry = options.registry ?? ensureBuiltinStoreBackends(defaultStoreBackendRegistry);

  // ── BM25 local encoder ──
  const bm25Encoder = createBM25Encoder(config.bm25, logger);

  if (config.storeBackend === "tcvdb") {
    const tcvdbCfg = config.tcvdb;
    if (!tcvdbCfg.url || !tcvdbCfg.apiKey) {
      throw new Error(`${TAG} TCVDB backend requires tcvdb.url and tcvdb.apiKey`);
    }
    if (!tcvdbCfg.database) {
      throw new Error(`${TAG} TCVDB backend requires tcvdb.database — please set a unique database name in your openclaw.json plugin config`);
    }

    const created = registry.create("tcvdb", {
      memoryCfg: config,
      dataDir: options.dataDir,
      logger,
      bm25Encoder,
    });

    return {
      store: created.store,
      embedding: (created.embedding ?? new NoopEmbeddingService()) as IEmbeddingService,
      bm25Encoder: created.bm25Encoder ?? bm25Encoder,
      storeSnapshot: created.storeSnapshot,
    };
  }

  const created = registry.create("sqlite", {
    memoryCfg: config,
    dataDir: options.dataDir,
    instanceId: "default",
    logger,
    bm25Encoder,
  });

  return {
    store: created.store,
    embedding: created.embedding as unknown as IEmbeddingService,
    bm25Encoder: created.bm25Encoder ?? bm25Encoder,
    storeSnapshot: created.storeSnapshot,
  };
}
