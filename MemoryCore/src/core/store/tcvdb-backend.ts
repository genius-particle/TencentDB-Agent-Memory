/**
 * TCVDB store backend factory — moved as-is from StorePool.createTcvdbStore
 * and plugin createStoreBundle. Behavior unchanged this phase.
 *
 * Parameter sources (preserved):
 *   - StorePool: per-instance VdbConfig + memoryCfg.tcvdb for embedding/timeout;
 *     HTTPS CA from VDB_CA_PEM_PATH.
 *   - Plugin createStoreBundle: memoryCfg.tcvdb (caller validates url/apiKey/database).
 */

import { TcvdbMemoryStore } from "./tcvdb.js";
import { NoopEmbeddingService } from "./embedding.js";
import type { StoreBackendFactory, StoreBackendCreateContext, CreatedStoreBackend } from "./registry.js";

export const tcvdbBackendFactory: StoreBackendFactory = {
  id: "tcvdb",
  create(ctx: StoreBackendCreateContext): CreatedStoreBackend {
    const tcvdbCfg = ctx.memoryCfg.tcvdb;
    const vdb = ctx.vdbConfig;

    const url = vdb?.url ?? tcvdbCfg.url;
    const username = vdb?.user ?? tcvdbCfg.username;
    const apiKey = vdb?.apiKey ?? tcvdbCfg.apiKey;
    const database = vdb?.database ?? tcvdbCfg.database;

    // StorePool path: HTTPS public endpoint needs a CA; plugin path uses config.
    const caPemPath = vdb
      ? (url.startsWith("https://") ? (process.env.VDB_CA_PEM_PATH || undefined) : undefined)
      : tcvdbCfg.caPemPath;

    const store = new TcvdbMemoryStore({
      url,
      username,
      apiKey,
      database,
      embeddingEnabled: tcvdbCfg?.embeddingEnabled,
      embeddingModel: tcvdbCfg?.embeddingModel ?? "bge-large-zh",
      timeout: tcvdbCfg?.timeout ?? 10000,
      caPemPath,
      logger: ctx.logger,
      bm25Encoder: ctx.bm25Encoder ?? undefined,
    });

    ctx.logger?.debug?.(
      `[memory-tdai][factory] Store created: backend=tcvdb, database=${database}, ` +
      `embedding=${tcvdbCfg?.embeddingEnabled ? `enabled(${tcvdbCfg.embeddingModel})` : "disabled"}, ` +
      `bm25=${ctx.bm25Encoder ? "enabled" : "disabled"}`,
    );

    return {
      store,
      embedding: new NoopEmbeddingService(),
      bm25Encoder: ctx.bm25Encoder,
      storeSnapshot: {
        type: "tcvdb",
        tcvdbUrl: url,
        tcvdbDatabase: database,
        tcvdbAlias: tcvdbCfg?.alias || undefined,
      },
    };
  },
};
