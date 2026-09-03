/**
 * SQLite store backend factory — extracted from StorePool.createSqliteStore /
 * getSqlitePath so Gateway StorePool and plugin createStoreBundle share one path.
 *
 * Skill tables stay same-DB: the returned VectorStore still exposes getRawDb().
 * Do NOT merge MetadataStorePool into this registry.
 */

import path from "node:path";
import { existsSync, mkdirSync } from "node:fs";
import { VectorStore } from "./sqlite.js";
import { createEmbeddingService } from "./embedding.js";
import type { EmbeddingService } from "./embedding.js";
import type { StoreBackendFactory, StoreBackendCreateContext, CreatedStoreBackend } from "./registry.js";
import type { StoreLogger } from "./types.js";

/**
 * SQLite file path:
 *   - "default" → dataDir/vectors.db (plugin + standalone default instance)
 *   - other instanceId → dataDir/instances/{instanceId}/vectors.db
 */
export function getSqlitePath(dataDir: string, instanceId = "default"): string {
  if (instanceId === "default") {
    return path.join(dataDir, "vectors.db");
  }
  return path.join(dataDir, "instances", instanceId, "vectors.db");
}

function createSqliteEmbedding(
  ctx: StoreBackendCreateContext,
  logger?: StoreLogger,
): EmbeddingService | undefined {
  const embCfg = ctx.memoryCfg.embedding;
  if (
    embCfg.enabled &&
    embCfg.provider !== "local" &&
    embCfg.provider !== "none" &&
    embCfg.apiKey
  ) {
    return createEmbeddingService({
      provider: embCfg.provider,
      baseUrl: embCfg.baseUrl,
      apiKey: embCfg.apiKey,
      model: embCfg.model,
      dimensions: embCfg.dimensions,
      sendDimensions: embCfg.sendDimensions,
      maxInputChars: embCfg.maxInputChars,
    }, logger);
  }
  return undefined;
}

export const sqliteBackendFactory: StoreBackendFactory = {
  id: "sqlite",
  create(ctx: StoreBackendCreateContext): CreatedStoreBackend {
    const instanceId = ctx.instanceId ?? "default";
    const dbPath = ctx.dbPath ?? getSqlitePath(ctx.dataDir, instanceId);
    const dbDir = path.dirname(dbPath);
    if (!existsSync(dbDir)) {
      mkdirSync(dbDir, { recursive: true });
    }

    const dims = ctx.memoryCfg.embedding.dimensions ?? 0;
    const store = new VectorStore(dbPath, dims, ctx.logger);
    const embedding = createSqliteEmbedding(ctx, ctx.logger);

    ctx.logger?.debug?.(
      `[memory-tdai][factory] Store created: backend=sqlite, dbPath=${dbPath}, dimensions=${dims}, ` +
      `embedding=${embedding ? "enabled" : "disabled"}, ` +
      `bm25=${ctx.bm25Encoder ? "enabled" : "disabled"}`,
    );

    return {
      store,
      embedding,
      bm25Encoder: ctx.bm25Encoder,
      storeSnapshot: {
        type: "sqlite",
        sqlitePath: path.relative(ctx.dataDir, dbPath),
      },
    };
  },
};
