/**
 * Postgres + pgvector store backend factory.
 *
 * Selected via STORE_MODE=postgres / storeBackend=postgres.
 * Does not change the documented tcvdb && !vdbConfig → sqlite silent fallback.
 */

import { PostgresMemoryStore } from "./postgres.js";
import { createEmbeddingService } from "./embedding.js";
import type { EmbeddingService } from "./embedding.js";
import type { StoreBackendFactory, StoreBackendCreateContext, CreatedStoreBackend } from "./registry.js";
import type { StoreLogger } from "./types.js";
import {
  postgresSchemaForInstance,
  resolvePostgresConnection,
} from "./postgres-env.js";

const TAG = "[memory-tdai][factory]";

function createPostgresEmbedding(
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

export const postgresBackendFactory: StoreBackendFactory = {
  id: "postgres",
  create(ctx: StoreBackendCreateContext): CreatedStoreBackend {
    const connectionString =
      ctx.postgresUrl ??
      ctx.memoryCfg.postgres?.url ??
      resolvePostgresConnection();
    if (!connectionString) {
      throw new Error(
        `${TAG} Postgres backend requires DATABASE_URL or PGHOST/PGDATABASE (or memory.postgres.url)`,
      );
    }

    const instanceId = ctx.instanceId ?? "default";
    const schema = ctx.postgresSchema ?? postgresSchemaForInstance(instanceId);
    const dims = ctx.memoryCfg.embedding.dimensions ?? 0;
    const store = new PostgresMemoryStore({
      connectionString,
      schema,
      dimensions: dims,
      logger: ctx.logger,
      bm25Encoder: ctx.bm25Encoder,
    });
    const embedding = createPostgresEmbedding(ctx, ctx.logger);

    ctx.logger?.debug?.(
      `${TAG} Store created: backend=postgres, schema=${schema}, dimensions=${dims}, ` +
      `embedding=${embedding ? "enabled" : "disabled"}, ` +
      `bm25=${ctx.bm25Encoder ? "enabled" : "disabled"}`,
    );

    return {
      store,
      embedding,
      bm25Encoder: ctx.bm25Encoder,
      storeSnapshot: {
        type: "postgres",
        postgresSchema: schema,
      },
    };
  },
};
