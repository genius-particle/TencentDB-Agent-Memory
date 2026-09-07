/**
 * State Backend — 接口 + 默认实现导出 + 后端工厂。
 *
 * 默认实现 (LocalStateBackend) 跟接口同住 core，自带可用。
 * Redis 二开实现在 ./redis-backend.ts；私有 submodule 在 integrations/redis/。
 */

export type {
  IStateBackend,
  PipelineSessionState,
  TimerEntry,
  TaskPayload,
  CaptureAtomicParams,
  CaptureAtomicResult,
} from "./types.js";
export { DEFAULT_PIPELINE_STATE } from "./types.js";

export { LocalStateBackend } from "./local-backend.js";
export { RedisStateBackend } from "./redis-backend.js";

import type { IStateBackend, TimerEntry } from "./types.js";
import { LocalStateBackend } from "./local-backend.js";
import { RedisStateBackend } from "./redis-backend.js";

export interface StateBackendConfig {
  type: "local" | "redis";
  local?: {
    onTimerExpired?: (entry: TimerEntry) => void;
  };
  redis?: {
    /** backend connection URL */
    url?: string;
    host?: string;
    port?: number;
    password?: string;
    /** database index (default: 0) */
    db?: number;
    keyPrefix?: string;
    consumerGroup?: string;
  };
}

async function createRedisClient(redisCfg: NonNullable<StateBackendConfig["redis"]>) {
  const { default: Redis } = await import("ioredis");
  if (redisCfg.url) {
    return new Redis(redisCfg.url);
  }
  return new Redis({
    host: redisCfg.host ?? "127.0.0.1",
    port: redisCfg.port ?? 6379,
    password: redisCfg.password,
    db: redisCfg.db ?? 0,
  });
}

/**
 * 工厂函数：根据配置创建对应的 State Backend。
 *
 * - type === "local": 内置 LocalStateBackend，零外部依赖
 * - type === "redis": 公开 RedisStateBackend（本 fork）；私有 submodule 可覆盖
 */
export async function createStateBackend(config: StateBackendConfig): Promise<IStateBackend> {
  if (config.type === "redis") {
    const redisCfg = config.redis;
    if (!redisCfg) throw new Error("redis config is required when state_backend=redis");

    const client = await createRedisClient(redisCfg);

    // Prefer public fork implementation; fall back to private submodule if present.
    let BackendCtor: typeof RedisStateBackend = RedisStateBackend;
    try {
      // Private submodule optional — absent on this fork.
      const privateMod = await import("../../integrations/redis/index.js" as string);
      if (privateMod?.RedisStateBackend) {
        BackendCtor = privateMod.RedisStateBackend as typeof RedisStateBackend;
      }
    } catch {
      // Public fork path — expected on this repository.
    }

    const backend = new BackendCtor({
      client: client as never,
      keyPrefix: redisCfg.keyPrefix,
      consumerGroup: redisCfg.consumerGroup,
    });
    await backend.initialize();
    return backend;
  }

  const backend = new LocalStateBackend(config.local);
  await backend.initialize?.();
  return backend;
}
