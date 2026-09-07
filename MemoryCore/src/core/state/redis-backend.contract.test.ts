import { afterEach, beforeEach, describe, expect, it } from "vitest";
import Redis from "ioredis";
import { createStateBackend } from "./index.js";
import { RedisStateBackend } from "./redis-backend.js";
import type { TaskPayload } from "./types.js";
import { buildPipelineTimerMember } from "./timer-member.js";

function hasRedisEnv(): boolean {
  return Boolean(process.env.REDIS_URL || process.env.REDIS_HOST);
}

function redisOpts() {
  if (process.env.REDIS_URL) {
    return { url: process.env.REDIS_URL };
  }
  return {
    host: process.env.REDIS_HOST ?? "127.0.0.1",
    port: Number(process.env.REDIS_PORT ?? 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: Number(process.env.REDIS_DB ?? 15),
  };
}

describe.skipIf(!hasRedisEnv())("RedisStateBackend contract", () => {
  let client: Redis;
  let backend: RedisStateBackend;
  const prefix = `tdai_test_${process.pid}_${Date.now()}`;

  beforeEach(async () => {
    const opts = redisOpts();
    client = opts.url ? new Redis(opts.url) : new Redis(opts);
    await client.flushdb();
    backend = new RedisStateBackend({ client, keyPrefix: prefix });
    await backend.initialize();
  });

  afterEach(async () => {
    await backend.destroy?.();
    await client.flushdb();
    await client.quit();
  });

  it("factory loads public RedisStateBackend with getClient()", async () => {
    const opts = redisOpts();
    const created = await createStateBackend({
      type: "redis",
      redis: { ...opts, keyPrefix: `${prefix}_factory` },
    });
    expect(created).toBeInstanceOf(RedisStateBackend);
    expect(typeof (created as RedisStateBackend).getClient).toBe("function");
    await created.destroy?.();
  });

  it("acquireLock is exclusive until release", async () => {
    const ok1 = await backend.acquireLock("pipeline:{inst1}", "w1", 5000);
    const ok2 = await backend.acquireLock("pipeline:{inst1}", "w2", 5000);
    expect(ok1).toBe(true);
    expect(ok2).toBe(false);
    await backend.releaseLock("pipeline:{inst1}", "w1");
    const ok3 = await backend.acquireLock("pipeline:{inst1}", "w2", 5000);
    expect(ok3).toBe(true);
  });

  it("captureAtomic triggers enqueue at threshold", async () => {
    const task: TaskPayload = {
      id: "t1",
      type: "L1",
      instanceId: "inst1",
      sessionId: "sess1",
      priority: 0,
      createdAt: Date.now(),
    };
    const timerMember = buildPipelineTimerMember("sess1", "L1_idle", { teamId: "t1", agentId: "a1" });
    const now = Date.now();

    const r1 = await backend.captureAtomic({
      instanceId: "inst1",
      sessionId: "sess1",
      teamId: "t1",
      agentId: "a1",
      threshold: 2,
      fireAtMs: now + 60_000,
      timerMember,
      taskPayload: task,
      nowMs: now,
      rounds: 1,
    });
    expect(r1.triggered).toBe(false);
    expect(r1.conversationCount).toBe(1);

    const r2 = await backend.captureAtomic({
      instanceId: "inst1",
      sessionId: "sess1",
      teamId: "t1",
      agentId: "a1",
      threshold: 2,
      fireAtMs: now + 60_000,
      timerMember,
      taskPayload: task,
      nowMs: now + 1,
      rounds: 1,
    });
    expect(r2.triggered).toBe(true);
    expect(r2.conversationCount).toBe(0);

    const queued = await backend.listQueuedTasks();
    expect(queued.some((t) => t.id === "t1")).toBe(true);
  });

  it("claimExpiredFromShard does not double-claim timers", async () => {
    const member = buildPipelineTimerMember("sess2", "L1_idle");
    const now = Date.now();
    await backend.setTimer("inst2", member, now - 1000);

    let firstTotal = 0;
    let secondTotal = 0;
    for (let s = 0; s < backend.timerShardCount; s++) {
      const shardKey = backend.getTimerShardKeyByIndex(s);
      firstTotal += (await backend.claimExpiredFromShard(shardKey, now, 10)).length;
    }
    for (let s = 0; s < backend.timerShardCount; s++) {
      const shardKey = backend.getTimerShardKeyByIndex(s);
      secondTotal += (await backend.claimExpiredFromShard(shardKey, now, 10)).length;
    }
    expect(firstTotal).toBe(1);
    expect(secondTotal).toBe(0);
  });

  it("enqueue/consume/ack round-trip attaches _msgId", async () => {
    const task: TaskPayload = {
      id: "consume-1",
      type: "L1",
      instanceId: "inst3",
      sessionId: "sess3",
      priority: 0,
      createdAt: Date.now(),
    };
    await backend.enqueueTask(task);
    const consumed = await backend.consumeTask("worker-a", 1000) as TaskPayload & { _msgId?: string };
    expect(consumed?.id).toBe("consume-1");
    expect(consumed?._msgId).toBeTruthy();
    if (consumed?._msgId) await backend.ackTask(consumed._msgId);
  });

  it("claimStaleTasks reclaims unacked pending messages", async () => {
    const task: TaskPayload = {
      id: "stale-1",
      type: "L1",
      instanceId: "inst4",
      sessionId: "sess4",
      priority: 0,
      createdAt: Date.now(),
    };
    await backend.enqueueTask(task);
    const first = await backend.consumeTask("worker-dead", 500) as TaskPayload & { _msgId?: string };
    expect(first?._msgId).toBeTruthy();
    // Do not ACK — simulate dead worker
    await new Promise((r) => setTimeout(r, 50));
    const reclaimed = await backend.claimStaleTasks("worker-live", 1, 5);
    expect(reclaimed.some((t) => t.id === "stale-1")).toBe(true);
    for (const t of reclaimed) {
      const msgId = (t as TaskPayload & { _msgId?: string })._msgId;
      if (msgId) await backend.ackTask(msgId);
    }
  });
});
