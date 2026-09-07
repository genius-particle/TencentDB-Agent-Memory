import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { parseConfig } from "../../config.js";
import { createBM25Encoder } from "./bm25-local.js";
import { hasPostgresEnv, resolvePostgresConnection } from "./postgres-env.js";
import { PostgresSkillStore } from "./postgres-skill-store.js";
import type { ISkillStore } from "../skill/skill-store.interface.js";

const hasPostgres = hasPostgresEnv();

describe.skipIf(!hasPostgres)("ISkillStore contract: postgres", () => {
  let store: ISkillStore;
  let schema: string;

  beforeEach(async () => {
    schema = `mem_skill_${process.pid}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    const cfg = parseConfig({
      storeBackend: "postgres",
      bm25: { enabled: true, language: "en" },
      embedding: { provider: "none" },
    });
    store = new PostgresSkillStore({
      connectionString: resolvePostgresConnection()!,
      schema,
      dimensions: 0,
      bm25Encoder: createBM25Encoder(cfg.bm25),
    });
    store.init();
    await (store as PostgresSkillStore).ensureReady();
    expect(store.isDegraded()).toBe(false);
  });

  afterEach(() => {
    store?.close();
  });

  it("appendVersion + getHead + listSkills", async () => {
    const skillId = `skill-${randomUUID()}`;
    const created = await store.appendVersion({
      skill_id: skillId,
      team_id: "team-1",
      owner_agent_id: "agent-1",
      user_id: "user-1",
      name: "contract-skill",
      description: "desc",
      content: "# Contract Skill\nDo things.",
      content_hash: "hash-1",
      manifest: [],
      storage_dir: `skills/${skillId}`,
    });
    expect(created.skill_id).toBe(skillId);
    expect(created.version).toBe(1);

    const head = await store.getHead(skillId, "team-1");
    expect(head?.name).toBe("contract-skill");

    const listed = await store.listSkills({ team_id: "team-1" });
    expect(listed.items.some((s) => s.skill_id === skillId)).toBe(true);
  });

  it("searchSkills lexical path", async () => {
    const skillId = `skill-${randomUUID()}`;
    await store.appendVersion({
      skill_id: skillId,
      team_id: "team-2",
      owner_agent_id: "agent-2",
      user_id: "user-2",
      name: "searchable-skill",
      description: "alpha beta",
      content: "gamma delta contract token",
      content_hash: "hash-2",
      manifest: [],
      storage_dir: `skills/${skillId}`,
    });
    const hits = await store.searchSkills({
      query: "contract",
      team_id: "team-2",
      topK: 5,
    });
    expect(hits.some((h) => h.skill.skill_id === skillId)).toBe(true);
  });

  it("advertises honest postgres skill capabilities", () => {
    const caps = store.getCapabilities();
    expect(caps.ftsSearch).toBe(true);
    expect(caps.vectorSearch).toBe(false);
    expect(caps.sparseVectors).toBe(true);
  });
});
