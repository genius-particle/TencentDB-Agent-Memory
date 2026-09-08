import { describe, expect, it } from "vitest";
import { parseTimerShardMember } from "./timer-member.js";

describe("parseTimerShardMember", () => {
  it("parses null-separated instance prefix", () => {
    const parsed = parseTimerShardMember("mem-inst\x00sess-1:L1_idle");
    expect(parsed.instanceId).toBe("mem-inst");
    expect(parsed.sessionId).toBe("sess-1");
    expect(parsed.taskType).toBe("L1");
  });

  it("does not treat scoped timer prefix as instanceId", () => {
    const member =
      "scope:team:team-1|agent:agt-1|session:sess-demo:L2_schedule";
    const parsed = parseTimerShardMember(member);
    expect(parsed.instanceId).toBe("default");
    expect(parsed.instanceId).not.toBe("scope");
    expect(parsed.sessionId).toBe("sess-demo");
    expect(parsed.teamId).toBe("team-1");
    expect(parsed.agentId).toBe("agt-1");
    expect(parsed.taskType).toBe("L2");
  });

  it("preserves explicit instanceId for scoped timers with shard prefix", () => {
    const member =
      "mem-custom\x00scope:team:team-1|agent:agt-1|session:sess-demo:L2_schedule";
    const parsed = parseTimerShardMember(member);
    expect(parsed.instanceId).toBe("mem-custom");
    expect(parsed.sessionId).toBe("sess-demo");
    expect(parsed.taskType).toBe("L2");
  });
});
