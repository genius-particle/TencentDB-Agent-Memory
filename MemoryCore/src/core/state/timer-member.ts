import type { TaskPayload } from "./types.js";

export interface PipelineTimerMemberContext {
  teamId?: string;
  agentId?: string;
}

export interface ParsedPipelineTimerMember extends PipelineTimerMemberContext {
  sessionId: string;
  timerType: string;
  taskType: TaskPayload["type"];
  priority: number;
}

const SCOPED_TIMER_PREFIX = "scope:";

export { SCOPED_TIMER_PREFIX };

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function classifyTimerType(timerType: string): Pick<ParsedPipelineTimerMember, "taskType" | "priority"> {
  if (timerType.startsWith("L1")) return { taskType: "L1", priority: 0 };
  if (timerType.startsWith("L2")) return { taskType: "L2", priority: 1 };
  if (timerType.startsWith("L3")) return { taskType: "L3", priority: 2 };
  return { taskType: "flush", priority: 0 };
}

export function buildPipelineTimerMember(
  sessionId: string,
  timerType: string,
  ctx?: PipelineTimerMemberContext,
): string {
  if (ctx?.teamId && ctx?.agentId) {
    return `${SCOPED_TIMER_PREFIX}team:${encodeURIComponent(ctx.teamId)}|agent:${encodeURIComponent(ctx.agentId)}|session:${encodeURIComponent(sessionId)}:${timerType}`;
  }
  return `${sessionId}:${timerType}`;
}

export function parseProfileSessionTenant(sessionId: string): PipelineTimerMemberContext | undefined {
  const m = sessionId.match(/^profile:team:([^|]+)\|agent:([^|]+)(?:\|session:.+)?$/);
  if (!m) return undefined;
  return { teamId: m[1], agentId: m[2] };
}

export function parsePipelineTimerMember(member: string): ParsedPipelineTimerMember {
  if (member.startsWith(SCOPED_TIMER_PREFIX)) {
    const lastColon = member.lastIndexOf(":");
    if (lastColon > SCOPED_TIMER_PREFIX.length) {
      const timerType = member.slice(lastColon + 1);
      const scope = member.slice(SCOPED_TIMER_PREFIX.length, lastColon);
      const values: Record<string, string> = {};
      for (const part of scope.split("|")) {
        const idx = part.indexOf(":");
        if (idx > 0) values[part.slice(0, idx)] = part.slice(idx + 1);
      }
      const sessionId = values.session ? safeDecodeURIComponent(values.session) : "";
      if (sessionId) {
        const classified = classifyTimerType(timerType);
        return {
          sessionId,
          timerType,
          ...classified,
          ...(values.team ? { teamId: safeDecodeURIComponent(values.team) } : {}),
          ...(values.agent ? { agentId: safeDecodeURIComponent(values.agent) } : {}),
        };
      }
    }
  }

  const lastColon = member.lastIndexOf(":");
  if (lastColon <= 0) {
    return { sessionId: member, timerType: "L1_idle", taskType: "L1", priority: 0 };
  }

  const sessionId = member.slice(0, lastColon);
  const timerType = member.slice(lastColon + 1);
  const classified = classifyTimerType(timerType);
  const tenant = parseProfileSessionTenant(sessionId);
  return { sessionId, timerType, ...classified, ...tenant };
}

export interface ParsedTimerShardMember extends ParsedPipelineTimerMember {
  instanceId: string;
}

/**
 * Parse a timer shard member from Redis ZSET.
 * Format: `{instanceId}\x00{timerMember}` or legacy variants.
 *
 * Scoped pipeline timers (`scope:team:...|session:...:L2_schedule`) must NOT
 * be split on the first colon — that would treat `scope` as instanceId.
 */
export function parseTimerShardMember(
  member: string,
  defaultInstanceId = "default",
): ParsedTimerShardMember {
  const sep = member.indexOf("\x00");
  let instanceId: string;
  let rest: string;

  if (sep >= 0) {
    instanceId = member.slice(0, sep);
    rest = member.slice(sep + 1);
  } else if (member.startsWith(SCOPED_TIMER_PREFIX)) {
    instanceId = defaultInstanceId;
    rest = member;
  } else {
    const firstColon = member.indexOf(":");
    if (firstColon <= 0) {
      instanceId = defaultInstanceId;
      rest = member;
    } else {
      instanceId = member.slice(0, firstColon);
      rest = member.slice(firstColon + 1);
    }
  }

  if (rest.startsWith("offload-l15:")) {
    const afterPrefix = rest.slice("offload-l15:".length);
    const colonIdx = afterPrefix.indexOf(":");
    const sessionId = colonIdx > 0 ? afterPrefix.slice(colonIdx + 1) : afterPrefix;
    return { instanceId, sessionId, taskType: "offload-l15", priority: 0, timerType: rest };
  }
  if (rest.startsWith("offload-l2:")) {
    const afterPrefix = rest.slice("offload-l2:".length);
    const colonIdx = afterPrefix.indexOf(":");
    let sessionId = colonIdx > 0 ? afterPrefix.slice(colonIdx + 1) : afterPrefix;
    if (sessionId.endsWith(".mmd")) {
      const lastColon = sessionId.lastIndexOf(":");
      if (lastColon > 0) sessionId = sessionId.slice(0, lastColon);
    }
    return { instanceId, sessionId, taskType: "offload-l2", priority: 1, timerType: rest };
  }
  if (rest.startsWith("offload-l1:")) {
    const afterPrefix = rest.slice("offload-l1:".length);
    const colonIdx = afterPrefix.indexOf(":");
    const sessionId = colonIdx > 0 ? afterPrefix.slice(colonIdx + 1) : afterPrefix;
    return { instanceId, sessionId, taskType: "offload-l1", priority: 0, timerType: rest };
  }

  const parsed = parsePipelineTimerMember(rest);
  return { instanceId, ...parsed };
}
