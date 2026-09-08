import { describe, expect, it, vi } from "vitest";
import type { TdaiClient } from "../client.js";
import type { TdaiIdentity, TdaiMessage } from "../types.js";
import { recordTdaiTurn, recordTdaiTurnIfFinal } from "../recorder.js";

const identity: TdaiIdentity = {
  teamId: "team-1",
  userId: "user-1",
  agentId: "agent-1",
  sessionId: "sess-1",
  taskId: "task-1",
};

const userMessage: TdaiMessage = { role: "user", content: "hello" };

function mockClient(): TdaiClient {
  return {
    addConversation: vi.fn().mockResolvedValue(undefined),
  } as unknown as TdaiClient;
}

describe("recordTdaiTurnIfFinal", () => {
  it("skips intermediate tool-loop rounds when toolCallCountOverride > 0", async () => {
    const client = mockClient();
    await recordTdaiTurnIfFinal(client, identity, userMessage, null, {
      toolCallCountOverride: 2,
    });
    expect(client.addConversation).not.toHaveBeenCalled();
  });

  it("writes once on final round (toolCallCountOverride === 0)", async () => {
    const client = mockClient();
    await recordTdaiTurnIfFinal(client, identity, userMessage, "done", {
      toolCallCountOverride: 0,
    });
    expect(client.addConversation).toHaveBeenCalledOnce();
    expect(client.addConversation).toHaveBeenCalledWith(identity, [
      userMessage,
      { role: "assistant", content: "done" },
    ]);
  });

  it("skips non-stream assistant with tool_calls", async () => {
    const client = mockClient();
    await recordTdaiTurnIfFinal(client, identity, userMessage, "calling tool", {
      assistantMessage: {
        role: "assistant",
        content: "calling tool",
        tool_calls: [{ id: "tc-1", type: "function", function: { name: "read", arguments: "{}" } }],
      },
    });
    expect(client.addConversation).not.toHaveBeenCalled();
  });

  it("writes user-only on final round with empty assistant text", async () => {
    const client = mockClient();
    await recordTdaiTurnIfFinal(client, identity, userMessage, "", {
      toolCallCountOverride: 0,
    });
    expect(client.addConversation).toHaveBeenCalledOnce();
    expect(client.addConversation).toHaveBeenCalledWith(identity, [userMessage]);
  });

  it("writes non-stream final assistant without tool_calls", async () => {
    const client = mockClient();
    await recordTdaiTurnIfFinal(client, identity, userMessage, "final answer", {
      assistantMessage: { role: "assistant", content: "final answer" },
    });
    expect(client.addConversation).toHaveBeenCalledOnce();
  });
});

describe("recordTdaiTurn", () => {
  it("always writes when called directly (mem-command path)", async () => {
    const client = mockClient();
    await recordTdaiTurn(client, identity, userMessage, "mem result");
    expect(client.addConversation).toHaveBeenCalledOnce();
  });
});
