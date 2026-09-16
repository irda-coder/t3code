import { describe, expect, it } from "@effect/vitest";

import {
  compactArgsText,
  extractPiResultText,
  piToolItemExtra,
  tailText,
  toolCommandOf,
} from "./PiAdapter.ts";

describe("extractPiResultText", () => {
  it("joins text blocks", () => {
    expect(
      extractPiResultText([
        { type: "text", text: "total 48" },
        { type: "text", text: "file.txt" },
      ]),
    ).toBe("total 48\nfile.txt");
  });

  it("skips non-text blocks and non-arrays", () => {
    expect(extractPiResultText([{ type: "image", data: "x" }, null, "s"])).toBe("");
    expect(extractPiResultText(undefined)).toBe("");
    expect(extractPiResultText("nope")).toBe("");
  });
});

describe("tailText", () => {
  it("passes short text through", () => {
    expect(tailText("  abc  ")).toBe("abc");
  });

  it("keeps the tail with a marker", () => {
    const out = tailText("x".repeat(100), 10);
    expect(out).toBe(`…[truncated]${"x".repeat(10)}`);
  });
});

describe("toolCommandOf", () => {
  it("reads the command for bash-like tools", () => {
    expect(toolCommandOf("bash", { command: "ls -la" })).toBe("ls -la");
    expect(toolCommandOf("Shell", { command: "pwd" })).toBe("pwd");
  });

  it("returns undefined otherwise", () => {
    expect(toolCommandOf("read", { path: "a.txt" })).toBeUndefined();
    expect(toolCommandOf("bash", {})).toBeUndefined();
    expect(toolCommandOf("bash", { command: "   " })).toBeUndefined();
  });
});

describe("compactArgsText", () => {
  it("returns undefined for empty args", () => {
    expect(compactArgsText({})).toBeUndefined();
  });

  it("renders args as JSON", () => {
    expect(compactArgsText({ path: "a.txt" })).toBe('{"path":"a.txt"}');
  });
});

describe("piToolItemExtra", () => {
  const start = {
    type: "tool_execution_start",
    toolCallId: "call_1",
    toolName: "bash",
    args: { command: "ls -la" },
  };

  it("maps a bash start to command detail + data", () => {
    expect(piToolItemExtra("bash", start)).toEqual({
      detail: "ls -la",
      data: {
        toolCallId: "call_1",
        kind: "bash",
        command: "ls -la",
        rawInput: { command: "ls -la" },
      },
    });
  });

  it("prefers accumulated partial text on updates", () => {
    const extra = piToolItemExtra(
      "bash",
      {
        ...start,
        type: "tool_execution_update",
        partialResult: { content: [{ type: "text", text: "total 48\n" }] },
      },
      "total 48\n",
    );
    expect(extra.detail).toBe("total 48");
    expect(extra.data).toMatchObject({
      toolCallId: "call_1",
      command: "ls -la",
      content: "total 48",
    });
  });

  it("carries result content on end", () => {
    const extra = piToolItemExtra(
      "bash",
      {
        ...start,
        type: "tool_execution_end",
        result: { content: [{ type: "text", text: "total 48\nfile.txt" }] },
        isError: false,
      },
      "total 48\nfile.txt",
    );
    expect(extra.detail).toBe("total 48\nfile.txt");
    expect(extra.data).toMatchObject({ content: "total 48\nfile.txt" });
    const rawOutput = extra.data?.["rawOutput"] as { content?: unknown };
    expect(Array.isArray(rawOutput?.content)).toBe(true);
  });

  it("falls back to args JSON for non-bash tools", () => {
    const extra = piToolItemExtra("read", {
      type: "tool_execution_start",
      toolCallId: "call_2",
      toolName: "read",
      args: { path: "a.txt" },
    });
    expect(extra.detail).toBe('{"path":"a.txt"}');
    expect(extra.data).toMatchObject({ kind: "read", rawInput: { path: "a.txt" } });
  });
});
