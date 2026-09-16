import { describe, expect, it } from "@effect/vitest";

import { buildPiRpcSpawnInput, splitJsonLines } from "./PiRpcSupport.ts";

describe("splitJsonLines", () => {
  it("splits on LF only", () => {
    expect(splitJsonLines('{"a":1}\n{"b":2}\n')).toEqual({
      lines: ['{"a":1}', '{"b":2}'],
      rest: "",
    });
  });

  it("strips one trailing CR (CRLF input)", () => {
    expect(splitJsonLines('{"a":1}\r\n')).toEqual({ lines: ['{"a":1}'], rest: "" });
  });

  it("does not split on U+2028/U+2029 inside JSON strings", () => {
    const sep28 = String.fromCharCode(0x2028);
    const sep29 = String.fromCharCode(0x2029);
    const line = `{"text":"a${sep28}b${sep29}c"}`;
    expect(splitJsonLines(`${line}\n`)).toEqual({ lines: [line], rest: "" });
  });

  it("keeps a partial line in rest", () => {
    expect(splitJsonLines('{"a":1}\n{"par')).toEqual({ lines: ['{"a":1}'], rest: '{"par' });
  });

  it("passes through empty lines for the caller to skip", () => {
    expect(splitJsonLines("\n\n")).toEqual({ lines: ["", ""], rest: "" });
  });
});

describe("buildPiRpcSpawnInput", () => {
  it("defaults to the pi binary in rpc mode with approval disabled", () => {
    expect(buildPiRpcSpawnInput(undefined, "/tmp/project")).toEqual({
      command: "pi",
      args: ["--mode", "rpc", "--no-approve"],
      cwd: "/tmp/project",
      env: {},
    });
  });

  it("pins provider/model and honors binaryPath/sessionDir overrides", () => {
    expect(
      buildPiRpcSpawnInput(
        { binaryPath: "/opt/bin/pi.cmd", sessionDir: "/tmp/pi-sessions" },
        "/tmp/project",
        { OPENROUTER_API_KEY: "secret" },
        { provider: "openrouter", model: "inclusionai/ling-3.0-flash-vl:free" },
      ),
    ).toEqual({
      command: "/opt/bin/pi.cmd",
      args: [
        "--mode",
        "rpc",
        "--no-approve",
        "--provider",
        "openrouter",
        "--model",
        "inclusionai/ling-3.0-flash-vl:free",
        "--session-dir",
        "/tmp/pi-sessions",
      ],
      cwd: "/tmp/project",
      env: { OPENROUTER_API_KEY: "secret" },
    });
  });

  it("trims blank overrides back to defaults", () => {
    expect(buildPiRpcSpawnInput({ binaryPath: "  ", sessionDir: "  " }, "/tmp/project")).toEqual({
      command: "pi",
      args: ["--mode", "rpc", "--no-approve"],
      cwd: "/tmp/project",
      env: {},
    });
  });
});
