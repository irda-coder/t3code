/**
 * Transport tests against a fake pi: a node one-liner speaking the JSONL
 * RPC shape (responses by id, a hello turn, one malformed line). No network,
 * no model, no real binary.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { makePiRpcRuntime } from "./PiRpcSupport.ts";

const FAKE_SCRIPT = [
  "import { createInterface } from 'node:readline';",
  "const rl = createInterface({ input: process.stdin });",
  "rl.on('line', (line) => {",
  "  const msg = JSON.parse(line);",
  "  const respond = (extra) => process.stdout.write(JSON.stringify({ type: 'response', id: msg.id, command: msg.type, success: true, ...extra }) + '\\n');",
  "  if (msg.type === 'prompt') {",
  "    respond({});",
  "    process.stdout.write('NOT JSON AT ALL\\n');",
  "    for (const ev of [{ type: 'agent_start' }, { type: 'turn_start' }, { type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: 0, delta: 'FAKE-OK' } }, { type: 'message_end', message: {} }, { type: 'turn_end', message: {}, toolResults: [] }, { type: 'agent_end', messages: [], willRetry: false }, { type: 'agent_settled' }]) {",
  "      process.stdout.write(JSON.stringify(ev) + '\\n');",
  "    }",
  "    return;",
  "  }",
  "  respond(msg.type === 'get_state' ? { data: { sessionId: 'fake-session', isStreaming: false } } : {});",
  "});",
].join("\n");

const makeFakeRuntime = () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = yield* fileSystem.makeTempDirectoryScoped();
    const scriptPath = path.join(dir, "fake-pi.mjs");
    yield* fileSystem.writeFileString(scriptPath, FAKE_SCRIPT);
    return yield* makePiRpcRuntime({
      spawn: {
        command: process.execPath,
        args: [scriptPath],
        cwd: process.cwd(),
        env: process.env,
      },
      commandTimeoutMs: 30_000,
    });
  });

describe("pi RPC fake transport", () => {
  it.effect(
    "correlates responses, streams events, and survives a malformed line",
    () =>
      Effect.gen(function* () {
        const rpc = yield* makeFakeRuntime();
        const seen: Array<string> = [];
        const pump = yield* Stream.runForEach(Stream.fromQueue(rpc.events), (event) =>
          Effect.sync(() => {
            seen.push(event.type);
          }),
        ).pipe(Effect.forkChild);
        const state = yield* rpc.send({ type: "get_state" });
        expect(state.success).toBe(true);
        expect((state.data as { sessionId?: string }).sessionId).toBe("fake-session");
        yield* rpc.send({ type: "prompt", message: "hello" });
        yield* Fiber.interrupt(pump);
        const deadline = Date.now() + 15_000;
        while (Date.now() < deadline && !seen.includes("agent_settled")) {
          const next = yield* Queue.take(rpc.events).pipe(Effect.timeoutOption(1_000));
          if (next._tag === "Some") {
            seen.push(next.value.type);
            if (next.value.type === "agent_settled") {
              break;
            }
          }
        }
        expect(seen).toContain("agent_start");
        expect(seen).toContain("agent_settled");
        expect(seen).toContain("parse-error");
        expect(seen.filter((type) => type === "message_update")).toHaveLength(1);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "fails fast after close",
    () =>
      Effect.gen(function* () {
        const rpc = yield* makeFakeRuntime();
        const first = yield* rpc.send({ type: "get_state" });
        expect(first.success).toBe(true);
        yield* rpc.close;
        const result = yield* rpc.send({ type: "get_state" }).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "drain helper collects queued events",
    () =>
      Effect.gen(function* () {
        const rpc = yield* makeFakeRuntime();
        yield* rpc.send({ type: "prompt", message: "hello" });
        const collected: Array<string> = [];
        const deadline = Date.now() + 15_000;
        while (Date.now() < deadline) {
          const next = yield* Queue.take(rpc.events).pipe(Effect.timeoutOption(1_000));
          if (next._tag === "Some") {
            collected.push(next.value.type);
            if (next.value.type === "agent_settled") {
              break;
            }
          }
        }
        expect(collected).toContain("agent_settled");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    60_000,
  );
});
