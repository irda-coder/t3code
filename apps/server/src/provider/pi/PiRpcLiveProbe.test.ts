/**
 * Optional integration check against a real `pi --mode rpc` runtime.
 *
 * Enable with: T3_PI_RPC_PROBE=1 vp test run PiRpcLiveProbe
 * Keyed runs need OPENROUTER_API_KEY in env (or another working route via
 * T3_PI_PROVIDER / T3_PI_MODEL):
 *
 *   T3_PI_RPC_PROBE=1 OPENROUTER_API_KEY=... vp test run PiRpcLiveProbe
 *
 * T3_PI_BINARY overrides the spawned binary (default "pi").
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { buildPiRpcSpawnInput, makePiRpcRuntime } from "./PiRpcSupport.ts";

const piSettings = () => ({
  binaryPath: process.env.T3_PI_BINARY || "pi",
  sessionDir: process.env.T3_PI_SESSION_DIR ?? "",
});

const provider = () => process.env.T3_PI_PROVIDER || "openrouter";
const model = () => process.env.T3_PI_MODEL || "inclusionai/ling-3.0-flash-vl:free";

describe.runIf(process.env.T3_PI_RPC_PROBE === "1")("pi RPC live probe", () => {
  it.effect(
    "get_state answers on a fresh rpc process",
    () =>
      Effect.gen(function* () {
        const rpc = yield* makePiRpcRuntime({
          spawn: buildPiRpcSpawnInput(piSettings(), process.cwd(), process.env, {
            provider: provider(),
            model: model(),
          }),
        });
        const state = yield* rpc.send({ type: "get_state" });
        expect(state.success).toBe(true);
        expect(typeof (state.data as { sessionId?: unknown } | undefined)?.sessionId).toBe(
          "string",
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    120_000,
  );

  it.effect(
    "finishes a real turn and streams its answer",
    () =>
      Effect.gen(function* () {
        const rpc = yield* makePiRpcRuntime({
          spawn: buildPiRpcSpawnInput(piSettings(), process.cwd(), process.env, {
            provider: provider(),
            model: model(),
          }),
        });
        yield* rpc.send({ type: "new_session" });
        const chunks: Array<string> = [];
        const settled = yield* Deferred.make<void>();
        const pump = yield* Stream.runForEach(Stream.fromQueue(rpc.events), (event) =>
          Effect.gen(function* () {
            if (event.type === "message_update") {
              const delta = (event["assistantMessageEvent"] ?? {}) as {
                type?: string;
                delta?: string;
              };
              if (delta.type === "text_delta" && typeof delta.delta === "string") {
                chunks.push(delta.delta);
              }
            }
            if (event.type === "agent_settled") {
              yield* Deferred.succeed(settled, undefined);
            }
          }),
        ).pipe(Effect.forkChild);
        yield* rpc.send({ type: "prompt", message: "Reply with PI-PROBE-OK and nothing else." });
        yield* Deferred.await(settled).pipe(Effect.timeoutOption(240_000));
        yield* Fiber.interrupt(pump);
        expect(chunks.join("")).toContain("PI-PROBE-OK");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    300_000,
  );
});
