/**
 * Optional integration check against a real `dsh --profile acp` runtime.
 * Enable with: T3_DSH_ACP_PROBE=1 vp test run DshAcpCliProbe
 * Set T3_DSH_LIVE_TURN=1 to also send small prompts to the real model.
 *
 * The live-turn tests need a model route with credentials, e.g. a scratch
 * DSH_HOME whose settings.yaml declares an OpenRouter route plus
 * OPENROUTER_API_KEY in the environment:
 *
 *   DSH_HOME=/tmp/dsh-probe-home OPENROUTER_API_KEY=... \
 *     T3_DSH_ACP_PROBE=1 T3_DSH_LIVE_TURN=1 T3_DSH_MODEL='["openrouter","model-id"]' \
 *     vp test run DshAcpCliProbe
 *
 * T3_DSH_BINARY overrides the spawned binary (default "dsh").
 * T3_DSH_MODEL overrides the model route pinned for live turns
 * (default '["deepseek-official","deepseek-v4-flash"]').
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect } from "vite-plus/test";

import {
  DSH_ACP_MODEL_CONFIG_ID,
  decodeDshModelOptionValue,
  makeDshAcpRuntime,
} from "./DshAcpSupport.ts";

const dshSettings = () => ({
  binaryPath: process.env.T3_DSH_BINARY || "dsh",
  profile: "acp",
  homePath: process.env.T3_DSH_HOME ?? "",
});

const makeProbeRuntime = (cwd: string) =>
  Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return yield* makeDshAcpRuntime({
      dshSettings: dshSettings(),
      environment: process.env,
      childProcessSpawner,
      cwd,
      clientInfo: { name: "t3-dsh-probe", version: "0.0.0" },
    });
  });

const liveModelValue = () =>
  process.env.T3_DSH_MODEL ?? JSON.stringify(["deepseek-official", "deepseek-v4-flash"]);

describe.runIf(process.env.T3_DSH_ACP_PROBE === "1")("DSH ACP CLI probe", () => {
  it.effect("starts a real session over ACP initialize", () =>
    Effect.gen(function* () {
      const runtime = yield* makeProbeRuntime(process.cwd());
      const started = yield* runtime.start();
      expect(typeof started.sessionId).toBe("string");
      expect(started.initializeResult.protocolVersion).toBe(1);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("session/new advertises a model config option", () =>
    Effect.gen(function* () {
      const runtime = yield* makeProbeRuntime(process.cwd());
      const started = yield* runtime.start();
      const options = yield* runtime.getConfigOptions;
      const model = options.find((option) => option.id === DSH_ACP_MODEL_CONFIG_ID);
      expect(model).toBeDefined();
      expect(decodeDshModelOptionValue(model?.currentValue)).toBeDefined();
      expect(started.sessionId).toBeDefined();
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect.skipIf(process.env.T3_DSH_LIVE_TURN !== "1")(
    "finishes a real turn and streams its answer",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const cwd = yield* fileSystem.makeTempDirectoryScoped();
        const runtime = yield* makeProbeRuntime(cwd);
        yield* runtime.start();
        yield* runtime.setConfigOption(DSH_ACP_MODEL_CONFIG_ID, liveModelValue());
        const chunks: string[] = [];
        const events = yield* Stream.runForEach(runtime.getEvents(), (event) => {
          if (event._tag === "EventStreamBarrier") {
            return Deferred.succeed(event.acknowledge, undefined);
          }
          if (event._tag === "ContentDelta") {
            chunks.push(event.text);
          }
          return Effect.void;
        }).pipe(Effect.forkChild);
        const result = yield* runtime.prompt({
          prompt: [{ type: "text", text: "Reply with exactly DSH_T3_OK. Do not use any tools." }],
        });
        yield* runtime.drainEvents;
        expect(result.stopReason).toBe("end_turn");
        expect(chunks.join("")).toContain("DSH_T3_OK");
        yield* Fiber.interrupt(events);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect.skipIf(process.env.T3_DSH_LIVE_TURN !== "1")(
    "cancels a running turn through the shared runtime",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const cwd = yield* fileSystem.makeTempDirectoryScoped();
        const runtime = yield* makeProbeRuntime(cwd);
        yield* runtime.start();
        yield* runtime.setConfigOption(DSH_ACP_MODEL_CONFIG_ID, liveModelValue());
        const dispatched = yield* Deferred.make<void>();
        const events = yield* Stream.runForEach(runtime.getEvents(), (event) => {
          if (event._tag === "EventStreamBarrier") {
            return Deferred.succeed(event.acknowledge, undefined);
          }
          if (event._tag === "ToolCallUpdated") {
            return Deferred.succeed(dispatched, undefined).pipe(Effect.ignore);
          }
          return Effect.void;
        }).pipe(Effect.forkChild);
        const t0 = yield* Clock.currentTimeMillis;
        const mark = (label: string) =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((now) => Effect.logInfo(`[cancel-probe +${now - t0}ms] ${label}`)),
          );
        yield* mark("prompt fiber forking");
        const promptFiber = yield* runtime
          .prompt({
            prompt: [
              {
                type: "text",
                text: "Sleep in the foreground for 45 seconds, then reply done. Do not background it.",
              },
            ],
          })
          .pipe(Effect.forkChild);
        // Wait for dispatch proof before cancelling: the cancel must target
        // a registered prompt, never a queued one (free-tier first-token
        // latency varies, so a fixed sleep would race dispatch).
        const dispatch = yield* Deferred.await(dispatched).pipe(Effect.timeoutOption("90 seconds"));
        yield* mark(`dispatch gate exit (arrived=${Option.isSome(dispatch)})`);
        if (Option.isNone(dispatch)) {
          return yield* Effect.fail("prompt never dispatched a tool call");
        }
        yield* runtime.cancel;
        yield* mark("cancel sent");
        const result = yield* Fiber.join(promptFiber);
        yield* mark("prompt settled");
        expect(result.stopReason).toBe("cancelled");
        yield* Fiber.interrupt(events);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    // Free-tier first-token latency varies; the scenario itself is ~60s.
    300_000,
  );
});
