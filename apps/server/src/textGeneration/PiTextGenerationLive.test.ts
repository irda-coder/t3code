/**
 * Optional live check: thread title through headless pi.
 *
 * Enable with: T3_PI_RPC_PROBE=1 OPENROUTER_API_KEY=... vp test run PiTextGenerationLive
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect } from "vite-plus/test";

import { makePiTextGeneration } from "./PiTextGeneration.ts";

describe.runIf(process.env.T3_PI_RPC_PROBE === "1")("pi text generation live", () => {
  it.effect(
    "generates a thread title through headless pi",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const cwd = yield* fileSystem.makeTempDirectoryScoped();
        const textGeneration = yield* makePiTextGeneration(
          {
            enabled: true,
            binaryPath: process.env.T3_PI_BINARY || "pi",
            sessionDir: "",
            customModels: [],
          },
          process.env,
        );
        const result = yield* textGeneration.generateThreadTitle({
          cwd,
          message: "Repair the login redirect loop on the settings page",
          modelSelection: {
            instanceId: ProviderInstanceId.make("pi"),
            model: process.env.T3_PI_MODEL || "openrouter/inclusionai/ling-3.0-flash-vl:free",
          },
        });
        expect(result.title.trim().length).toBeGreaterThan(0);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    300_000,
  );
});
