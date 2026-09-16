/**
 * PiTextGeneration — git/thread text generation through headless pi.
 *
 * Phase C: stub. Every operation fails with a typed error until Phase D
 * lands the `pi -p` / `--mode json` one-shot runtime (route pin + output
 * collect, mirroring CursorTextGeneration).
 *
 * @module textGeneration/PiTextGeneration
 */
import { type PiSettings, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type * as TextGeneration from "./TextGeneration.ts";
import { TextGeneration as TextGenerationService } from "./TextGeneration.ts";

const notImplemented = (
  operation:
    | "generateCommitMessage"
    | "generatePrContent"
    | "generateBranchName"
    | "generateThreadTitle",
) =>
  new TextGenerationError({
    operation,
    detail: "pi headless text generation is not yet implemented (Phase D).",
  });

/**
 * Build a pi text-generation closure bound to a specific `PiSettings`
 * payload. See `makeCodexAdapter` for the overall per-instance rationale.
 */
export const makePiTextGeneration = (
  _piSettings: PiSettings,
  _environment?: NodeJS.ProcessEnv,
): Effect.Effect<TextGeneration.TextGeneration["Service"]> =>
  Effect.succeed(
    TextGenerationService.of({
      generateCommitMessage: () => Effect.fail(notImplemented("generateCommitMessage")),
      generatePrContent: () => Effect.fail(notImplemented("generatePrContent")),
      generateBranchName: () => Effect.fail(notImplemented("generateBranchName")),
      generateThreadTitle: () => Effect.fail(notImplemented("generateThreadTitle")),
    }),
  );
