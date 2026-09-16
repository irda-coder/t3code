/**
 * PiProvider — health probe and model snapshot for the pi RPC runtime.
 *
 * The probe is `pi --version`-only in v1: one local round trip that reports
 * the runtime version and confirms the binary answers. It never boots the
 * agent runtime, never creates a session, so background checks cannot open
 * browsers, run tools, or leave persisted sessions behind. The fuller
 * `get_available_models` RPC check arrives with the Phase D transport.
 *
 * Model catalog is static in v1: the `pi-default` sentinel (adapter uses the
 * session's current model, no `set_model` call) plus user custom models in
 * `provider/id` form. The live 395-model catalog arrives with Phase D.
 *
 * @module provider/Layers/PiProvider
 */
import {
  type CustomModelSetting,
  type ModelCapabilities,
  type PiSettings,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";

export const PI_DRIVER_KIND = ProviderDriverKind.make("pi");

const PI_PRESENTATION = {
  displayName: "pi",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

// `pi --version` is a single local round trip, so this is generous even on slow machines.
const PI_VERSION_TIMEOUT_MS = 8_000;

/** Sentinel slug: adapter uses the session default model, no `set_model` call. */
export const PI_DEFAULT_MODEL_SLUG = "pi-default";

const PI_BUILT_IN_MODELS = [{ slug: PI_DEFAULT_MODEL_SLUG, name: "pi Default" }] as const;

function piModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
): ReturnType<typeof providerModelsFromSettings> {
  return providerModelsFromSettings(
    PI_BUILT_IN_MODELS.map((model, index) => ({
      slug: model.slug,
      name: model.name,
      isCustom: false,
      ...(index === 0 ? { isDefault: true } : {}),
      capabilities: EMPTY_CAPABILITIES,
    })),
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

export interface PiModelRef {
  readonly provider: string;
  readonly model: string;
}

/**
 * T3's pi slug. `pi-default` (or empty) selects the session's current model;
 * `provider/id` selects an explicit `set_model` target.
 */
export function resolvePiModelRef(model: string | null | undefined): PiModelRef | undefined {
  const trimmed = model?.trim();
  if (!trimmed || trimmed === PI_DEFAULT_MODEL_SLUG) {
    return undefined;
  }
  const slash = trimmed.indexOf("/");
  if (slash > 0) {
    const provider = trimmed.slice(0, slash).trim();
    const id = trimmed.slice(slash + 1).trim();
    if (provider && id) {
      return { provider, model: id };
    }
    return undefined;
  }
  return undefined;
}

/** Encode a model ref back to a T3 slug. */
export function piSlugForModelRef(ref: PiModelRef | undefined): string {
  if (!ref) {
    return PI_DEFAULT_MODEL_SLUG;
  }
  return `${ref.provider}/${ref.model}`;
}

const runPiCliCommand = (
  piSettings: PiSettings,
  args: ReadonlyArray<string>,
  environment: NodeJS.ProcessEnv,
) =>
  Effect.gen(function* () {
    const command = piSettings.binaryPath || "pi";
    const spawnCommand = yield* resolveSpawnCommand(command, args, { env: environment });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings.customModels);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "pi is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking pi availability...",
      },
    });
  });
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = piModelsFromSettings(piSettings.customModels);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionExit = yield* runPiCliCommand(piSettings, ["--version"], environment).pipe(
    Effect.timeoutOption(PI_VERSION_TIMEOUT_MS),
    Effect.exit,
  );

  if (Exit.isFailure(versionExit)) {
    const errorTag = causeErrorTag(versionExit.cause);
    yield* Effect.logWarning("pi version probe failed.", { errorTag });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(versionExit.cause),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(versionExit.cause)
          ? "pi is not installed or not on PATH. Install with `npm i -g @earendil-works/pi-coding-agent`."
          : "pi is installed but did not answer `--version`.",
      },
    });
  }

  if (Option.isNone(versionExit.value)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "pi timed out while answering `--version`.",
      },
    });
  }

  const versionOutput = versionExit.value.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  // Credentials live in the runtime's own store; T3 cannot observe login state
  // without starting a session, which probes must not do.
  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models: fallbackModels,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

export const enrichPiSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> => {
  const { snapshot, publishSnapshot } = input;

  return enrichProviderSnapshotWithVersionAdvisory(snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("pi version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
