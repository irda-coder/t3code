/**
 * DshProvider — health probe and model snapshot for the DeepSeek Harness ACP runtime.
 *
 * The probe is `initialize`-only: one local round trip that reports the
 * runtime version and confirms the ACP server answers. It never calls
 * `authenticate` with a real credential (DSH answers any method id with
 * immediate success) and never creates a session, so background checks cannot
 * open browsers, boot MCP servers, or leave persisted sessions behind.
 *
 * Model catalog is static in v1 (the live route options arrive with each
 * `session/new` in Phase D): flash/pro default plus user custom models.
 *
 * @module provider/Layers/DshProvider
 */
import {
  type CustomModelSetting,
  type DshSettings,
  type ModelCapabilities,
  type ServerProvider,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { HttpClient } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import { DSH_DEFAULT_MODEL_SLUG, makeDshAcpRuntime } from "../acp/DshAcpSupport.ts";

const DSH_PRESENTATION = {
  displayName: "DeepSeek",
  supportsConversationRollback: false,
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

// `initialize` is a single local round trip, so this is generous even on slow machines.
const DSH_ACP_INITIALIZE_TIMEOUT_MS = 8_000;

const DSH_BUILT_IN_MODELS = [
  { slug: DSH_DEFAULT_MODEL_SLUG, name: "DeepSeek V4 Flash" },
  { slug: "deepseek-v4-pro", name: "DeepSeek V4 Pro" },
  { slug: "deepseek-flash", name: "DeepSeek Flash" },
] as const;

function dshModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
): ReturnType<typeof providerModelsFromSettings> {
  return providerModelsFromSettings(
    DSH_BUILT_IN_MODELS.map((model, index) => ({
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

export function buildInitialDshProviderSnapshot(
  dshSettings: DshSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = dshModelsFromSettings(dshSettings.customModels);

    if (!dshSettings.enabled) {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "DeepSeek is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking DeepSeek Harness availability...",
      },
    });
  });
}

export const checkDshProviderStatus = Effect.fn("checkDshProviderStatus")(function* (
  dshSettings: DshSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = dshModelsFromSettings(dshSettings.customModels);

  if (!dshSettings.enabled) {
    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "DeepSeek is disabled in T3 Code settings.",
      },
    });
  }

  const initializeExit = yield* Effect.gen(function* () {
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const acp = yield* makeDshAcpRuntime({
      dshSettings,
      environment,
      childProcessSpawner,
      cwd: process.cwd(),
      clientInfo: { name: "t3-code-provider-probe", version: "0.0.0" },
    });
    return yield* acp.initialize();
  }).pipe(Effect.scoped, Effect.timeoutOption(DSH_ACP_INITIALIZE_TIMEOUT_MS), Effect.exit);

  if (Exit.isFailure(initializeExit)) {
    const errorTag = causeErrorTag(initializeExit.cause);
    yield* Effect.logWarning("DeepSeek Harness ACP initialize probe failed.", { errorTag });
    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: dshSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(initializeExit.cause),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(initializeExit.cause)
          ? "DeepSeek Harness (`dsh`) is not installed or not on PATH. Install with `npm i -g @deepseek-ai/dsh`."
          : "DeepSeek Harness is installed but the ACP runtime did not answer `initialize`.",
      },
    });
  }

  if (Option.isNone(initializeExit.value)) {
    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: dshSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "DeepSeek Harness timed out while answering `initialize`.",
      },
    });
  }

  // Credentials live in the runtime's own store; T3 cannot observe login state
  // without starting a session, which probes must not do.
  return buildServerProvider({
    presentation: DSH_PRESENTATION,
    enabled: dshSettings.enabled,
    checkedAt,
    models: fallbackModels,
    probe: {
      installed: true,
      version: initializeExit.value.value.agentInfo?.version ?? null,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

export const enrichDshSnapshot = (input: {
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
      Effect.logWarning("DeepSeek version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
};
