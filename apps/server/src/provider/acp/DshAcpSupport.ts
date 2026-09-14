/**
 * DshAcpSupport — spawn + model-route helpers for the DeepSeek Harness ACP runtime.
 *
 * DSH ships a standard ACP v1 stdio server (`dsh --profile acp`). Auth is
 * runtime-owned: `authenticate` answers immediate success for any method id,
 * so T3 never handles a DeepSeek credential. Model selection is the session's
 * `model` config option whose value is a JSON-encoded `[provider, model]` pair
 * (e.g. `["deepseek-official","deepseek-v4-flash"]`).
 *
 * @module provider/acp/DshAcpSupport
 */
import { type DshSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";

import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

export const DSH_DRIVER_KIND = ProviderDriverKind.make("dsh");
/** DSH never challenges `authenticate`; any method id answers `{}`. */
export const DSH_ACP_AUTH_METHOD_ID = "dsh_managed";
export const DSH_HOME_ENV = "DSH_HOME";
export const DSH_DEFAULT_PROVIDER_ROUTE = "deepseek-official";
export const DSH_DEFAULT_MODEL_SLUG = "deepseek-v4-flash";
export const DSH_ACP_MODEL_CONFIG_ID = "model";

type DshAcpRuntimeDshSettings = Pick<DshSettings, "binaryPath" | "profile" | "homePath">;

export interface DshAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly dshSettings: DshAcpRuntimeDshSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

export function buildDshAcpSpawnInput(
  dshSettings: DshAcpRuntimeDshSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  const homePath = dshSettings?.homePath?.trim();
  return {
    command: dshSettings?.binaryPath || "dsh",
    args: ["--profile", dshSettings?.profile?.trim() || "acp"],
    cwd,
    ...(environment || homePath
      ? {
          env: {
            ...environment,
            ...(homePath ? { [DSH_HOME_ENV]: homePath } : {}),
          },
        }
      : {}),
  };
}

export const makeDshAcpRuntime = (
  input: DshAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildDshAcpSpawnInput(input.dshSettings, input.cwd, input.environment),
        authMethodId: DSH_ACP_AUTH_METHOD_ID,
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });

export interface DshModelRoute {
  readonly provider: string;
  readonly model: string;
}

/**
 * T3's DSH slug. The product slug (e.g. `deepseek-v4-flash`) selects the
 * session's current route; `provider/model` selects an explicit route.
 */
export function resolveDshModelRoute(model: string | null | undefined): DshModelRoute {
  const trimmed = model?.trim();
  if (!trimmed) {
    return { provider: DSH_DEFAULT_PROVIDER_ROUTE, model: DSH_DEFAULT_MODEL_SLUG };
  }
  const slash = trimmed.indexOf("/");
  if (slash > 0) {
    const provider = trimmed.slice(0, slash).trim() || DSH_DEFAULT_PROVIDER_ROUTE;
    const name = trimmed.slice(slash + 1).trim() || DSH_DEFAULT_MODEL_SLUG;
    return { provider, model: name };
  }
  return { provider: DSH_DEFAULT_PROVIDER_ROUTE, model: trimmed };
}

/** Encode a route as the ACP `model` config-option value. */
export function encodeDshModelOptionValue(route: DshModelRoute): string {
  return JSON.stringify([route.provider, route.model]);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/** Decode an ACP `model` config-option value. Returns undefined when malformed. */
export function decodeDshModelOptionValue(value: unknown): DshModelRoute | undefined {
  if (!isNonEmptyString(value)) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(value);
    if (
      Array.isArray(parsed) &&
      parsed.length === 2 &&
      isNonEmptyString(parsed[0]) &&
      isNonEmptyString(parsed[1])
    ) {
      return { provider: parsed[0].trim(), model: parsed[1].trim() };
    }
    return undefined;
  } catch {
    return undefined;
  }
}
