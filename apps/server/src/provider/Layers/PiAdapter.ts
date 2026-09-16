/**
 * PiAdapter — pi (`pi --mode rpc`) via JSONL RPC.
 *
 * Phase C: stub. The provider is visible (settings, picker, catalog) but
 * every session operation fails with a typed error until Phase D lands the
 * RPC lifecycle (`new_session` / `prompt` / `abort` / kill), the
 * `message_update` / `tool_execution_*` event mapping, and the
 * `extension_ui_request` full-access auto-answer.
 *
 * Out of scope by design (mirroring dsh): approval answers, structured
 * user input, conversation rollback, mid-thread model switch (new thread
 * instead), steer/follow_up queueing.
 *
 * @module provider/Layers/PiAdapter
 */
import {
  type PiSettings,
  ProviderDriverKind,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

import {
  ProviderAdapterRequestError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { PI_DRIVER_KIND } from "./PiProvider.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER: ProviderDriverKind = PI_DRIVER_KIND;

type Adapter = ProviderAdapterShape<ProviderAdapterError>;

export interface PiAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

const notImplemented = (method: string) =>
  new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: "pi RPC lifecycle is not yet implemented (Phase D).",
  });

export function makePiAdapter(
  _piSettings: PiSettings,
  options?: PiAdapterOptions,
): Effect.Effect<Adapter, never, never> {
  const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
  void boundInstanceId;
  return Effect.succeed({
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported",
      supportsConversationRollback: false,
    },
    startSession: (input) => Effect.fail(notImplemented(`startSession:${input.threadId}`)),
    sendTurn: (input) => Effect.fail(notImplemented(`sendTurn:${input.threadId}`)),
    interruptTurn: (threadId) => Effect.fail(notImplemented(`interruptTurn:${threadId}`)),
    respondToRequest: (_threadId, requestId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail: `Approval request ${requestId} is disabled in full-access mode.`,
        }),
      ),
    respondToUserInput: (_threadId, requestId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: `User-input request ${requestId} is disabled in full-access mode.`,
        }),
      ),
    stopSession: (threadId) => Effect.fail(notImplemented(`stopSession:${threadId}`)),
    listSessions: () => Effect.succeed([]),
    hasSession: (_threadId) => Effect.succeed(false),
    readThread: (threadId) => Effect.fail(notImplemented(`readThread:${threadId}`)),
    rollbackThread: (threadId, numTurns) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "rollbackThread",
          detail: `Rollback of ${numTurns} turn(s) on ${threadId} is unsupported for pi: no checkpoint boundary exists.`,
        }),
      ),
    stopAll: () => Effect.fail(notImplemented("stopAll")),
    streamEvents: Stream.empty,
  } satisfies Adapter);
}
