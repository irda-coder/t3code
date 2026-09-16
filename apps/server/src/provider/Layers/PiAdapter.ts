/**
 * PiAdapter — pi (`pi --mode rpc`) via JSONL RPC.
 *
 * Sessions are one RPC runtime each: spawn, `new_session` (or
 * `switch_session` on resume), `prompt()` per turn, `abort` (+`clear_queue`
 * first) for interrupt, process kill for stop. The runtime owns
 * credentials; this adapter runs full-access: approval is launched
 * disabled (`--no-approve`) and `extension_ui_request` dialogs are
 * auto-answered permissively + logged so the event stream never stalls.
 *
 * Turn completion is `agent_settled` (not `turn_end`: retries/compaction can
 * follow). Interrupt completion is the `abort` response itself — the spike
 * showed no `settled` is emitted for aborted runs.
 *
 * Out of scope by design: approval answers, structured user input,
 * conversation rollback, mid-thread model switch (new thread instead),
 * steer/follow_up queueing, images (paths ride in prompt text instead).
 *
 * Protocol reference: `PI-RPC-REFERENCE.md` (worktree root).
 *
 * @module provider/Layers/PiAdapter
 */
import {
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import { canonicalItemTypeFromAcpToolKind } from "../acp/AcpRuntimeModel.ts";
import { PI_DRIVER_KIND, piSlugForModelRef, resolvePiModelRef, type PiModelRef } from "./PiProvider.ts";
import {
  buildPiRpcSpawnInput,
  makePiRpcRuntime,
  type PiRpcEvent,
  type PiRpcRuntime,
} from "../pi/PiRpcSupport.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER: ProviderDriverKind = PI_DRIVER_KIND;
const PI_RESUME_VERSION = 1 as const;

type Adapter = ProviderAdapterShape<ProviderAdapterError>;

export interface PiAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface PiSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly rpc: PiRpcRuntime;
  readerFiber: Fiber.Fiber<void, never> | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  pendingTurn:
    | { readonly turnId: TurnId; readonly done: Deferred.Deferred<TurnOutcome, never> }
    | undefined;
  modelRef: PiModelRef | undefined;
  stopped: boolean;
}

type TurnOutcome = "completed" | "cancelled" | "failed";

function parsePiResume(raw: unknown): { sessionFile: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  if (record["schemaVersion"] !== PI_RESUME_VERSION) return undefined;
  if (typeof record["sessionFile"] !== "string" || !record["sessionFile"].trim()) return undefined;
  return { sessionFile: (record["sessionFile"] as string).trim() };
}

function sameModelRef(left: PiModelRef | undefined, right: PiModelRef | undefined): boolean {
  if (!left || !right) return left === right;
  return left.provider === right.provider && left.model === right.model;
}

/** Map pi tool names onto ACP tool kinds for the canonical item-type mapping. */
function acpKindForToolName(toolName: string): string | undefined {
  const name = toolName.trim().toLowerCase();
  if (name === "bash" || name === "shell" || name === "command") return "execute";
  if (name === "read" || name === "edit" || name === "write" || name === "apply_patch") {
    return "edit";
  }
  if (name === "search" || name === "fetch" || name === "webfetch") return "search";
  return undefined;
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value : undefined;
}

/**
 * Delta text eligible for `content.delta` forwarding. Whitespace-only
 * deltas (notably pi's standalone `"\n"` events) must pass through
 * verbatim — trimming them here joins lines in the UI. Only truly empty
 * strings are dropped as noise.
 */
export function forwardableDeltaText(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

/** Bound output text per tool item (mirrors the ACP tail-window idea). */
export const PI_TOOL_TEXT_MAX_CHARS = 2000;

const encodeArgsJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** Join `{type:"text", text}` blocks of a pi result/partial content array. */
export function extractPiResultText(content: unknown): string {
  if (!Array.isArray(content)) {
    return "";
  }
  const parts: Array<string> = [];
  for (const entry of content) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const record = entry as Record<string, unknown>;
    if (record["type"] === "text" && typeof record["text"] === "string") {
      parts.push(record["text"]);
    }
  }
  return parts.join("\n");
}

export function tailText(text: string, maxChars: number = PI_TOOL_TEXT_MAX_CHARS): string {
  const trimmed = text.trim();
  if (trimmed.length <= maxChars) {
    return trimmed;
  }
  return `…[truncated]${trimmed.slice(trimmed.length - maxChars)}`;
}

/** Command string for bash-like tools; undefined for everything else. */
export function toolCommandOf(
  toolName: string,
  args: Record<string, unknown>,
): string | undefined {
  const name = toolName.trim().toLowerCase();
  if (name !== "bash" && name !== "shell" && name !== "command") {
    return undefined;
  }
  return asString(args["command"]);
}

/** One-line args summary for non-bash tools. */
export function compactArgsText(args: Record<string, unknown>): string | undefined {
  const keys = Object.keys(args);
  if (keys.length === 0) {
    return undefined;
  }
  return tailText(encodeArgsJson(args), 500);
}

/** Detail + data for a tool_execution_* event (args, command, outputs). */
export function piToolItemExtra(
  toolName: string,
  event: PiRpcEvent,
  resultText?: string,
): { readonly detail?: string; readonly data?: Record<string, unknown> } {
  const args = asRecord(event["args"]);
  const command = toolCommandOf(toolName, args);
  const text = resultText?.trim() ?? "";
  const detail = text ? tailText(text) : (command ?? compactArgsText(args));
  const data: Record<string, unknown> = {
    toolCallId: asString(event["toolCallId"]) ?? "",
    kind: toolName,
    ...(command ? { command } : {}),
    rawInput: args,
  };
  if (resultText !== undefined) {
    data["rawOutput"] = event["result"] ?? event["partialResult"] ?? null;
    if (text) {
      data["content"] = text.slice(0, PI_TOOL_TEXT_MAX_CHARS);
    }
  }
  return {
    ...(detail ? { detail } : {}),
    data,
  };
}

export function makePiAdapter(
  piSettings: PiSettings,
  options?: PiAdapterOptions,
): Effect.Effect<Adapter, never, Crypto.Crypto | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const sessions = new Map<ThreadId, PiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate pi runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<PiSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        if (ctx.pendingTurn) {
          const pending = ctx.pendingTurn;
          ctx.pendingTurn = undefined;
          yield* Deferred.succeed(pending.done, "cancelled" as TurnOutcome);
        }
        if (ctx.readerFiber) {
          yield* Fiber.interrupt(ctx.readerFiber);
        }
        yield* Effect.ignore(Scope.close(ctx.scope, Exit.void));
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const emitTurnCompleted = (
      ctx: PiSessionContext,
      turnId: TurnId,
      outcome: TurnOutcome,
      errorMessage?: string,
    ) =>
      Effect.gen(function* () {
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: {
            state: outcome,
            stopReason: outcome === "completed" ? "end_turn" : outcome,
            ...(errorMessage ? { errorMessage: errorMessage.slice(0, 500) } : {}),
          },
        });
      });

    const answerExtensionUi = (ctx: PiSessionContext, event: PiRpcEvent) =>
      Effect.gen(function* () {
        const id = asString(event["id"]);
        const method = asString(event["method"]) ?? "";
        if (!id) {
          yield* Effect.logWarning("pi extension_ui_request without id; ignoring.", {
            threadId: ctx.threadId,
          });
          return;
        }
        if (
          method !== "select" &&
          method !== "confirm" &&
          method !== "input" &&
          method !== "editor"
        ) {
          // Fire-and-forget methods (notify/setStatus/setWidget/setTitle/
          // set_editor_text) need no answer; surface warnings+ as logs.
          if (method === "notify") {
            const level = asString(event["notifyType"]) ?? "info";
            const message = asString(event["message"]) ?? "pi notification";
            if (level === "warning" || level === "error") {
              yield* Effect.logWarning("pi extension notification.", {
                threadId: ctx.threadId,
                message,
              });
            }
          }
          return;
        }
        const options = event["options"];
        const firstOption = Array.isArray(options)
          ? options.find((entry): entry is string => typeof entry === "string")
          : undefined;
        const response: Record<string, unknown> =
          method === "confirm"
            ? { type: "extension_ui_response", id, confirmed: true }
            : method === "select"
              ? { type: "extension_ui_response", id, value: firstOption ?? "Allow" }
              : method === "input"
                ? { type: "extension_ui_response", id, value: "" }
                : { type: "extension_ui_response", id, cancelled: true };
        yield* Effect.logDebug("pi auto-answered an extension UI request (full-access).", {
          threadId: ctx.threadId,
          method,
          title: asString(event["title"]) ?? "",
        });
        yield* ctx.rpc.send(response).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("pi extension UI auto-answer failed.", {
              threadId: ctx.threadId,
              detail: cause.detail,
            }),
          ),
        );
      });

    const emitToolItem = (
      ctx: PiSessionContext,
      lifecycle: "item.updated" | "item.completed",
      toolCallId: string,
      toolName: string,
      status: "inProgress" | "completed" | "failed",
      rawEvent: PiRpcEvent,
      extra?: { readonly detail?: string; readonly data?: Record<string, unknown> },
    ) =>
      Effect.gen(function* () {
        yield* offerRuntimeEvent({
          type: lifecycle,
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId: ctx.activeTurnId,
          itemId: RuntimeItemId.make(toolCallId),
          payload: {
            itemType: canonicalItemTypeFromAcpToolKind(acpKindForToolName(toolName)),
            status,
            title: toolName,
            ...(extra?.detail ? { detail: extra.detail } : {}),
            ...(extra?.data ? { data: extra.data } : {}),
          },
          raw: { source: "pi.rpc", method: rawEvent.type, payload: rawEvent },
        });
      });

    /** Detail + data for a tool_execution_* event (args, command, outputs). */
    const toolExecutionExtra = (
      toolName: string,
      event: PiRpcEvent,
      resultText?: string,
    ): { readonly detail?: string; readonly data?: Record<string, unknown> } =>
      piToolItemExtra(toolName, event, resultText);

    const handleRpcEvent = (ctx: PiSessionContext, event: PiRpcEvent) =>
      Effect.gen(function* () {
        switch (event.type) {
          case "message_start": {
            yield* offerRuntimeEvent(
              makeAcpAssistantItemEvent({
                stamp: yield* makeEventStamp(),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                itemId: `pi-assistant-${ctx.activeTurnId ?? "unknown"}`,
                lifecycle: "item.started",
              }),
            );
            return;
          }
          case "message_end": {
            yield* offerRuntimeEvent(
              makeAcpAssistantItemEvent({
                stamp: yield* makeEventStamp(),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId: ctx.activeTurnId,
                itemId: `pi-assistant-${ctx.activeTurnId ?? "unknown"}`,
                lifecycle: "item.completed",
              }),
            );
            return;
          }
          case "message_update": {
            const delta = asRecord(event["assistantMessageEvent"]);
            const kind = asString(delta["type"]) ?? "";
            if (kind === "text_delta") {
              // Forward verbatim, including whitespace-only deltas: pi
              // streams newlines as standalone events and trimming them
              // here joins lines in the UI (asString would drop "\n").
              const text = forwardableDeltaText(delta["delta"]);
              if (text !== undefined) {
                yield* offerRuntimeEvent(
                  makeAcpContentDeltaEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId: ctx.activeTurnId,
                    text,
                    rawPayload: event,
                  }),
                );
              }
              return;
            }
            if (kind === "thinking_delta") {
              const text = forwardableDeltaText(delta["delta"]);
              if (text !== undefined) {
                yield* offerRuntimeEvent(
                  makeAcpContentDeltaEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: ctx.threadId,
                    turnId: ctx.activeTurnId,
                    streamKind: "reasoning_text",
                    text,
                    rawPayload: event,
                  }),
                );
              }
              return;
            }
            if (kind === "toolcall_start") {
              yield* emitToolItem(
                ctx,
                "item.updated",
                asString(delta["id"]) ?? `pi-tool-${ctx.activeTurnId ?? "x"}`,
                asString(delta["toolName"]) ?? "tool",
                "inProgress",
                event,
              );
              return;
            }
            if (kind === "toolcall_end") {
              const full = asRecord(delta["toolCall"]);
              const toolName =
                asString(delta["toolName"]) ?? asString(full["name"]) ?? "tool";
              const toolArgs = asRecord(full["arguments"] ?? full["input"]);
              const command = toolCommandOf(toolName, toolArgs);
              const detail = command ?? compactArgsText(toolArgs) ?? toolName;
              yield* emitToolItem(
                ctx,
                "item.updated",
                asString(delta["id"]) ??
                  asString(full["id"]) ??
                  `pi-tool-${ctx.activeTurnId ?? "x"}`,
                toolName,
                "inProgress",
                event,
                {
                  detail,
                  data: {
                    toolCallId: asString(delta["id"]) ?? "",
                    kind: toolName,
                    ...(command ? { command } : {}),
                    rawInput: toolArgs,
                  },
                },
              );
              return;
            }
            return;
          }
          case "tool_execution_start":
          case "tool_execution_update": {
            const toolName = asString(event["toolName"]) ?? "tool";
            const partial = asRecord(event["partialResult"]);
            const partialText = extractPiResultText(partial["content"]);
            yield* emitToolItem(
              ctx,
              "item.updated",
              asString(event["toolCallId"]) ?? `pi-tool-${ctx.activeTurnId ?? "x"}`,
              toolName,
              "inProgress",
              event,
              toolExecutionExtra(toolName, event, partialText || undefined),
            );
            return;
          }
          case "tool_execution_end": {
            const toolName = asString(event["toolName"]) ?? "tool";
            const resultText = extractPiResultText(asRecord(event["result"])["content"]);
            yield* emitToolItem(
              ctx,
              "item.completed",
              asString(event["toolCallId"]) ?? `pi-tool-${ctx.activeTurnId ?? "x"}`,
              toolName,
              event["isError"] === true ? "failed" : "completed",
              event,
              toolExecutionExtra(toolName, event, resultText || undefined),
            );
            return;
          }
          case "turn_end": {
            ctx.turns.push({
              id: ctx.activeTurnId ?? TurnId.make(yield* randomUUIDv4),
              items: [event],
            });
            return;
          }
          case "agent_settled": {
            if (ctx.pendingTurn) {
              const pending = ctx.pendingTurn;
              ctx.pendingTurn = undefined;
              yield* Deferred.succeed(pending.done, "completed" as TurnOutcome);
            }
            return;
          }
          case "extension_ui_request": {
            yield* answerExtensionUi(ctx, event);
            return;
          }
          case "parse-error": {
            yield* Effect.logWarning("pi emitted a non-JSON stdout line.", {
              threadId: ctx.threadId,
            });
            return;
          }
          default: {
            // agent_start/agent_end/message deltas without text/queue/
            // compaction/retry chatter: ride through to agent_settled.
            return;
          }
        }
      });

    const failRpc = (
      threadId: ThreadId,
      op: string,
      error: { readonly detail: string },
    ): ProviderAdapterProcessError =>
      new ProviderAdapterProcessError({
        provider: PROVIDER,
        threadId,
        detail: `${op}: ${error.detail}`,
      });

    const startSession: Adapter["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }

          const cwd = input.cwd.trim();
          const startModelSelection =
            input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
          const modelRef = resolvePiModelRef(startModelSelection?.model);
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );
          let ctx!: PiSessionContext;

          const resumeSessionFile = parsePiResume(input.resumeCursor)?.sessionFile;
          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const spawnEnv =
            options?.environment || mcpSession?.agentDeviceEnvironment
              ? McpProviderSession.withAgentDeviceEnvironment(
                  options?.environment ?? process.env,
                  mcpSession,
                )
              : (options?.environment ?? process.env);
          const rpc = yield* makePiRpcRuntime({
            spawn: buildPiRpcSpawnInput(
              piSettings,
              cwd,
              spawnEnv,
              modelRef === undefined
                ? undefined
                : { provider: modelRef.provider, model: modelRef.model },
            ),
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.mapError((error) => failRpc(input.threadId, "spawn", error)),
          );

          const started = yield* Effect.gen(function* () {
            let sessionFile: string;
            if (resumeSessionFile) {
              const switched = yield* rpc
                .send({ type: "switch_session", sessionPath: resumeSessionFile })
                .pipe(Effect.mapError((error) => failRpc(input.threadId, "switch_session", error)));
              if (!switched.success) {
                return yield* new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: `switch_session failed: ${switched.error ?? "unknown"}`,
                });
              }
              sessionFile = resumeSessionFile;
            } else {
              const created = yield* rpc
                .send({ type: "new_session" })
                .pipe(Effect.mapError((error) => failRpc(input.threadId, "new_session", error)));
              if (!created.success) {
                return yield* new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: `new_session failed: ${created.error ?? "unknown"}`,
                });
              }
              const state = yield* rpc
                .send({ type: "get_state" })
                .pipe(Effect.mapError((error) => failRpc(input.threadId, "get_state", error)));
              const file = asString(asRecord(state.data)["sessionFile"]);
              if (!state.success || !file) {
                return yield* new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: "new_session did not report a sessionFile.",
                });
              }
              sessionFile = file;
            }

            const now = yield* nowIso;
            const session: ProviderSession = {
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              status: "ready",
              runtimeMode: input.runtimeMode,
              cwd,
              model: piSlugForModelRef(modelRef),
              threadId: input.threadId,
              resumeCursor: {
                schemaVersion: PI_RESUME_VERSION,
                sessionFile,
              },
              createdAt: now,
              updatedAt: now,
            };

            ctx = {
              threadId: input.threadId,
              session,
              scope: sessionScope,
              rpc,
              readerFiber: undefined,
              turns: [],
              activeTurnId: undefined,
              pendingTurn: undefined,
              modelRef,
              stopped: false,
            };

            const reader = yield* Queue.take(rpc.events).pipe(
              Effect.flatMap((event) =>
                handleRpcEvent(ctx, event).pipe(
                  Effect.catch((cause) =>
                    Effect.logError("Failed to process pi runtime event.", { cause }),
                  ),
                ),
              ),
              Effect.forever,
              Effect.forkIn(ctx.scope),
            );

            ctx.readerFiber = reader;
            sessions.set(input.threadId, ctx);
            sessionScopeTransferred = true;

            yield* offerRuntimeEvent({
              type: "session.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: {},
            });
            yield* offerRuntimeEvent({
              type: "session.state.changed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { state: "ready", reason: "pi RPC session ready" },
            });
            yield* offerRuntimeEvent({
              type: "thread.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { providerThreadId: sessionFile },
            });

            return session;
          }).pipe(Effect.scoped);

          return started;
        }).pipe(Effect.scoped),
      );

    const sendTurn: Adapter["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        if (ctx.pendingTurn) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "A turn is already in progress on this pi session.",
          });
        }
        if (
          input.modelSelection?.model !== undefined &&
          !sameModelRef(resolvePiModelRef(input.modelSelection.model), ctx.modelRef)
        ) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "pi does not support mid-thread model switches. Start a new thread to change model.",
          });
        }

        const turnId = TurnId.make(yield* randomUUIDv4);
        ctx.activeTurnId = turnId;
        ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };

        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          payload: { model: piSlugForModelRef(ctx.modelRef) },
        });

        const rawPrompt = input.input?.trim() ?? "";
        for (const attachment of input.attachments ?? []) {
          if (attachment.type === "image") {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "prompt",
              detail: `Image attachment '${attachment.id}' is not supported by the pi RPC surface in v1.`,
            });
          }
        }
        if (!rawPrompt && input.continuation !== true) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text.",
          });
        }

        const done = yield* Deferred.make<TurnOutcome, never>();
        ctx.pendingTurn = { turnId, done };
        const promptText = [
          rawPrompt,
          buildRuntimeInstructions({ harness: "pi", model: piSlugForModelRef(ctx.modelRef) }),
        ]
          .filter((part) => part.length > 0)
          .join("\n\n");

        const accepted = yield* ctx.rpc.send({ type: "prompt", message: promptText }).pipe(
          Effect.mapError((error) => failRpc(input.threadId, "prompt", error)),
        );
        if (!accepted.success) {
          ctx.pendingTurn = undefined;
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "prompt",
            detail: `pi rejected the prompt: ${accepted.error ?? "unknown"}`,
          });
        }

        const outcome = yield* Deferred.await(done);
        if (outcome === "failed") {
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            detail: "pi turn failed before settling.",
          });
        }
        yield* emitTurnCompleted(ctx, turnId, outcome);

        ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };
        return {
          threadId: input.threadId,
          turnId,
          resumeCursor: ctx.session.resumeCursor,
        };
      });

    const interruptTurn: Adapter["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        // Esc semantics: drop queued messages first, otherwise they continue
        // after the abort.
        yield* Effect.ignore(ctx.rpc.send({ type: "clear_queue" }));
        yield* Effect.ignore(ctx.rpc.send({ type: "abort" }));
        if (ctx.pendingTurn) {
          const pending = ctx.pendingTurn;
          ctx.pendingTurn = undefined;
          yield* Deferred.succeed(pending.done, "cancelled" as TurnOutcome);
        }
      });

    const respondToRequest: Adapter["respondToRequest"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail:
            "pi runs full-access: approval answers are not supported. Extension dialogs are auto-answered by the runtime.",
        }),
      );

    const respondToUserInput: Adapter["respondToUserInput"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: "pi does not support structured user-input questions.",
        }),
      );

    const readThread: Adapter["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return { threadId, turns: ctx.turns };
      });

    const rollbackThread: Adapter["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "thread/rollback",
          detail: "pi RPC sessions do not support provider-side rollback.",
        });
      });

    const stopSession: Adapter["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          yield* stopSessionInternal(ctx);
        }),
      );

    const listSessions: Adapter["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (c) => ({ ...c.session })));

    const hasSession: Adapter["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const c = sessions.get(threadId);
        return c !== undefined && !c.stopped;
      });

    const stopAll: Adapter["stopAll"] = () =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      Effect.forEach(sessions.values(), stopSessionInternal, { discard: true }).pipe(
        Effect.catch((cause) =>
          Effect.logError("Failed to emit pi session shutdown event.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    const streamEvents = Stream.fromPubSub(runtimeEventPubSub);

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "unsupported",
        supportsConversationRollback: false,
      },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents,
    } satisfies Adapter;
  });
}
