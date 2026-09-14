/**
 * DshAdapter — DeepSeek Harness (`dsh --profile acp`) via ACP.
 *
 * Sessions are one ACP runtime each: `start()` (initialize, authenticate,
 * session new/resume), `prompt()` per turn, `cancel` (notification) for
 * interrupt, scope close + `session/close` for stop. The runtime owns
 * credentials and permission policy; this adapter runs full-access and
 * auto-allows `session/request_permission` (allow_always, else allow_once,
 * else the first offered option).
 *
 * Out of scope by design: approval answers, structured user input,
 * conversation rollback, mid-thread model switch (new thread instead),
 * images (the ACP surface advertises `image: false`).
 *
 * @module provider/Layers/DshAdapter
 */
import {
  type DshSettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
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
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpSchema from "effect-acp/schema";

import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { mapAcpToAdapterError } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import {
  DSH_ACP_MODEL_CONFIG_ID,
  DSH_DRIVER_KIND,
  encodeDshModelOptionValue,
  makeDshAcpRuntime,
  resolveDshModelRoute,
  type DshModelRoute,
} from "../acp/DshAcpSupport.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER: ProviderDriverKind = DSH_DRIVER_KIND;
const DSH_RESUME_VERSION = 1 as const;

type Adapter = ProviderAdapterShape<ProviderAdapterError>;

export interface DshAdapterOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

interface DshSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  /** Prompts in flight; >0 means a new sendTurn steers the running turn. */
  promptsInFlight: number;
  modelRoute: DshModelRoute;
  stopped: boolean;
}

function parseDshResume(raw: unknown): { sessionId: string } | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  if (record["schemaVersion"] !== DSH_RESUME_VERSION) return undefined;
  if (typeof record["sessionId"] !== "string" || !record["sessionId"].trim()) return undefined;
  return { sessionId: (record["sessionId"] as string).trim() };
}

/** T3 slug for a route: bare model for the official route, `provider/model` otherwise. */
export function dshModelSlugForRoute(route: DshModelRoute): string {
  return route.provider === "deepseek-official" ? route.model : `${route.provider}/${route.model}`;
}

function sameRoute(left: DshModelRoute, right: DshModelRoute): boolean {
  return left.provider === right.provider && left.model === right.model;
}

function selectAutoAllowOptionId(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  const validIds = request.options
    .map((option) => option.optionId)
    .filter(
      (optionId): optionId is string => typeof optionId === "string" && optionId.trim().length > 0,
    );
  if (validIds.length === 0) return undefined;
  const byKind = (kind: string) =>
    request.options.find((option) => option.kind === kind)?.optionId as string | undefined;
  return byKind("allow_always") ?? byKind("allow_once") ?? byKind("allow") ?? validIds[0];
}

export function makeDshAdapter(
  dshSettings: DshSettings,
  options?: DshAdapterOptions,
): Effect.Effect<
  Adapter,
  never,
  Crypto.Crypto | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("dsh");
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const crypto = yield* Crypto.Crypto;

    const sessions = new Map<ThreadId, DshSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate DeepSeek runtime identifier.",
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
    ): Effect.Effect<DshSessionContext, ProviderAdapterSessionNotFoundError> => {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }),
        );
      }
      return Effect.succeed(ctx);
    };

    const stopSessionInternal = (ctx: DshSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        if (ctx.notificationFiber) {
          yield* Fiber.interrupt(ctx.notificationFiber);
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

    const applyModelRoute = (
      acp: AcpSessionRuntime.AcpSessionRuntime["Service"],
      threadId: ThreadId,
      route: DshModelRoute,
    ) =>
      acp.setConfigOption(DSH_ACP_MODEL_CONFIG_ID, encodeDshModelOptionValue(route)).pipe(
        Effect.mapError((error) =>
          mapAcpToAdapterError(PROVIDER, threadId, "session/set_config_option", error),
        ),
        Effect.asVoid,
      );

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
          const existing = sessions.get(input.threadId);
          if (existing && !existing.stopped) {
            yield* stopSessionInternal(existing);
          }

          const sessionScope = yield* Scope.make("sequential");
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred ? Effect.void : Scope.close(sessionScope, Exit.void),
          );
          let ctx!: DshSessionContext;

          const resumeSessionId = parseDshResume(input.resumeCursor)?.sessionId;
          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const acp = yield* makeDshAcpRuntime({
            dshSettings,
            ...(options?.environment || mcpSession?.agentDeviceEnvironment
              ? {
                  environment: McpProviderSession.withAgentDeviceEnvironment(
                    options?.environment ?? process.env,
                    mcpSession,
                  ),
                }
              : {}),
            childProcessSpawner,
            cwd,
            ...(resumeSessionId ? { resumeSessionId, resumeMethod: "resume" as const } : {}),
            clientInfo: { name: "t3-code", version: "0.0.0" },
            ...(mcpSession
              ? {
                  mcpServers: [
                    {
                      type: "http" as const,
                      name: "t3-code",
                      url: mcpSession.endpoint,
                      headers: [
                        {
                          name: "Authorization",
                          value: mcpSession.authorizationHeader,
                        },
                      ],
                    },
                  ],
                }
              : {}),
          }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, sessionScope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );

          const started = yield* Effect.gen(function* () {
            yield* acp.handleRequestPermission((params) =>
              Effect.gen(function* () {
                const optionId = selectAutoAllowOptionId(params);
                yield* Effect.logDebug("DeepSeek auto-allowed a permission request.", {
                  threadId: input.threadId,
                  toolCall: params.toolCall.title,
                  optionId,
                });
                if (optionId === undefined) {
                  return { outcome: { outcome: "cancelled" as const } };
                }
                return { outcome: { outcome: "selected" as const, optionId } };
              }),
            );

            const startedResult = yield* acp
              .start()
              .pipe(
                Effect.mapError((error) =>
                  mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", error),
                ),
              );

            // Pin the route once. Mid-thread switches are unsupported
            // (new thread instead), so this is the only selection point.
            const route = resolveDshModelRoute(startModelSelection?.model);
            yield* applyModelRoute(acp, input.threadId, route);

            const now = yield* nowIso;
            const session: ProviderSession = {
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              status: "ready",
              runtimeMode: input.runtimeMode,
              cwd,
              model: dshModelSlugForRoute(route),
              threadId: input.threadId,
              resumeCursor: {
                schemaVersion: DSH_RESUME_VERSION,
                sessionId: startedResult.sessionId,
              },
              createdAt: now,
              updatedAt: now,
            };

            ctx = {
              threadId: input.threadId,
              session,
              scope: sessionScope,
              acp,
              notificationFiber: undefined,
              turns: [],
              activeTurnId: undefined,
              promptsInFlight: 0,
              modelRoute: route,
              stopped: false,
            };

            const nf = yield* Stream.runDrain(
              Stream.mapEffect(acp.getEvents(), (event) =>
                Effect.gen(function* () {
                  switch (event._tag) {
                    case "EventStreamBarrier":
                      yield* Deferred.succeed(event.acknowledge, undefined);
                      return;
                    case "ModeChanged":
                      return;
                    case "ConnectionTerminated": {
                      const detail = event.error.message;
                      yield* Effect.logError("DeepSeek runtime connection terminated.", {
                        threadId: ctx.threadId,
                        detail,
                      });
                      if (ctx.activeTurnId) {
                        yield* offerRuntimeEvent({
                          type: "turn.completed",
                          ...(yield* makeEventStamp()),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: ctx.activeTurnId,
                          payload: {
                            state: "failed",
                            stopReason: "error",
                            errorMessage: detail.slice(0, 500),
                          },
                        });
                      }
                      yield* offerRuntimeEvent({
                        type: "session.state.changed",
                        ...(yield* makeEventStamp()),
                        provider: PROVIDER,
                        threadId: ctx.threadId,
                        payload: {
                          state: "error",
                          reason: "DeepSeek runtime connection terminated.",
                        },
                      });
                      return;
                    }
                    case "AssistantItemStarted":
                      yield* offerRuntimeEvent(
                        makeAcpAssistantItemEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: ctx.activeTurnId,
                          itemId: event.itemId,
                          lifecycle: "item.started",
                        }),
                      );
                      return;
                    case "AssistantItemCompleted":
                      yield* offerRuntimeEvent(
                        makeAcpAssistantItemEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: ctx.activeTurnId,
                          itemId: event.itemId,
                          lifecycle: "item.completed",
                        }),
                      );
                      return;
                    case "PlanUpdated":
                      // DSH emits no plans; ignore rather than surface noise.
                      return;
                    case "ToolCallUpdated":
                      yield* offerRuntimeEvent(
                        makeAcpToolCallEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: ctx.activeTurnId,
                          toolCall: event.toolCall,
                          rawPayload: event.rawPayload,
                        }),
                      );
                      return;
                    case "ContentDelta":
                      yield* offerRuntimeEvent(
                        makeAcpContentDeltaEvent({
                          stamp: yield* makeEventStamp(),
                          provider: PROVIDER,
                          threadId: ctx.threadId,
                          turnId: ctx.activeTurnId,
                          ...(event.itemId ? { itemId: event.itemId } : {}),
                          text: event.text,
                          rawPayload: event.rawPayload,
                        }),
                      );
                      return;
                  }
                }),
              ),
            ).pipe(
              Effect.catch((cause) =>
                Effect.logError("Failed to process DeepSeek runtime notification.", { cause }),
              ),
              // Fork into the session scope, not the calling fiber (see #5781).
              Effect.forkIn(ctx.scope),
            );

            ctx.notificationFiber = nf;
            sessions.set(input.threadId, ctx);
            sessionScopeTransferred = true;

            yield* offerRuntimeEvent({
              type: "session.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { resume: startedResult.initializeResult },
            });
            yield* offerRuntimeEvent({
              type: "session.state.changed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { state: "ready", reason: "DeepSeek ACP session ready" },
            });
            yield* offerRuntimeEvent({
              type: "thread.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              payload: { providerThreadId: startedResult.sessionId },
            });

            return session;
          }).pipe(Effect.scoped);

          return started;
        }).pipe(Effect.scoped),
      );

    const sendTurn: Adapter["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        // A sendTurn while a prompt is in flight steers the running turn.
        const steeringTurnId = ctx.promptsInFlight > 0 ? ctx.activeTurnId : undefined;
        const turnId = steeringTurnId ?? TurnId.make(yield* randomUUIDv4);
        ctx.promptsInFlight += 1;

        return yield* Effect.gen(function* () {
          if (
            input.modelSelection?.model !== undefined &&
            !sameRoute(resolveDshModelRoute(input.modelSelection.model), ctx.modelRoute)
          ) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue:
                "DeepSeek does not support mid-thread model switches. Start a new thread to change model.",
            });
          }
          ctx.activeTurnId = turnId;
          ctx.session = {
            ...ctx.session,
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
          };

          if (steeringTurnId === undefined) {
            yield* offerRuntimeEvent({
              type: "turn.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: { model: dshModelSlugForRoute(ctx.modelRoute) },
            });
          }

          const promptParts: Array<EffectAcpSchema.ContentBlock> = [];
          const rawPrompt = input.input?.trim() ?? "";
          if (rawPrompt) {
            promptParts.push({ type: "text", text: rawPrompt });
          }
          for (const attachment of input.attachments ?? []) {
            // The ACP surface advertises image:false; ProviderService already
            // puts attachment paths in the prompt text, so nothing else rides along.
            if (attachment.type === "image") {
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/prompt",
                detail: `Image attachment '${attachment.id}' is not supported by the DeepSeek ACP surface.`,
              });
            }
          }

          if (promptParts.length === 0 && input.continuation !== true) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text.",
            });
          }

          const result = yield* ctx.acp
            .prompt({
              prompt: [
                ...promptParts,
                {
                  type: "text",
                  text: buildRuntimeInstructions({
                    harness: "DeepSeek",
                    model: dshModelSlugForRoute(ctx.modelRoute),
                  }),
                },
              ],
            })
            .pipe(
              Effect.mapError((error) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", error),
              ),
            );

          yield* ctx.acp.drainEvents;

          const turnRecord = ctx.turns.find((turn) => turn.id === turnId);
          if (turnRecord) {
            turnRecord.items.push({ prompt: promptParts, result });
          } else {
            ctx.turns.push({ id: turnId, items: [{ prompt: promptParts, result }] });
          }
          ctx.session = {
            ...ctx.session,
            activeTurnId: turnId,
            updatedAt: yield* nowIso,
          };

          // Only the last remaining prompt settles the turn.
          if (ctx.promptsInFlight === 1) {
            yield* offerRuntimeEvent({
              type: "turn.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: {
                state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                stopReason: result.stopReason ?? null,
              },
            });
          }

          return {
            threadId: input.threadId,
            turnId,
            resumeCursor: ctx.session.resumeCursor,
          };
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              ctx.promptsInFlight = Math.max(0, ctx.promptsInFlight - 1);
            }),
          ),
        );
      });

    const interruptTurn: Adapter["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        yield* Effect.ignore(
          ctx.acp.cancel.pipe(
            Effect.mapError((error) =>
              mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", error),
            ),
          ),
        );
      });

    const respondToRequest: Adapter["respondToRequest"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToRequest",
          detail:
            "DeepSeek runs full-access: approval answers are not supported. Permissions are auto-allowed by the runtime.",
        }),
      );

    const respondToUserInput: Adapter["respondToUserInput"] = () =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "respondToUserInput",
          detail: "DeepSeek does not support structured user-input questions.",
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
          detail: "DeepSeek ACP sessions do not support provider-side rollback.",
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
          Effect.logError("Failed to emit DeepSeek session shutdown event.", { cause }),
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
