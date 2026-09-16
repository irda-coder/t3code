/**
 * PiRpcSupport — spawn + JSONL transport for the pi RPC runtime.
 *
 * pi speaks a custom newline-delimited JSON protocol over stdio
 * (`pi --mode rpc`), not ACP, so this transport is bespoke: Effect
 * platform process spawn, strict LF splitting (pi docs forbid generic
 * line readers), id-correlated command promises, an event queue, and
 * kill-on-close lifecycle. It mirrors the shape of `AcpSessionRuntime`
 * (spawn input, send, events, close) with plain Effect streams — no new
 * dependencies.
 *
 * Protocol reference: `PI-RPC-REFERENCE.md` (worktree root), distilled from
 * pi `packages/coding-agent/docs/rpc.md` (v0.85.1).
 *
 * @module provider/pi/PiRpcSupport
 */
import { type PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

export const PI_DRIVER_KIND = ProviderDriverKind.make("pi");
export const PI_RPC_MODE_ARGS = ["--mode", "rpc"] as const;
/** Full-access: launch with approval disabled (adapter auto-answers dialogs). */
export const PI_NO_APPROVE_ARG = "--no-approve";

type PiRpcRuntimePiSettings = Pick<PiSettings, "binaryPath" | "sessionDir">;

export interface PiRpcSpawnInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
}

export function buildPiRpcSpawnInput(
  piSettings: PiRpcRuntimePiSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
  opts?: { readonly provider?: string; readonly model?: string },
): PiRpcSpawnInput {
  const sessionDir = piSettings?.sessionDir?.trim();
  const args: Array<string> = [...PI_RPC_MODE_ARGS, PI_NO_APPROVE_ARG];
  if (opts?.provider?.trim()) {
    args.push("--provider", opts.provider.trim());
  }
  if (opts?.model?.trim()) {
    args.push("--model", opts.model.trim());
  }
  if (sessionDir) {
    args.push("--session-dir", sessionDir);
  }
  return {
    command: piSettings?.binaryPath?.trim() || "pi",
    args,
    cwd,
    env: {
      ...environment,
    },
  };
}

/** Minimal decoded shape of a pi RPC response line. */
export interface PiRpcResponse {
  readonly id?: string;
  readonly command: string;
  readonly success: boolean;
  readonly data?: unknown;
  readonly error?: string;
}

/** Pass-through shape of a pi RPC event line. */
export interface PiRpcEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

export class PiRpcTransportError extends Schema.TaggedError<PiRpcTransportError>()(
  "PiRpcTransportError",
  {
    detail: Schema.String,
  },
) {}

const decodeJsonLine = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

const encodeJsonLine = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/**
 * Strict LF splitter: split records on `\n` only, strip one trailing `\r`.
 * Generic line readers (Node readline) also split on U+2028/U+2029, which
 * are legal inside JSON strings — pi docs forbid them.
 */
export function splitJsonLines(buffer: string): {
  readonly lines: ReadonlyArray<string>;
  readonly rest: string;
} {
  const lines: Array<string> = [];
  let start = 0;
  for (;;) {
    const idx = buffer.indexOf("\n", start);
    if (idx < 0) {
      break;
    }
    const raw = buffer.slice(start, idx);
    lines.push(raw.endsWith("\r") ? raw.slice(0, -1) : raw);
    start = idx + 1;
  }
  return { lines, rest: buffer.slice(start) };
}



export interface PiRpcRuntime {
  /** Send a command; resolves with the correlated `response` line. */
  readonly send: (
    command: Record<string, unknown>,
  ) => Effect.Effect<PiRpcResponse, PiRpcTransportError>;
  /** Live event queue (id-correlated responses never land here). */
  readonly events: Queue.Queue<PiRpcEvent>;
  /** Last stderr chunk, for diagnostics. */
  readonly lastStderr: Effect.Effect<string>;
  /** SIGTERM the process (platform escalates to SIGKILL). */
  readonly close: Effect.Effect<void>;
}

/**
 * Spawn `pi --mode rpc` and attach pumps. The caller's Scope owns the
 * process and all pump fibers: closing it kills the child. Commands sent
 * after exit fail fast with the recorded exit detail.
 */
export function makePiRpcRuntime(input: {
  readonly spawn: PiRpcSpawnInput;
  readonly commandTimeoutMs?: number;
}): Effect.Effect<
  PiRpcRuntime,
  PiRpcTransportError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const commandTimeoutMs = input.commandTimeoutMs ?? 120_000;
  return Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const scope = yield* Effect.scope;
    const spawnCommand = yield* resolveSpawnCommand(input.spawn.command, [...input.spawn.args], {
      env: input.spawn.env,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new PiRpcTransportError({
            detail: `Failed to resolve pi spawn command: ${String(cause)}`,
          }),
      ),
    );
    const child = yield* spawner
      .spawn(
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          cwd: input.spawn.cwd,
          env: input.spawn.env,
          extendEnv: true,
          shell: spawnCommand.shell,
        }),
      )
      .pipe(
        Effect.mapError(
          (cause) => new PiRpcTransportError({ detail: `Failed to spawn pi: ${String(cause)}` }),
        ),
      );

    const stdinQueue = yield* Queue.unbounded<string>();
    const events = yield* Queue.unbounded<PiRpcEvent>();
    const pending = yield* Ref.make(
      new Map<string, Deferred.Deferred<PiRpcResponse, PiRpcTransportError>>(),
    );
    const idCounter = yield* Ref.make(0);
    const exited = yield* Ref.make<PiRpcTransportError | undefined>(undefined);
    const stderrRef = yield* Ref.make("");
    const stdoutBuffer = yield* Ref.make("");

    const failPending = (error: PiRpcTransportError) =>
      Ref.get(pending).pipe(
        Effect.flatMap((current) =>
          Effect.forEach([...current.values()], (deferred) =>
            Deferred.complete(deferred, Effect.fail(error)),
          ),
        ),
        Effect.asVoid,
      );

    const handleLine = (line: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!line.trim()) {
          return;
        }
        const decoded = decodeJsonLine(line);
        if (Option.isNone(decoded) || typeof decoded.value !== "object" || decoded.value === null) {
          yield* Queue.offer(events, {
            type: "parse-error",
            raw: line.slice(0, 500),
          });
          return;
        }
        const msg = decoded.value as Record<string, unknown>;
        if (msg["type"] === "response" && typeof msg["id"] === "string") {
          const current = yield* Ref.get(pending);
          const deferred = current.get(msg["id"]);
          if (deferred) {
            yield* Ref.update(pending, (next) => {
              const copy = new Map(next);
              copy.delete(msg["id"] as string);
              return copy;
            });
            yield* Deferred.complete(deferred, Effect.succeed(msg as unknown as PiRpcResponse));
            return;
          }
        }
        yield* Queue.offer(events, msg as unknown as PiRpcEvent);
      });

    // One long-lived stdin run: the queue never closes before scope close,
    // so stdin stays open across commands for the session lifetime.
    yield* Stream.run(
      Stream.encodeText(Stream.fromQueue(stdinQueue)),
      child.stdin,
    ).pipe(Effect.forkIn(scope));

    yield* child.stdout.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) =>
        Ref.modify(stdoutBuffer, (buffered) => {
          const { lines, rest } = splitJsonLines(buffered + chunk);
          return [lines, rest] as const;
        }).pipe(Effect.flatMap((lines) => Effect.forEach(lines, handleLine, { discard: true }))),
      ),
      Effect.forkIn(scope),
    );

    yield* child.stderr.pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => Ref.set(stderrRef, chunk.slice(-2000))),
      Effect.forkIn(scope),
    );

    // Exit monitor: fail everything waiting once the process is gone.
    yield* child.exitCode.pipe(
      Effect.flatMap((code) =>
        Effect.gen(function* () {
          const stderr = yield* Ref.get(stderrRef);
          const error = new PiRpcTransportError({
            detail: `pi process exited (code=${code}). Stderr: ${stderr.slice(-500)}`,
          });
          yield* Ref.set(exited, error);
          yield* failPending(error);
        }),
      ),
      Effect.ignore,
      Effect.forkIn(scope),
    );

    const send = (
      command: Record<string, unknown>,
    ): Effect.Effect<PiRpcResponse, PiRpcTransportError> =>
      Effect.gen(function* () {
        const gone = yield* Ref.get(exited);
        if (gone) {
          return yield* Effect.fail(gone);
        }
        const deferred = yield* Deferred.make<PiRpcResponse, PiRpcTransportError>();
        const id = yield* Ref.modify(idCounter, (n) => [`t3-pi-${n + 1}`, n + 1] as const);
        yield* Ref.update(pending, (current) => new Map(current).set(id, deferred));
        yield* Queue.offer(stdinQueue, `${encodeJsonLine({ id, ...command })}\n`);
        const result = yield* Deferred.await(deferred).pipe(Effect.timeoutOption(commandTimeoutMs));
        if (Option.isNone(result)) {
          yield* Ref.update(pending, (current) => {
            const copy = new Map(current);
            copy.delete(id);
            return copy;
          });
          return yield* Effect.fail(
            new PiRpcTransportError({
              detail: `pi command timed out: ${String(command["type"] ?? "?")}`,
            }),
          );
        }
        return result.value;
      });

    const close = child
      .kill({ forceKillAfter: "1 second" })
      .pipe(Effect.ignore, Effect.asVoid);

    return { send, events, lastStderr: Ref.get(stderrRef), close };
  });
}
