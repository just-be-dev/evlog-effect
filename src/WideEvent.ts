/**
 * Effect v4 bindings for evlog wide events.
 *
 * The core idea: one evlog logger instance per unit of work (request, job,
 * script run), exposed as an Effect service. Context accumulates via `set`
 * during the work; the wide event is emitted exactly once when the unit of
 * work completes — including on failure and interruption, with error context
 * extracted from the Effect `Cause`.
 */
import type { Exit, Scope } from "effect"
import { Context, Effect } from "effect"
import * as Layer from "effect/Layer"
import type { AuditableLogger, FieldContext, LogLevel as EvlogLevel, RequestLoggerOptions, WideEvent as WideEventData } from "evlog"
import { createLogger, createRequestLogger } from "evlog"

/**
 * The shape of the wide-event service. All methods are thin effectful
 * wrappers over a single mutable evlog logger instance scoped to the
 * current unit of work.
 */
export interface WideEventService {
  /**
   * Merge fields into the wide event. Plain objects merge recursively,
   * arrays concatenate (evlog semantics).
   */
  readonly set: (fields: FieldContext) => Effect.Effect<void>
  /**
   * Promote the event level without touching the `error` field.
   * Wins over levels derived from `.error()` / `.warn()`.
   */
  readonly setLevel: (level: EvlogLevel) => Effect.Effect<void>
  /** Capture an informational message inside the wide event. */
  readonly info: (message: string, fields?: FieldContext) => Effect.Effect<void>
  /** Capture a warning inside the wide event and promote the level. */
  readonly warn: (message: string, fields?: FieldContext) => Effect.Effect<void>
  /** Capture an error inside the wide event and promote the level. */
  readonly error: (error: Error | string, fields?: FieldContext) => Effect.Effect<void>
  /** Read the accumulated context so far. */
  readonly getContext: Effect.Effect<Record<string, unknown>>
  /**
   * Manually emit and seal the wide event. Normally you never call this —
   * the scope/`onExit` finalizer emits for you. Escape hatch for advanced
   * lifecycles. Returns the emitted event, or `null` if sampled out.
   */
  readonly emit: (overrides?: FieldContext) => Effect.Effect<WideEventData | null>
  /**
   * The raw evlog logger. For synchronous interop (e.g. the Effect `Logger`
   * bridge) and evlog APIs not wrapped here (audit, fork).
   */
  readonly unsafe: AuditableLogger
}

/**
 * Service key for the wide event of the current unit of work.
 *
 * v4 note: `Context.Service<Self, Shape>()("id")` replaces v3's
 * `Context.Tag("id")<Self, Shape>()`.
 */
export class WideEvent extends Context.Service<WideEvent, WideEventService>()(
  "evlog-effect/WideEvent"
) {}

/** Wrap a raw evlog logger instance as a `WideEventService`. */
export const fromLogger = (log: AuditableLogger): WideEventService => ({
  set: (fields) => Effect.sync(() => log.set(fields)),
  setLevel: (level) => Effect.sync(() => log.setLevel(level)),
  info: (message, fields) => Effect.sync(() => log.info(message, fields)),
  warn: (message, fields) => Effect.sync(() => log.warn(message, fields)),
  error: (error, fields) => Effect.sync(() => log.error(error, fields)),
  getContext: Effect.sync(() => log.getContext()),
  emit: (overrides) => Effect.sync(() => log.emit(overrides)),
  unsafe: log
})

const toError = (value: unknown): Error =>
  value instanceof Error ? value : new Error(typeof value === "string" ? value : String(value))

/**
 * Record the outcome of a unit of work on the wide event, then emit it.
 *
 * - Failure: first error goes through `log.error` (sets `error` field +
 *   promotes level); additional concurrent/sequential failures merge in
 *   under `error.additional`.
 * - Interruption (with no failures): marked `outcome: "interrupted"` at
 *   `warn` level.
 * - Success: emitted as-is.
 *
 * evlog seals the logger after `emit`, so late `set` calls from leaked
 * fibers surface as `[evlog]` console warnings rather than silent loss.
 */
export const finalize = (log: AuditableLogger, exit: Exit.Exit<unknown, unknown>): void => {
  if (exit._tag === "Failure") {
    // v4 note: Cause is flat — an array of Fail | Die | Interrupt reasons.
    const errors = exit.cause.reasons.flatMap((reason) => {
      switch (reason._tag) {
        case "Fail":
          return [toError(reason.error)]
        case "Die":
          return [toError(reason.defect)]
        case "Interrupt":
          return []
      }
    })
    const first = errors[0]
    if (first !== undefined) {
      log.error(first)
      if (errors.length > 1) {
        log.set({
          error: { additional: errors.slice(1).map((e) => e.message) }
        })
      }
    } else if (exit.cause.reasons.some((r) => r._tag === "Interrupt")) {
      log.setLevel("warn")
      log.set({ outcome: "interrupted" })
    }
  }
  log.emit()
}

/**
 * Acquire a wide event bound to the current `Scope`: the event is emitted
 * when the scope closes, with the scope's exit recorded on it.
 */
export const acquire = (
  make: () => AuditableLogger
): Effect.Effect<WideEventService, never, Scope.Scope> =>
  Effect.gen(function* () {
    const log = make()
    yield* Effect.addFinalizer((exit) => Effect.sync(() => finalize(log, exit)))
    return fromLogger(log)
  })

const provideAndEmit = (make: () => AuditableLogger) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, E, Exclude<R, WideEvent>> =>
    Effect.suspend(() => {
      const log = make()
      return self.pipe(
        Effect.provideService(WideEvent, fromLogger(log)),
        Effect.onExit((exit) => Effect.sync(() => finalize(log, exit)))
      )
    })

/**
 * Run an effect as its own unit of work: a fresh wide event is created,
 * provided as the `WideEvent` service, and emitted when the effect
 * succeeds, fails, or is interrupted.
 *
 * This is the Effect-native equivalent of evlog's framework middleware,
 * and the recommended entry point for jobs, scripts, and handlers:
 *
 * ```ts
 * const job = Effect.gen(function* () {
 *   const log = yield* WideEvent
 *   yield* log.set({ found: users.length })
 *   // ...
 *   yield* log.set({ migrated, status: "complete" })
 * })
 *
 * await Effect.runPromise(job.pipe(withWideEvent({ task: "user-migration" })))
 * // one wide event emitted, success or failure
 * ```
 *
 * Deliberately implemented with `onExit` rather than a `Layer`, so each
 * invocation gets a fresh logger — see the note on `layer` about v4's
 * cross-provide layer memoization.
 */
export const withWideEvent = (initialContext?: Record<string, unknown>) =>
  provideAndEmit(() => createLogger(initialContext))

/**
 * Like `withWideEvent`, but pre-populates HTTP fields (`method`, `path`,
 * `requestId`) via evlog's `createRequestLogger`. Use inside HTTP server
 * middleware:
 *
 * ```ts
 * import { HttpMiddleware, HttpServerRequest } from "effect/unstable/http"
 *
 * const evlogMiddleware = HttpMiddleware.make((app) =>
 *   Effect.gen(function* () {
 *     const request = yield* HttpServerRequest.HttpServerRequest
 *     return yield* app.pipe(
 *       withRequestWideEvent({ method: request.method, path: request.url })
 *     )
 *   })
 * )
 * ```
 */
export const withRequestWideEvent = (options?: RequestLoggerOptions) =>
  provideAndEmit(() => createRequestLogger(options))

/**
 * Layer form: the wide event lives for the lifetime of the layer's scope
 * and is emitted when that scope closes. Useful when a whole layered
 * program *is* the unit of work (a script, a CLI invocation).
 *
 * v4 caveat: layers are memoized **across** `Effect.provide` calls in v4.
 * A module-level `const Live = layer()` provided around two different
 * requests would share one wide event. `Layer.fresh` is applied here so
 * each composition gets its own event — but for per-request use, prefer
 * `withWideEvent` / `withRequestWideEvent`.
 */
export const layer = (initialContext?: Record<string, unknown>): Layer.Layer<WideEvent> =>
  Layer.fresh(Layer.effect(WideEvent, acquire(() => createLogger(initialContext))))

/** Layer form of `withRequestWideEvent`. Same memoization caveat as `layer`. */
export const layerRequest = (options?: RequestLoggerOptions): Layer.Layer<WideEvent> =>
  Layer.fresh(Layer.effect(WideEvent, acquire(() => createRequestLogger(options))))

/**
 * Convenience accessors: `set`/`info`/`warn`/`error` against the current
 * wide event without yielding the service first.
 *
 * ```ts
 * yield* annotate({ cart: { items: 3, total: 9999 } })
 * ```
 */
export const annotate = (fields: FieldContext): Effect.Effect<void, never, WideEvent> =>
  WideEvent.use((log) => log.set(fields))
