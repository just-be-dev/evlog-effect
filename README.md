# evlog-effect

Effect **v4** (effect-smol) bindings for [evlog](https://evlog.dev) wide events.

One evlog logger per unit of work, exposed as an Effect service. Context accumulates via `set` as the work proceeds; the wide event is emitted exactly once when the work completes — success, failure, or interruption — with error context extracted from the Effect `Cause`.

Verified against `effect@4.0.0-beta.78` and `evlog@2.19.0`.

## Quick start

```ts
import { Effect } from "effect"
import { Evlog, EvlogBridge, EvlogInit, WideEvent } from "@just-be/evlog-effect"
import type { DrainContext } from "evlog"
import { createAxiomDrain } from "evlog/axiom"
import { createDrainPipeline } from "evlog/pipeline"

// App-level: initLogger on startup, drain.flush() on shutdown
const drain = createDrainPipeline<DrainContext>({ batch: { size: 50 } })(createAxiomDrain())
const AppLive = EvlogInit.layer({
  env: { service: "checkout", environment: "production" },
  drain
})

const checkout = Effect.gen(function* () {
  const log = yield* WideEvent

  const user = yield* fetchUser(userId)
  yield* log.set({ user: { id: user.id, plan: user.plan } })

  const cart = yield* fetchCart(user)
  yield* log.set({ cart: { items: cart.length, total: cart.total } })

  const charge = yield* stripe.charge(cart.total)
  yield* log.set({ stripe: { chargeId: charge.id } })
}).pipe(
  Evlog.withWideEvent({ task: "checkout" })
  // one wide event emitted here — on success, failure, or interrupt
)

Effect.runPromise(checkout.pipe(Effect.provide(AppLive)))
```

## API

### `Evlog` (core)

- **`withWideEvent(initialContext?)`** — wrap an effect as its own unit of work. Creates a fresh evlog logger, provides it as the `WideEvent` service, and emits via `Effect.onExit` when the effect settles. The recommended entry point for jobs, scripts, and handlers.
- **`withRequestWideEvent(options?)`** — same, but uses evlog's `createRequestLogger` to pre-populate `method` / `path` / `requestId`. Pairs with `effect/unstable/http` middleware.
- **`layer(initialContext?)` / `layerRequest(options?)`** — layer forms; the event lives for the layer's scope and emits when it closes. Wrapped in `Layer.fresh` (see v4 notes).
- **`WideEvent`** — the service key (`Context.Service`). Shape: `set`, `setLevel`, `info`, `warn`, `error`, `getContext`, `emit` (escape hatch), and `unsafe` (the raw evlog logger, for audit/fork and sync interop).
- **`annotate(fields)`** — one-liner `WideEvent.use((log) => log.set(fields))`.
- **`acquire` / `fromLogger` / `finalize`** — lower-level building blocks if you need a custom lifecycle (e.g. a typed-fields service key of your own).

### `EvlogBridge`

Folds Effect's built-in logging into the current wide event, so `Effect.logInfo` / `Effect.logWarning` / `Effect.logError` become entries on the single event instead of separate lines:

```ts
program.pipe(Effect.provide(EvlogBridge.layer()))
```

- Inside a wide event: `Error`/`Fatal` → `log.error(...)` (promotes the event level), `Warn` → `log.warn`, everything else → `log.info`. Log annotations and log spans ride along as fields.
- Outside a wide event: falls back to Effect's default console logger (configurable via `fallback`), so standalone logs still appear and nothing is double-emitted.

### `EvlogInit`

- **`layer(config?)`** — calls evlog's global `initLogger(config)` when the layer builds; if `config.drain` exposes `flush` (evlog's `createDrainPipeline` output does), registers `drain.flush()` as a finalizer on the layer's scope so buffered events ship before shutdown.
- **`flush(drain)`** — standalone flush effect.

## Failure semantics

When the unit of work fails, `finalize` walks the (v4 flat) `Cause`:

- The first failure/defect goes through `log.error(...)` — evlog sets the `error` field (`name`, `message`, `stack`) and promotes the event to `level: "error"`.
- Additional concurrent/sequential failures merge in under `error.additional` (evlog's recursive `set` merge).
- Pure interruption (no failures) is recorded as `outcome: "interrupted"` at `warn` level.

evlog seals the logger after emit, so a leaked fiber calling `set` after the event shipped produces an `[evlog]` console warning rather than silent data loss.

## Why the design changed from a v3 sketch

Two Effect v4 changes shaped this binding:

1. **Layers are memoized across `Effect.provide` calls.** In v3, providing a module-level `Layer.scoped(RequestLogger, ...)` per request built a fresh logger each time. In v4 the shared `MemoMap` would silently reuse **one** wide event across every request. That's why `withWideEvent` is the primary API (plain `onExit`, no layer), and the layer forms are wrapped in `Layer.fresh`.

2. **Loggers receive the emitting fiber, and `fiber.context` is readable synchronously.** The v3 bridge needed an `Effect.runSync` hack to reach the request logger from inside a `Logger`. In v4 the bridge is just `Context.getOption(options.fiber.context, WideEvent)` — fully synchronous, no runtime re-entry, and it naturally resolves to whichever wide event the *current fiber* is inside, even across forks.

Other v4 surface differences used here: `Context.Service<Self, Shape>()("id")` replaces `Context.Tag`, `Layer.effect` runs its effect in the layer scope (no separate `Layer.scoped`), `LogLevel` is a string union, and `Cause` is a flat array of `Fail | Die | Interrupt` reasons.

## HTTP middleware sketch (`effect/unstable/http`)

```ts
import { Effect } from "effect"
import { HttpMiddleware, HttpServerRequest } from "effect/unstable/http"
import { Evlog } from "@just-be/evlog-effect"

export const evlogMiddleware = HttpMiddleware.make((app) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    return yield* app.pipe(
      Evlog.withRequestWideEvent({ method: request.method, path: request.url })
    )
  })
)
```

(`unstable/http` may shift between betas — the combinator only needs to wrap the per-request effect, so it adapts to whatever the middleware signature looks like.)

## Not covered (yet)

- **Typed fields** (`FieldContext<T>`): the default `WideEvent` service is `Record`-typed. For compile-time field safety, define your own `Context.Service` key with `WideEventService` specialized to your event type and reuse `acquire` / `finalize`.
- **`log.fork`**: evlog's child-event forking is integration-attached; in Effect you'd typically model background work as `Effect.forkDaemon(work.pipe(Evlog.withWideEvent({ operation, _parentRequestId })))` instead.

## Tests

```sh
bun run check   # typecheck + runtime smoke tests
bun run build   # emit dist/ for publishing
```

The smoke test exercises: success accumulation, typed-error failure capture, the `Effect.log*` bridge (including level promotion and annotations), the layer form, and drain flushing.
