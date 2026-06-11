import * as assert from "node:assert/strict"
import { inspect } from "node:util"
import { Effect, Exit } from "effect"
import { createMemoryDrain, readMemoryLogs } from "evlog/memory"
import { createDrainPipeline } from "evlog/pipeline"
import type { DrainContext } from "evlog"
import { Evlog, EvlogBridge, EvlogInit, WideEvent } from "./index.js"

// --- setup: evlog initialized via the Init layer, events captured in memory ---
const pipeline = createDrainPipeline<DrainContext>({ batch: { size: 1, intervalMs: 10 } })
const drain = pipeline(async (batch) => {
  await createMemoryDrain()(batch)
})

const AppLive = EvlogInit.layer({
  env: { service: "smoke-test" },
  pretty: false,
  silent: true,
  drain
})

const events = () => readMemoryLogs()

// --- 1. success path: fields accumulate, one event per unit of work ---
const successCase = Effect.gen(function* () {
  const log = yield* WideEvent
  yield* log.set({ user: { id: 1, plan: "pro" } })
  yield* log.set({ cart: { items: 3, total: 9999 } })
  return "ok"
}).pipe(Evlog.withWideEvent({ task: "checkout" }))

// --- 2. failure path: typed error lands on the wide event at error level ---
class PaymentDeclined extends Error {
  override readonly name = "PaymentDeclined"
}
const failureCase = Effect.gen(function* () {
  const log = yield* WideEvent
  yield* log.set({ user: { id: 2 } })
  return yield* Effect.fail(new PaymentDeclined("insufficient funds"))
}).pipe(Evlog.withWideEvent({ task: "checkout" }))

// --- 3. bridge: Effect.log* folds into the wide event ---
const bridgeCase = Effect.gen(function* () {
  const log = yield* WideEvent
  yield* log.set({ job: "sync" })
  yield* Effect.logInfo("fetched page").pipe(Effect.annotateLogs("page", 1))
  yield* Effect.logWarning("rate limited")
}).pipe(
  Evlog.withWideEvent({ task: "bridge" }),
  Effect.provide(EvlogBridge.layer())
)

// --- 4. layer form: event emits when the layer scope closes ---
const layerCase = Effect.gen(function* () {
  const log = yield* WideEvent
  yield* log.set({ via: "layer" })
}).pipe(Effect.provide(Evlog.layer({ task: "layered" })))

const main = Effect.gen(function* () {
  yield* successCase
  const exit = yield* Effect.exit(failureCase)
  assert.ok(Exit.isFailure(exit), "failure case should fail through")
  yield* bridgeCase
  yield* layerCase
  yield* Effect.promise(() => drain.flush())

  const all = events()
  assert.equal(all.length, 4, `expected 4 wide events, got ${all.length}`)

  const [a, b, c, d] = all as unknown as [Record<string, any>, Record<string, any>, Record<string, any>, Record<string, any>]

  // success
  assert.equal(a.task, "checkout")
  assert.deepEqual(a.user, { id: 1, plan: "pro" })
  assert.deepEqual(a.cart, { items: 3, total: 9999 })
  assert.equal(a.level, "info")

  // failure
  assert.equal(b.level, "error")
  assert.ok(String(b.error?.message ?? "").includes("insufficient funds"), "error message captured")
  assert.deepEqual(b.user, { id: 2 })

  // bridge
  assert.equal(c.task, "bridge")
  assert.equal(c.level, "warn", "logWarning should promote the event level")
  const logsField = inspect(c, { depth: null })
  assert.ok(logsField.includes("fetched page"), "logInfo folded into wide event")
  assert.ok(logsField.includes("rate limited"), "logWarning folded into wide event")

  // layer
  assert.equal(d.task, "layered")
  assert.equal(d.via, "layer")

  console.log("all smoke tests passed ✔")
  console.log(`events captured: ${all.length}`)
}).pipe(Effect.provide(AppLive))

await Effect.runPromise(main as Effect.Effect<void>)
