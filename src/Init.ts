/**
 * App-level lifecycle: `initLogger` on layer construction, `drain.flush()`
 * when the layer's scope closes (process shutdown for `Layer.launch` /
 * `runMain`-style programs).
 */
import { Effect } from "effect"
import * as Layer from "effect/Layer"
import type { LoggerConfig } from "evlog"
import { initLogger } from "evlog"

/** Anything with an async `flush` — matches evlog's `PipelineDrainFn`. */
export interface Flushable {
  readonly flush: () => Promise<void>
}

const hasFlush = (u: unknown): u is Flushable =>
  (typeof u === "function" || (typeof u === "object" && u !== null)) &&
  typeof (u as { flush?: unknown }).flush === "function"

/** Flush a drain pipeline, swallowing flush errors (shutdown path). */
export const flush = (drain: Flushable): Effect.Effect<void> =>
  Effect.promise(async () => {
    try {
      await drain.flush()
    } catch {
      // Drains already retry internally; a failed final flush should not
      // crash shutdown.
    }
  })

/**
 * Initialize evlog globally and, if the configured drain exposes `flush`
 * (evlog's `createDrainPipeline` does), flush buffered events when the
 * layer's scope closes.
 *
 * ```ts
 * import type { DrainContext } from "evlog"
 * import { createAxiomDrain } from "evlog/axiom"
 * import { createDrainPipeline } from "evlog/pipeline"
 *
 * const drain = createDrainPipeline<DrainContext>({ batch: { size: 50 } })(
 *   createAxiomDrain()
 * )
 *
 * const EvlogLive = EvlogInit.layer({
 *   env: { service: "checkout", environment: "production" },
 *   drain
 * })
 * ```
 */
export const layer = (config?: LoggerConfig): Layer.Layer<never> =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      initLogger(config)
      const drain = (config as { drain?: unknown } | undefined)?.drain
      if (hasFlush(drain)) {
        yield* Effect.addFinalizer(() => flush(drain))
      }
    })
  )
