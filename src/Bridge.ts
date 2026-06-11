/**
 * Bridge Effect's built-in logging (`Effect.log*`, `Effect.annotateLogs`,
 * `Effect.withLogSpan`) into the current evlog wide event.
 *
 * In v3 this required an `Effect.runSync` hack to read the request logger
 * service from inside a `Logger`. In v4 it's clean: every log event carries
 * the emitting fiber, and `options.fiber.context` is the fiber's live
 * `Context` — so the bridge reads the `WideEvent` service synchronously.
 */
import { Context, Option } from "effect"
import * as Logger from "effect/Logger"
import * as References from "effect/References"
import { WideEvent } from "./WideEvent.js"

const toMessage = (message: unknown): string => {
  if (typeof message === "string") return message
  if (Array.isArray(message)) return message.map(toMessage).join(" ")
  if (message instanceof Error) return message.message
  try {
    return JSON.stringify(message)
  } catch {
    return String(message)
  }
}

/**
 * Make a `Logger` that accumulates log events into the current fiber's
 * wide event when one is present, and falls back to `fallback` (default:
 * Effect's default console logger) when it isn't.
 *
 * Inside a wide event:
 * - `Error`/`Fatal` → `log.error(...)` (promotes the event level)
 * - `Warn`          → `log.warn(...)`
 * - everything else → `log.info(...)`
 *
 * Log annotations and log spans ride along as fields on the captured entry.
 */
export const make = (options?: {
  readonly fallback?: Logger.Logger<unknown, unknown> | undefined
}): Logger.Logger<unknown, void> => {
  const fallback = options?.fallback ?? Logger.defaultLogger
  return Logger.make<unknown, void>((opts) => {
    const wide = Context.getOption(opts.fiber.context, WideEvent)
    if (Option.isNone(wide)) {
      fallback.log(opts)
      return
    }
    const log = wide.value.unsafe
    const message = toMessage(opts.message)

    const annotations = opts.fiber.getRef(References.CurrentLogAnnotations)
    const spans = opts.fiber.getRef(References.CurrentLogSpans)
    const fields: Record<string, unknown> = {}
    if (Object.keys(annotations).length > 0) fields.annotations = annotations
    if (spans.length > 0) {
      fields.spans = Object.fromEntries(
        spans.map(([label, timestamp]) => [label, opts.date.getTime() - timestamp])
      )
    }
    const extra = Object.keys(fields).length > 0 ? fields : undefined

    switch (opts.logLevel) {
      case "Fatal":
      case "Error": {
        const errors = opts.cause.reasons.length > 0 ? opts.cause : undefined
        const primary = errors !== undefined
          ? errors.reasons.flatMap((r) => (r._tag === "Fail" && r.error instanceof Error ? [r.error] : []))[0]
          : undefined
        log.error(primary ?? message, extra)
        break
      }
      case "Warn": {
        log.warn(message, extra)
        break
      }
      case "None":
        break
      default: {
        log.info(message, extra)
      }
    }
  })
}

/**
 * Install the bridge as the active Effect logger.
 *
 * With the default (replace) behavior, `Effect.logInfo` outside a wide
 * event still reaches the console via the fallback logger, while inside
 * `withWideEvent` it's folded into the single wide event — no doubled
 * output.
 *
 * ```ts
 * program.pipe(
 *   Effect.provide(EvlogBridge.layer())
 * )
 * ```
 */
export const layer = (options?: {
  readonly fallback?: Logger.Logger<unknown, unknown> | undefined
  readonly mergeWithExisting?: boolean | undefined
}) =>
  Logger.layer([make(options)], {
    mergeWithExisting: options?.mergeWithExisting ?? false
  })
