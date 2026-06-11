import { describe, expect, test } from "bun:test"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Logger from "effect/Logger"
import type { AuditableLogger, FieldContext, LogLevel, WideEvent as WideEventData } from "evlog"
import { Evlog, EvlogBridge, EvlogInit, WideEvent } from "./index.js"

type Call = {
  readonly method: string
  readonly args: ReadonlyArray<unknown>
}

const makeLogger = (initialContext: Record<string, unknown> = {}) => {
  const calls: Array<Call> = []
  const context = { ...initialContext }

  const logger = {
    set: (fields: FieldContext) => {
      calls.push({ method: "set", args: [fields] })
      Object.assign(context, fields)
    },
    setLevel: (level: LogLevel) => {
      calls.push({ method: "setLevel", args: [level] })
      context.level = level
    },
    info: (message: string, fields?: FieldContext) => {
      calls.push({ method: "info", args: [message, fields] })
    },
    warn: (message: string, fields?: FieldContext) => {
      calls.push({ method: "warn", args: [message, fields] })
    },
    error: (error: Error | string, fields?: FieldContext) => {
      calls.push({ method: "error", args: [error, fields] })
    },
    getContext: () => {
      calls.push({ method: "getContext", args: [] })
      return context
    },
    emit: (overrides?: FieldContext) => {
      calls.push({ method: "emit", args: [overrides] })
      return { ...context, ...overrides } as WideEventData
    }
  } as unknown as AuditableLogger

  return { calls, context, logger }
}

describe("Evlog.fromLogger", () => {
  test("delegates service methods to the wrapped logger", async () => {
    const { calls, logger } = makeLogger({ initial: true })
    const service = Evlog.fromLogger(logger)

    await Effect.runPromise(Effect.gen(function* () {
      yield* service.set({ user: { id: 1 } })
      yield* service.setLevel("warn")
      yield* service.info("loaded", { count: 2 })
      yield* service.warn("slow")
      yield* service.error("failed")

      const context = yield* service.getContext
      const emitted = yield* service.emit({ done: true })

      expect(context).toEqual({ initial: true, user: { id: 1 }, level: "warn" })
      expect(emitted as Record<string, unknown>).toEqual({
        initial: true,
        user: { id: 1 },
        level: "warn",
        done: true
      })
    }))

    expect(calls.map((call) => call.method)).toEqual([
      "set",
      "setLevel",
      "info",
      "warn",
      "error",
      "getContext",
      "emit"
    ])
    expect(calls[2]?.args).toEqual(["loaded", { count: 2 }])
  })
})

describe("Evlog.finalize", () => {
  test("emits successful exits without changing the event", () => {
    const { calls, logger } = makeLogger({ task: "success" })

    Evlog.finalize(logger, Exit.succeed("ok"))

    expect(calls).toEqual([{ method: "emit", args: [undefined] }])
  })

  test("records the first failure and preserves additional failures", () => {
    const { calls, logger } = makeLogger()
    const first = new Error("primary failure")
    const second = new Error("secondary failure")
    const cause = Cause.combine(Cause.fail(first), Cause.fail(second))

    Evlog.finalize(logger, Exit.failCause(cause))

    expect(calls.map((call) => call.method)).toEqual(["error", "set", "emit"])
    expect(calls[0]?.args[0]).toBe(first)
    expect(calls[1]?.args[0]).toEqual({ error: { additional: ["secondary failure"] } })
  })

  test("marks pure interruption as a warning", () => {
    const { calls, logger } = makeLogger()

    Evlog.finalize(logger, Exit.interrupt())

    expect(calls).toEqual([
      { method: "setLevel", args: ["warn"] },
      { method: "set", args: [{ outcome: "interrupted" }] },
      { method: "emit", args: [undefined] }
    ])
  })
})

describe("Evlog.annotate", () => {
  test("sets fields on the current wide event", async () => {
    const { calls, logger } = makeLogger()

    await Effect.runPromise(
      Evlog.annotate({ requestId: "req_123" }).pipe(
        Effect.provideService(WideEvent, Evlog.fromLogger(logger))
      )
    )

    expect(calls).toEqual([{ method: "set", args: [{ requestId: "req_123" }] }])
  })
})

describe("EvlogBridge", () => {
  test("folds Effect log levels into the current wide event", async () => {
    const { calls, logger } = makeLogger()
    const fallbackMessages: Array<unknown> = []
    const fallback = Logger.make<unknown, void>((options) => {
      fallbackMessages.push(options.message)
    })

    const program = Effect.gen(function* () {
      yield* Effect.logInfo("hello", "world").pipe(Effect.annotateLogs("requestId", "req_123"))
      yield* Effect.logWarning("careful")
      yield* Effect.logError(new Error("boom"))
    }).pipe(
      Effect.provideService(WideEvent, Evlog.fromLogger(logger)),
      Effect.provide(EvlogBridge.layer({ fallback }))
    )

    await Effect.runPromise(program)

    expect(fallbackMessages).toEqual([])
    expect(calls.map((call) => call.method)).toEqual(["info", "warn", "error"])
    expect(calls[0]?.args).toEqual(["hello world", { annotations: { requestId: "req_123" } }])
    expect(calls[1]?.args).toEqual(["careful", undefined])
    expect(calls[2]?.args).toEqual(["boom", undefined])
  })

  test("uses the fallback logger outside a wide event", async () => {
    const fallbackMessages: Array<unknown> = []
    const fallback = Logger.make<unknown, void>((options) => {
      fallbackMessages.push(options.message)
    })

    await Effect.runPromise(
      Effect.logInfo("outside").pipe(Effect.provide(EvlogBridge.layer({ fallback })))
    )

    expect(fallbackMessages).toEqual([["outside"]])
  })
})

describe("EvlogInit.flush", () => {
  test("runs drain flush", async () => {
    let calls = 0

    await Effect.runPromise(EvlogInit.flush({
      flush: async () => {
        calls += 1
      }
    }))

    expect(calls).toBe(1)
  })

  test("swallows drain flush failures", async () => {
    await expect(Effect.runPromise(EvlogInit.flush({
      flush: async () => {
        throw new Error("flush failed")
      }
    }))).resolves.toBeUndefined()
  })
})
