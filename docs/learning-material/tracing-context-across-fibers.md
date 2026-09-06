---
title: Why OtelMonitor adopts the ambient span, and why that is not configurable
type: learning-material
status: current
updated: 2026-09-06
tags: [opentelemetry, zio-telemetry, tracing, java-agent, fibers, context-propagation, homelab-telemetry]
---

# Why `OtelMonitor` adopts the ambient span, and why that is not configurable

`OtelMonitor` uses zio-telemetry's fiber-local context storage (`OpenTelemetry.contextZIO`) and, on the
first span a fiber opens, copies in whatever span the OpenTelemetry Java agent has made current. That second
half looks redundant — surely picking the right storage is the whole job — and it is not. This is the
reasoning, and the measurements behind it, so nobody removes the adoption believing it to be belt and
braces.

## Two places to keep "the current span"

A trace is a parent chain, and something has to know what the current parent is. There are two mechanisms in
play and they do not know about each other:

| | where the current span lives | who writes it |
|---|---|---|
| OpenTelemetry Java agent | a `ThreadLocal` (`Context.current()`) | the agent, around instrumented calls |
| zio-telemetry, by default | a `FiberRef` | `Tracing.span` and friends |

zio-telemetry exposes the choice as a layer: `OpenTelemetry.contextJVM` reads the thread-local,
`OpenTelemetry.contextZIO` reads the `FiberRef`.

## Why the thread-local option is a trap

`contextJVM` is the tempting pick, because it is the only one that makes manual spans nest under agent spans
out of the box — the agent's server span is in the thread-local, so a manual span created on that thread
finds it and becomes its child. One connected trace, no code.

It breaks on any code path that parks. A thread-local does not survive a fiber suspending and resuming on
another worker, so a span opened *after* a wait sees whatever context that worker happens to hold — usually
nothing — and starts a trace of its own.

This is not theoretical. In `distributed-keyed-queue`, whose claim operation waits for a readiness token
before looking at Redis, we counted the parent of every span over 100 traces per operation:

| span | parents observed |
|---|---|
| `RedisQueueStore.attempt` (opened after a wait) | **99 root**, 1 correctly parented |
| `RedisQueueStore.settle` (never waits) | 100 correctly parented |
| `RedisQueueStore.enqueue` (never waits) | 100 correctly parented |

The single correctly-parented `attempt` was a call that found work immediately and therefore never parked.
Parking is the whole of the difference.

Reading `ContextStorage.Native` shows it is worse than merely lossy:

```scala
override def set(context: Context): UIO[Unit] =
  ZIO.succeed(context.makeCurrent()).unit          // the Scope that would restore is discarded

override def locally[R, E, A](context: Context)(zio: ZIO[R, E, A]) =
  ZIO.acquireReleaseWith(get <* set(context))(set)(_ => zio)
```

`makeCurrent()` returns an `AutoCloseable` whose only job is restoring the previous value, and it is thrown
away. `locally`'s acquire, body and release are three separate ZIO steps that can each land on a different
thread, so the "restore" writes the old context onto a thread that never held the new one, and leaves the
new one on threads that did. On a fiber runtime that is stomping, not propagation. zio-telemetry says as
much, documenting the layer as making sense *only* under an agent.

## Why the fiber-local option alone is not enough

Switching to `contextZIO` fixes propagation between our own spans, and nothing else — because nothing in
zio-telemetry ever reads `Context.current()`. `Tracing.span` builds its span with an explicit parent:

```scala
tracer.spanBuilder(spanName).setParent(parentCtx)   // parentCtx comes from ctxStorage.get
```

The `FiberRef` is initialised to `Context.root()`. An explicit `setParent` with a span-less context makes
the SDK start a **root** span — the fallback to the ambient context only happens when `setParent` is never
called at all. So the first span of every request becomes a trace root, and the agent's server span sits in
a trace by itself.

Measured, same request, a dequeue that parks for four seconds:

```
contextZIO alone — two traces

  trace A:  QueueService.dequeue (4015ms)
                RedisQueueStore.claim
                    RedisQueueStore.attempt          <- both attempts nest correctly...
                    RedisQueueStore.attempt

  trace B:  homelab.keyedqueue.v1.KeyedQueue/Dequeue (4027ms)
                EVALSHA                              <- ...but the agent's span is on its own
```

```
contextZIO + adoption — one trace

  homelab.keyedqueue.v1.KeyedQueue/Dequeue (4022ms)
      QueueService.dequeue
          RedisQueueStore.claim
              RedisQueueStore.attempt
              RedisQueueStore.attempt                <- still nested, after the park
      EVALSHA
```

So the two mechanisms each solve half. The `FiberRef` is the only one that survives a park; the thread-local
is the only one that knows about the agent. Neither alone produces a correct trace.

## The adoption, and its one condition

`OtelMonitor.adopting` runs an operation under the ambient span, but **only on a fiber that carries no span
of its own**:

```scala
storage.get.flatMap: carried =>
  if recorded(carried) then observed
  else
    ZIO.succeed(Context.current()).flatMap: ambient =>
      if recorded(ambient) then storage.locally(ambient)(observed) else observed
```

Three things about that are load-bearing:

- **The condition picks out the inbound edge.** A fiber that has not opened a span yet is at the start of a
  request, still running on the thread the agent made its server span current on. Every deeper operation
  already has a span in the `FiberRef`, is left alone, and so keeps nesting across as many parks as it
  likes. Without the condition, every operation would re-read the thread-local and a woken fiber would
  attach itself to whatever unrelated request last used that worker.
- **The read happens inside the effect, not where it is described.** An operation is described wherever the
  layer graph was built and *run* on a worker; only the worker has the agent's context.
- **A root context is not adopted.** `recorded` checks the span context is valid, so a background fiber on a
  thread holding nothing stays a root rather than adopting an empty context.

## What it deliberately does not fix

Spans the *agent* opens — `EVALSHA`, JDBC, an outbound HTTP call — still read the thread-local, which does
not hold our spans. A client call made after a park is therefore its own trace rather than a child of the
operation that made it.

Fixing that means pushing our context back onto the thread around every interop call, which reintroduces
exactly the failure mode above. The trade taken instead: let those spans go, and rely on the operation span
around them, which measures the same call. In the trace above, `EVALSHA` lands as a sibling of
`QueueService.dequeue` rather than under `attempt` — inside the right trace, at the wrong depth.

## Why the storage is not a parameter

`make` takes the `api.OpenTelemetry` — which SDK is a genuine deployment question, and a test can pass its
own — but not the `ContextStorage`, and `components` is private. Offering the choice would be offering a
setting whose wrong value produces no error, no warning, and a trace that looks plausible until someone
counts parents. The failure is silent and shows up only on operations that wait, which are the ones most
worth tracing. So the toolkit settles it: `contextZIO`, plus adoption, always.

One consequence worth knowing when reading `OtelMonitor`: the layer value the tracer and meter are built
from is a single `val` referenced three times, because a layer graph memoises by reference. Calling
`OpenTelemetry.contextZIO` at each use site would build three separate `FiberRef`s, and a span written to
one would be invisible to the others — the same broken traces, from a different cause.
