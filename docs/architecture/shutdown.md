---
title: "Shutdown — draining consumers and processors without losing work"
type: architecture
status: current
updated: 2026-11-11
tags: [shutdown, drain, interruption, consumer, processor, jetstream, scope, flow, messaging]
---

# Shutdown

When an application stops, its scope closes and every fiber in it is interrupted. `Shutdown` lets the work
that must not be cut off finish first: running handlers complete, nothing new is taken, and a message caught
on its way is handed back at once rather than left to its redelivery timeout.

It comes in two layers:

| Layer | Where | What |
|---|---|---|
| `Shutdown` | `homelab.common.flow` | sections of work a shutdown waits for (`graceful`, `await`, `closing`), and the refusal of an effect inside one (`gated`). Knows nothing of consumers. |
| `localDrain`, `globalDrain` | `homelab.common.messaging.Consumer` | consumer wrappers built from those, so a processor's intake drains |

How it came to this shape, including what was tried and rejected, is in
[`../research/coordinated-shutdown.md`](../research/coordinated-shutdown.md).

## Using it

```scala
override def run =
  for
    shutdown <- Shutdown.make
    consumer <- StreamConsumer(connection, "PAYMENTS", "worker", "payments.>", config)
    _        <- Processor.parallel(consumer.globalDrain(shutdown).mapZIO(decode), 8)(charge).forkScoped
    _        <- ZIO.addFinalizer(shutdown.await)   // last: it runs first, before the scope interrupts the work
    _        <- ZIO.never
  yield ()
```

**`await` must be the last finalizer added**, after the processors are started. Finalizers run
last-added-first, so it then runs before anything is interrupted, and waits. The processors fork into a
scope of their own (`Processor.run` takes no `Scope`), so they install nothing in the application's scope
after startup that could run ahead of it.

## Which drain

| Drain | Waits for | Use over | Why |
|---|---|---|---|
| `localDrain` | running `logic` | any consumer, including ones that block until a value arrives (`Consumer.fromQueue`, `PollConsumer`, Core NATS) | interrupting their wait loses nothing |
| `globalDrain` | the whole `consume`, fetch included | consumers making **one bounded fetch per call** (the JetStream `stream.Consumer` and `stream.BatchConsumer`) | a pull still open at shutdown can bring back a message; it is refused and handed back at once |

`globalDrain` over a consumer that blocks until a value arrives holds `await` for as long as its intake stays
idle, possibly forever. That precondition is documented, not typed.

**Decode on top of the drain**, as above. A drain refuses a value where it sits in the chain, so a decoding
`mapZIO` *inside* it runs first: a late value that does not decode then goes through the adapter's failure
policy, like any poison message, instead of being handed back.

## How a refusal works

After `await` has begun:

- **A new section is refused by parking** (`ZIO.never`, interruptible). It never starts and never fails, so a
  fail-fast processor is not torn down; the closing scope ends it.
- **A `gated` effect inside an admitted section** (the `logic` after a `globalDrain` fetch) is refused by
  interrupting the fiber with the instance's own `FiberId`, before the effect starts. The adapter settles that
  as any interrupted handler, handing the message back at once (see
  [messaging: settling a handler that was interrupted](messaging.md#settling-a-handler-that-was-interrupted)).
  The section then ends with that refusal: `graceful` counts it out and parks, outside the count, so `await`
  is not held up. Only the refusal **alone** parks; a refusal joined with a failure or a defect, any other
  interruption, and another instance's refusal reach the caller.

## Guarantees

With `await` added as the last finalizer:

- **Running work completes**: a `logic` running when shutdown begins, under either drain, and any section
  run through `graceful`.
- **Nothing new is taken**: a `consume` started after shutdown began parks before reaching the consumer.
- **No message is lost on its way** (`globalDrain`, decoding on top): an open fetch is waited for, never
  interrupted, and what it brings back is refused and handed back at once.
- **A refusal is never a failure** to the processor running the drain.
- **`await` is bounded** by the longest running `logic`, plus one fetch `expiry` under `globalDrain`.

## Limits, and what to do about them

| Limit | What to do |
|---|---|
| A refused value is lost where the intake cannot hand it back (Core NATS, the in-memory queues) | Expected: those lose an interrupted message anyway, and in-memory contents are lost at exit. Use an acknowledging intake for work that must survive a restart |
| `await` can take up to one JetStream `expiry` (30 s by default) | Set `expiry` within the pod's shutdown grace period. The default is unchanged: the deployment knows its grace period, the library does not |
| `await` has no timeout of its own | The orchestrator's grace period is the bound. Long JetStream handlers keep their message alive with `heartbeat` |
| `logic` must not call `graceful`: it already runs inside a section, and a nested one parks once shutdown has begun, so `await` never returns | Documented on both drains. A re-entrant `graceful` would remove the trap; not done |
| `graceful` protects the section and nothing after it: once the last section returns, the scope may interrupt its fiber before the next line | Put what must happen inside the section. The adapters settle in an uninterruptible step, so an ack lands |
| `consume` may return without running `logic` (an empty fetch, a refusal) | Loops that mean one value per step call `Consumer.once`; the processors do |

## Alternatives considered: ZStream

The question comes back whenever a processor is a stream: couldn't ZStream stop it gracefully instead? It
can't, independently of the rule that keeps `ZStream` off the toolkit's public types. ZStream has two ways
to stop a stream (zio-streams 2.1.23):
- **`interruptWhen(signal)`** "will also interrupt any in-progress element being pulled". That is exactly the
  cut `Shutdown` exists to avoid: a handler stopped halfway, and a pull cut off with a message on its way.
- **`haltWhen(signal)`** is the closer one: "an element in the process of being pulled will not be
  interrupted". It lets an open fetch finish, then stops pulling.

Even `haltWhen` leaves the problem unsolved, for four reasons:

1. **Nobody waits for the halt.** `haltWhen` makes a stream end *eventually*, but the closing scope still
   interrupts the fiber running it. Something has to fire the signal, then wait for every stream, in every
   processor, to finish its in-flight work, all before the scope's interruption. That signal and that
   barrier are `Shutdown`: `await`, run as the first finalizer. ZStream stops one stream. It doesn't wait for
   work across an application.
2. **Settling is the consumer's job, not the stream's.** `consume(logic)` lets the adapter settle around
   `logic`: ack when it succeeds, nak at once when it is interrupted ("interruption is not a failure"). In a
   stream of messages, an element leaves the adapter as soon as it is emitted, and settling it becomes
   downstream's job. A halted or interrupted downstream naks nothing it was holding, unless every element
   carries a bracket of its own.
3. **Buffers hide what's in flight.** Chunked pulls, `buffer` and `mapZIOPar` pull ahead of the processing.
   A halt downstream of them drops the elements already taken without acking or nakking them, so they wait
   out `ackWait`. That's the message on its way again, just in a buffer. `graceful` sections count exactly
   the work in flight, wherever it is.
4. **A late message is refused, not let through.** `globalDrain` refuses a value that arrives after shutdown
   began and hands it back at once, so `await` stays bounded and another instance picks it up. `haltWhen`
   has no notion of refusing and handing back. The value reaches `logic`, or is dropped, depending on where
   the halt sits.

So ZStream can stop a stream, but `Shutdown` is about *finishing* what's in flight, across the whole
application and before the interruption, with each message settled correctly. `graceful` also covers work
that is not a stream at all, a `Worker` or a request handler, so one mechanism serves every kind of work.

## Tests

- `flow/ShutdownSpec`: sections, `await`, the finalizer ordering, `make` inside an uninterruptible region,
  `gated`, and how `graceful` parks a refusal.
- `messaging/ConsumerDrainSpec`: both drains over in-memory and fake fetching intakes, the decoding order,
  mixed causes, and a `Processor.parallel` started before `await`.
- `NatsSpec`, against a real broker: a message arriving while a `globalDrain` pull is open at shutdown is
  redelivered well before `ackWait`, and a `localDrain` handler running at shutdown finishes and is acked.

## Open

- **A pull that expires on its own is assumed not to drop a message the server sent just before the expiry.**
  jnats should hand it over; no `NatsSpec` case checks it.
