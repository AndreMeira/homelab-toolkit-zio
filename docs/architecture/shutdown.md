---
title: "Shutdown — draining consumers and processors without losing work"
type: architecture
status: current
updated: 2026-11-01
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
| `drainLocal`, `globalDrain` | `homelab.common.messaging.Consumer` | consumer wrappers built from those, so a processor's intake drains |

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
| `drainLocal` | running `logic` | any consumer, including ones that block until a value arrives (`Consumer.fromQueue`, `PollConsumer`, Core NATS) | interrupting their wait loses nothing |
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

## Tests

- `flow/ShutdownSpec`: sections, `await`, the finalizer ordering, `make` inside an uninterruptible region,
  `gated`, and how `graceful` parks a refusal.
- `messaging/ConsumerDrainSpec`: both drains over in-memory and fake fetching intakes, the decoding order,
  mixed causes, and a `Processor.parallel` started before `await`.
- `NatsSpec`, against a real broker: a message arriving while a `globalDrain` pull is open at shutdown is
  redelivered well before `ackWait`, and a `drainLocal` handler running at shutdown finishes and is acked.

## Open

- **A pull that expires on its own is assumed not to drop a message the server sent just before the expiry.**
  jnats should hand it over; no `NatsSpec` case checks it.
