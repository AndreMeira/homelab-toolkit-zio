---
title: "Coordinated shutdown — draining consumers and processors without losing work"
type: research
status: draft
updated: 2026-11-01
tags: [shutdown, drain, interruption, consumer, processor, jetstream, scope, zio, incubator]
---

# Coordinated shutdown

When an application stops, its scope closes and every fiber in it is interrupted. For a processor that is the
wrong moment to be interrupted: a handler halfway through a message is cut off, and a fetch that is still
open may have a message on its way that nobody will read. This note records how `Shutdown` (in
`incubator/processing/shutdown/`) came to be shaped the way it is, what it guarantees, and what it cannot.

The code is an incubator sketch. Where this note and the code disagree, the code wins; where it and
[`../architecture/messaging.md`](../architecture/messaging.md) disagree, architecture wins.

## The starting point: sections and a drain

The first version (`e39f09b`) has three pieces:

- **`graceful(section)`** runs a section of work that a shutdown waits for. It counts the section in and out
  inside one uninterruptible region, with only the section itself restored, so an interrupted section is
  always counted out.
- **`await`** begins the shutdown: it refuses every section started from then on, and waits for the running
  ones to finish. It belongs in the application as **the last finalizer added to its scope**, so that it runs
  first, before the scope interrupts the work.
- **A refused section parks** (`ZIO.never`, interruptible) instead of failing. The closing scope interrupts it
  like any other waiting fiber, so the adapter settles its message exactly as it settles any interrupted one.
  Failing would tear down a fail-fast processor (`Processor.parallel`) at the first refusal.

On top of that, a consumer wrapper. It started as `gate`, which only stopped new `consume` calls, and became
**`drainLocal`**, which also runs each delivered value through `graceful`:

- a value whose `logic` is running when shutdown begins is waited for, and completes;
- a `consume` started after shutdown began parks before reaching the consumer, so nothing more is pulled;
- a `consume` already waiting inside the consumer that receives a value after shutdown began is refused:
  `logic` never runs, the call parks, and the closing scope interrupts it.

## What interruption means to a consumer

The design rests on a rule the adapters already followed, which is now written on the `Consumer` trait:
**interruption is not a failure.** An adapter that can hand a message back does so at once when `logic` is
interrupted, whatever its failure policy says. Nobody finished the work, so it must neither be dropped nor
wait out a backoff or a lease. Failure policies (`HandlerFailurePolicy`, `nackDelay`) are for work that ran
and failed.

| Adapter | Interrupted `logic` | Typed failure |
|---|---|---|
| `nats.stream.Consumer` / `BatchConsumer` | `nak` at once | `HandlerFailurePolicy` |
| `PollConsumer` | nacked with no delay | nacked after `nackDelay` |
| `nats.core.Consumer` / `BatchConsumer` | lost (no ack; delivered once) | lost |
| `Consumer.fromQueue` / `fromKeyedQueue` | lost (already taken) | lost |

So a refused value is handed back promptly wherever the substrate allows it.

## The gap `drainLocal` leaves: a message on its way

`drainLocal` waits for `logic`, not for the fetch. A JetStream pull is a request the server holds open for
`expiry`. If shutdown begins while a pull is open, the closing scope interrupts the pull, and a message the
server sends for that request can arrive after nobody is reading. That message waits out `ackWait` (30 s by
default) before it is redelivered. The adapter scaladocs already named a narrow form of this ("an interrupt
landing as the pull returns"). The real window is any message dispatched for a pull that is still open.

A second wrapper, **`globalDrain`**, closes it by waiting for the whole `consume` call, fetch included.

### What was tried on the way

1. **`graceful` twice: once around `consume`, once around `logic` inside it.** This deadlocks. Once shutdown
   has begun, the inner section is refused and parks, while the outer one, which `await` is waiting for, can
   never finish. It is the nesting case the class doc warns about.

2. **Refuse by interrupting instead of parking.** Inside the outer section, a value arriving after shutdown
   began is refused with an interruption rather than parked. A throwaway spec against ZIO 2.1.23 confirmed the
   two facts this rests on. The adapter sees a self-interrupt as an interruption (`exit.isInterrupted`), so
   it hands the message back at once. And a self-interrupt is an ordinary, catchable failure with an
   `Interrupt` cause: the fiber carries on.

3. **The catch that makes it work.** Because a self-interrupt is catchable, the next `catchAllCause` up
   catches it. `Processor.parallel`'s spawn step would turn it into the failure of the whole processor. So
   the wrapper **catches its own refusal** once the adapter has settled the message, and parks there,
   outside the section:

   ```scala
   graceful {
     consumer.consume: value =>
       closing.flatMap: stopped =>
         if stopped then ZIO.interruptAs(refusal) else logic(value)
   }.catchSomeCause:
     case cause if cause.isInterruptedOnly && cause.interruptors == Set(refusal) => ZIO.never
   ```

   `refusal` is the id of a fiber forked and interrupted in `Shutdown.make`. A fiber id compares by number,
   start time and fork site together, so an interruption raised by the user's own `logic` is never mistaken
   for a refusal, and passes through untouched.

4. **An idle consume holds `await` forever.** The outer section counts a `consume` that is simply waiting on
   an empty intake. JetStream's `next` used to pull again by itself after an empty pull, so on a quiet stream
   one `consume` never returned, and `Processor.parallel` always keeps one listener inside `consume`.

5. **A grace period was rejected.** Waiting for intake only up to a grace period, then interrupting, does not
   close the window. It moves it: a message published during the grace can still be on its way when the
   grace ends.

6. **One bounded pull per `consume`.** The only safe way to end a pull is to let it finish by itself, with a
   message or empty at `expiry`, and not start another. So the JetStream consumers now make exactly one pull
   (or one fetch) per `consume`, and return without calling `logic` when it comes back empty. Pulling again
   is the caller's loop. `globalDrain` then waits at most one `expiry` per open pull, with no interruption,
   and so no window.

7. **`Consumer.once` for run loops.** A `consume` that may return without delivering broke
   `Processor.parallel`, whose next listener starts only once the current one receives a value. Rather than
   change the spawn step, `Consumer` gained `once`, which calls `consume` until one value has been processed,
   and the processors call it. An empty pull is retried by the same listener.

## Guarantees

With `await` added as the last finalizer of the application's scope:

- **Running work completes.** A value whose `logic` is running when shutdown begins is waited for, under both
  wrappers. So is any section run through `graceful`.
- **Nothing new is taken.** A `consume` started after shutdown began parks before reaching the consumer:
  nothing more is pulled, fetched or claimed.
- **No message is lost on its way** (`globalDrain`). A fetch that is open when shutdown begins is waited for,
  never interrupted. A message it brings back is refused and, on an acknowledging adapter, handed back at once
  rather than after `ackWait`. That holds for every message only when the drain wraps the raw message
  consumer, with decoding on top (`shutdown.globalDrain(rawConsumer).mapZIO(decode)`). Wrapped around a
  decoding consumer (`stream.Consumer.make[A]`), decoding runs first, so a late message that does not
  decode is settled by the failure policy instead, as a poison message is at any other time.
- **A refusal is never a failure.** A refused value parks; a fail-fast processor running the wrapper does not
  see it. Only the wrapper's own refusal is caught; any other interruption, including one raised by `logic`,
  passes through.
- **`await` is bounded** by the longest running `logic` plus, under `globalDrain`, one fetch `expiry`.
- **Refusals settle like interruptions.** Every refused value goes through the adapter's interruption path,
  so what happens to it is exactly what happens to an interrupted one (see the table above).

## What it cannot guarantee, and the workaround

- **A refused value is lost where the substrate cannot hand it back.** Core NATS delivers once and forgets;
  the in-memory queues have already removed a value once it is taken. *Workaround:* none at this level, and
  none needed for Core NATS, which loses an interrupted or failed message anyway. The in-memory queues lose
  their whole contents when the process exits, so a value refused during shutdown is lost either way. Work
  that must survive a restart needs an acknowledging intake (JetStream, `PollConsumer`).

- **`globalDrain` needs one bounded fetch per `consume`.** Over a consumer that blocks until a value arrives
  (`Consumer.fromQueue`, `PollConsumer`'s callers, Core NATS), `await` waits for as long as the intake stays
  idle, which may be forever. *Workaround:* use `drainLocal` for those. They have no in-flight window to
  protect: interrupting a queue `take` loses nothing, and an interrupted `PollConsumer` caller posts a debt
  that the canceller nacks at once.

- **`await` takes as long as `expiry` allows.** The JetStream default `expiry` is 30 s, so a drain can wait
  that long for an open pull. *Workaround:* set `expiry` within the deployment's shutdown grace period
  (Kubernetes `terminationGracePeriodSeconds`), leaving room for the longest `logic`. The default is kept on
  purpose: the deployment knows its grace period, the library does not. The scaladoc on `Config.expiry` says so.

- **`await` has no timeout of its own.** A `logic` that never finishes holds the shutdown until the
  orchestrator kills the process. *Workaround:* the orchestrator's grace period is the real bound, which is
  how `distributed-keyed-queue` already runs (`gracefulShutdownTimeout` left at `Duration.Infinity`). A
  JetStream handler that runs long keeps its message alive with the `heartbeat` setting.

- **`logic` must not call `graceful`.** Under both wrappers `logic` already runs inside a section. A nested
  section parks once shutdown has begun, and `await` never returns. *Workaround:* documented on both wrappers
  and on the class. Making `graceful` re-entrant (a `FiberRef` marking a fiber already inside an admitted
  section) would remove the trap; it was considered and not done.

- **`graceful` protects the section, and nothing after it.** Once the last section returns, `await` returns
  and the scope interrupts the fiber that ran it, possibly before the code after `graceful` has run.
  *Workaround:* put everything that must happen inside the section. The adapters already do: the
  JetStream consumers and `PollConsumer` settle in an uninterruptible step, so a `drainLocal` value's ack
  still lands after its section has returned. The finalizer-ordering spec hung for exactly this reason: it
  recorded its result after the section.

- **The ordering is the application's job.** `await` only drains if it runs before the scope interrupts the
  work, which means it must be the last finalizer added: start the processors, then add `await`.
  *Workaround:* none in code; a spec covers the ordering.

  That rule only holds if nothing keeps installing finalizers after startup. `Processor.parallel` used to:
  it forked every listener into the caller's scope, so each listener's finalizer landed after `await`'s and
  ran first, cutting its handler before `await` could wait. A probe lost both running handlers in 20 of 20
  runs. `parallel` now runs its listeners in a scope of its own, and `Processor.run` no longer takes the
  caller's `Scope` at all, so a processor installs nothing in it after it starts.

- **`consume` may never run `logic`.** This is now part of the `Consumer` contract, and the cost of
  single-pull fetching. A caller must not count calls as deliveries. *Workaround:* `Consumer.once` for a loop
  that means one value per step; the processors use it.

## A hang in the spec, and what it showed

The spec "await added as the last finalizer…" hung three times, each on the first run after a sizeable
compile, and could not be reproduced in about 3,000 warm runs. Two races were behind it:

- **The app could be interrupted before its finalizer was registered**, so the scope closed with no shutdown
  at all. The spec now waits for a `ready` signal first.
- **The result was recorded outside the section.** A fiber dump from `TestAspect.diagnose` showed the test
  waiting on an outcome the section's fiber never wrote: the section returned, `await` returned, and the
  scope interrupted that fiber before it reached the `outcome.succeed` after `graceful`. A 5 ms gap after
  the section lost the outcome in 20 of 20 runs; recording it inside the section kept it in 20 of 20. A cold
  JVM is what made the gap wide enough to hit.

The second race is not specific to the test; it is the "nothing after the section" limitation above.

## Open

- **A pull that expires on its own is assumed not to drop a message sent just before the expiry.** jnats
  should hand it over, but no `NatsSpec` case checks this yet.
