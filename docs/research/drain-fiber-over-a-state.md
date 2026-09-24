---
title: "A drain fiber over a state — the loop that starts because there is work"
type: research
status: draft
updated: 2026-09-21
tags: [concurrency, fiber, state-machine, interruption, scope, zio, api-design, batcher]
---

# A drain fiber over a state

A recurring shape: a component has background work to do, but only while someone is giving it work. The
obvious implementation forks a loop when the component is built and lets it sleep through the idle
stretches. The better one keeps a single `Ref` that is *both* the work list and the fiber's liveness flag —
the submission that finds it empty forks the loop, and the loop stands down when it finds it empty again.

Written out twice already, in two places that look nothing alike, so it is worth naming.

## The shape

```scala
enum State:
  case Idle
  case Working(work: Work)

final class Component(scope: Scope, state: Ref[State]):

  def submit(item: Item): UIO[Unit] =
    ZIO.uninterruptible:
      for
        alone <- register(item)                                    // modify: also elects a starter
        _     <- ZIO.when(alone)(draining.interruptible.forkIn(scope))
      yield ()

  private def draining: UIO[Unit] =
    for
      next <- pending                                              // modify: also stands down
      _    <- proceed(next)
    yield ()

  private def proceed(next: Option[Work]): UIO[Unit] =
    next match
      case None       => ZIO.unit                                  // the loop ends here
      case Some(work) => run(work) *> draining

  private def register(item: Item): UIO[Boolean] =
    state.modify:
      case State.Idle          => true  -> State.Working(Work(item))
      case State.Working(work) => false -> State.Working(work + item)

  private def pending: UIO[Option[Work]] =
    state.modify:
      case State.Working(work) if work.nonEmpty => Some(work) -> State.Working(work)
      case _                                    => None       -> State.Idle

object Component:
  def make: URIO[Scope, Component] =
    for
      scope <- ZIO.scope
      state <- Ref.make[State](State.Idle)
    yield Component(scope, state)
```

Four things are load-bearing. Each closes a specific hole, and the shape is not safe with any of them
removed.

### 1. The stand-down is the same atomic step as the read

`pending` decides *and* publishes: either it takes work out and stays `Working`, or it sets `Idle` and
returns nothing. Splitting it — read, decide, then write `Idle` — opens the window that strands work
forever:

```
loop:      reads the state, sees it empty, decides to exit
submitter:                                                  adds an item, sees Working, does not fork
loop:      writes Idle, exits
                                                            → the item is never processed
```

With one `modify` the two orders are the only ones possible: the loop's modify sees the new item and keeps
going, or it sets `Idle` first and the submitter then sees `Idle` and forks a fresh loop. There is no third
outcome.

The election has to test for `Idle`, though, and not for *emptiness* — they are different questions and only
the first one means "no runner". `Working(∅)` is a loop that is still alive and has not yet reached its
stand-down, so a submitter that reads emptiness as its cue forks a second loop over the same state, and both
then see the new work and process it forever. dkq's heartbeat shipped with exactly that, in a branch that
read `held.isEmpty -> …` where it meant `false -> …`; a reviewer caught it. It is a quiet bug — every
transient emptiness that refills within one iteration adds a fiber, nothing fails, and the duplicated work
is idempotent in both implementations here. A test can see it by counting iterations against a baseline
measured in the same run.

### 2. Register-and-fork refuses interruption

`register` claiming "you are the one who starts it" is a promise the caller then has to keep. Interrupted
between the `modify` and the `forkIn`, the state says `Working` and nothing is working — and because every
later submitter sees `Working`, nothing ever starts it again. The component is wedged for good, not merely
delayed.

Wrapping both in `uninterruptible` costs nothing: neither step blocks. Where the submitter *also* waits for
a result (`Batcher.run` awaits a promise) it is `uninterruptibleMask` with `restore` around the wait alone,
so the caller stays interruptible for the part that actually takes time.

### 3. The forked body is explicitly `.interruptible`

A forked fiber inherits the interrupt status in force at the fork. The fork happens inside the
uninterruptible region from (2), so the loop inherits *uninterruptible* — and a loop that cannot be
interrupted outlives the scope close that was supposed to end it. `.interruptible` on the body puts that
back. This is the detail that is easiest to drop and hardest to see: nothing fails, the process just does
not shut down.

### 4. The scope is captured at construction, entered later

`make` takes `ZIO.scope` and holds it. The fiber is forked into it whenever the first submission arrives,
which keeps two properties at once: **construction starts nothing**, so a component can be built to look at
it, and the fiber is still bounded by the scope that owns the component. `forkScoped` at build time gives
the second without the first.

## What changes between instances

The axis is whether a submitter waits for a result.

- **Nobody waits** (a heartbeat). The loop can fail, skip, or be killed with no one to tell. No finalizer is
  needed, and a failed round is simply left to the next one.
- **Everyone waits** (a batcher). Every submission is a promise someone is blocked on, so the loop must be
  *total*: completed on success, on failure, on defect, and on interruption; plus a scope finalizer that
  interrupts whatever is still queued when the scope closes. Getting that wrong hangs a caller rather than
  losing work, which is worse and quieter.

That is the bulk of the difference between the two implementations below. The state machine is identical.

## Where it is used

- **`flow/batching/Serial`** and **`DeduplicatedSerial`** (this repo). `State.Idle | State.InFlight(queue)`;
  the caller that finds `Idle` forks the drain, which processes FIFO batches until the queue empties and
  returns to `Idle`. Used through `Batcher.serial` / `Batcher.deduplicated`.
- **`distributed-keyed-queue`'s consumer heartbeat** (`client/queue/managed/Heartbeat`).
  `State.Idle | State.Beating(held, interval)`; the claim that finds nothing held forks the beat, and the
  beat stands down when the last claim is settled. It arrived there from the opposite direction — the beat
  was a fiber forked at construction with a configured cadence, which meant a consumer holding nothing kept
  a fiber asleep, and a fresh claim's first renewal waited out whatever interval that fiber had already
  begun sleeping on. Carrying the cadence *in the state* fixed the second problem as a side effect: the
  claim writes its cadence in the same `modify` that starts the beat, so the first sleep is already the
  right one.

## When not to reach for it

When the work is not demand-driven. A poller, a subscription pump, a graph of stages — `PollConsumer`,
`QueueSource`, `Graph` in this repo — have something to do whether or not anyone is calling them, and their
fiber's life genuinely is the scope's. Forking those eagerly is correct, and adding a state machine to
them buys nothing.

The test is whether *idle* is a state the component spends real time in and can detect. If yes, this shape
removes a sleeping fiber per component, makes construction pure, and — where the state carries timing —
makes the first iteration's parameters correct instead of inherited from before the work existed.

## What it does not give

- **No backpressure.** The state grows as fast as submissions arrive; bounding it is a separate concern
  (`Batcher`'s `batchSize` bounds a *batch*, not the queue behind it).
- **No fairness across submitters** beyond whatever ordering the work structure itself has.
- **One loop, not a pool.** Concurrency comes from running several components (`Batcher.distributed`
  shards), not from forking more drains over one state — a second drain over one `Ref` reintroduces every
  race the atomic steps were there to remove.
