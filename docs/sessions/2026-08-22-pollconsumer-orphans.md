---
title: "PollConsumer — the orphan problem, three failed fixes, and why batching moves to the Source"
type: session
status: current
updated: 2026-08-23
tags: [pollconsumer, messaging, interruption, zio, batcher, incubator, design]
---

# 2026-08-22 — PollConsumer: orphans, and the design call that came out of it

Started as "is there a test for consume interruption?" and ended with the conclusion that the three-queue
design is unsound for the guarantee we actually want. One fix landed; three were tried and dropped.

Code: `modules/common/src/main/scala/homelab/common/messaging/PollConsumer.scala`
Specs: `PollConsumerSpec` (13), `PollConsumerTeardownSpec` (4), `PollConsumerConcurrencySpec` (4),
`PollConsumerTeardownStressSpec` (1)

## The requirement, stated mid-session

1. **Outstanding demand equals available workers** — exactly, not "at most".
2. **Every claimed job either reaches a worker or is nacked.** Nothing sits unowned in `supply`.

Everything below is measured against those two.

## What landed

**One mask across `consume`.** `consume` now opens a single `ZIO.uninterruptibleMask`, restores only the
parking on `supply` and `logic`, and passes its restorer to `process`, which no longer opens its own. Before
this, an element could exist in the frames between the take and `process`'s mask — out of every queue, no
verdict filed, invisible to the drain, recovered only on lease expiry.

The trap that makes this non-obvious: `InterruptibilityRestorer` is chosen from the interrupt flag **at mask
creation** (`ZIO.scala:6216-6230`), so a nested mask inside an uninterruptible region hands back
`MakeUninterruptible` — a restorer that does nothing. Leaving `process`'s own mask in place would silently
run `logic` uninterruptibly and make a busy caller un-tearable-down. Mutation-tested: five tests time out.

**Two tests added** to `PollConsumerSpec`: wake-up liveness (a parked consumer resumes when `wakeUp` is
raised — nothing covered it), and *"interrupting one caller records its verdict and leaves the consumer
usable"*, which is the single-caller interruption case as opposed to teardown. Mutation-tested by making the
verdict filing interruptible.

## What was tried and dropped

**(1) Withdraw the token, absorb the orphan.** On interruption, `demand.poll` to take a token back; on the
next `consume`, `supply.poll` first and work any waiting element *without* offering new demand. Both halves
were mutation-tested and worked as designed — and were reverted anyway: they bound and heal the drift but
leave the window open when the caller dies while the claim is in flight, at the cost of two extra code paths
and a weakened teardown test.

**(2) Caller-side compensating consume.** `supply.take.onExit(exit => if exit.isFailure then nack)`, with
`nack = consume(_ => ???)`. **Deadlocks**: `onExit` cleanup runs uninterruptibly, `consume` parks on
`supply.take`, and at teardown no element ever arrives — three tests hang. It also cannot converge, because
the compensating `consume` offers a fresh token and so buys a replacement orphan. And it fires on the
`raceFirst` failure path, i.e. exactly when the fetcher is dead and nothing can arrive.

**(3) Orphan counter on the fetcher.** A `Ref[Int]` the caller increments on interruption; the fetcher
redeems it after `claim`, nacking that many and placing the rest. Non-blocking, converges, and puts the
decision in the right actor — but redemption is gated on `demand.takeBetween`, so with no further callers the
counter is never applied and the orphan sits in `supply` until teardown: precisely the case the requirement
exists for. Also needs atomic redemption (`modify`, not `get`/`set`), belongs in `Channel` rather than
coupling the consumer to the fetcher, and should nack with zero delay since an orphan was never attempted.

## Why none of them work — the structural reason

**A demand token is anonymous.** When the fetcher returns from the store it cannot tell that the caller its
token stood for is gone, and `supply` has no notion of a recipient. At the moment a caller leaves, the
decisive fact — *did my token buy an element?* — is inside the fetcher, mid-claim. No caller-side bookkeeping
can close that, whatever shape it takes.

## The call

**Batching is at the wrong level.** The port was designed batch-first (claim `n`, ack `n`, nack `n`), which is
what forces the claim to be separated from the claimant. Instead:

- `Source` becomes **single-element** `claim` / `ack` / `nack`.
- `consume` becomes claim → run → settle **in one fiber**: the claimant *is* the worker, so orphans cannot
  arise and both requirements hold by construction rather than by accounting.
- Batching becomes the **Source implementation's** business, via the existing `Batcher` (DataLoader-style,
  addressed delivery) — read side and write side both.

`PollConsumer` as it stands is to be **demoted to `incubator`** (it would land beside `pool/v1..v3`). Not
executed yet. Note when doing it: its ~20 tests stop running in CI, and both
`docs/architecture/messaging.md` and the root README describe it as part of `common`.

## The one gap in the replacement, found while checking it

`Batcher` recovers the read side properly — `run(in)` returns *that caller's* element, so delivery is
addressed. But `flow/batching/Serial.fulfil` discards the `Boolean` from `promise.succeed(out)`, so a caller
interrupted mid-flight leaves an element completed into a promise nobody reads: the same orphan, one level
up. The fix is small and well-placed — a `dispose: Out => UIO[Unit]` supplied at construction, invoked when
`succeed` returns false and from `abandon`; for a poll consumer that is `element => source.nack(element)`.
Worth doing on its own merit, independently of PollConsumer's fate.

## Two corrections to claims made in passing

- **`logic` errors are not swallowed.** `process` ends with `done <- exit`, which re-raises; `"a mixed batch
  is split into one ack and one nack"` depends on it. The real, narrower case: if the settler dies while a
  caller awaits its verdict, the store's error replaces `logic`'s.
- **The hand-off window described on `Settler.run` is not where elements are lost.** `Queue.take` explicitly
  defers a taker's interruption when the promise already holds an item (`zio/Queue.scala:261-270`, with a
  comment saying so). The settler's real exposure is `takeBetween`'s **loop** (`zio/Dequeue.scala:88`), which
  accumulates across interruptible `flatMap` boundaries — which fits "one run in three under load" far better
  than a single hand-off would. `PollConsumerTeardownStressSpec`'s header still states the old explanation.

## Where it stands

Uncommitted in `homelab-toolkit-zio`:

- `PollConsumer.scala` — the single-mask fix **plus the abandoned sketches (2) and (3)**, so the suite
  currently hangs. First job tomorrow: strip the sketches, keep the mask fix, get back to green.
- `PollConsumerSpec.scala` — the two added tests.
- `Processor.scala`, `modules/nats/.../stream/Consumer.scala` — unrelated edits.

## Addendum, 2026-08-23 — the cancel queue, and the demotion called off

The demotion did not happen. A fourth queue and a fourth owner closed the gap instead:

- **`cancels`** — unbounded on purpose, because it is offered from a caller's interrupt finalizer where
  blocking would stall the interruption itself.
- **`Canceller`** — takes one debt, takes one element, nacks it with **zero delay** (the element was never
  attempted, so `nackDelay` would penalise unrelated work). It exists because redemption has to be able to
  *block*: a departing caller's finalizer cannot wait for an element that may still be mid-claim, and a fiber
  can. Its take is masked and its nack uninterruptible, so scope close cannot lose an element handed to it.
- `consume` posts the debt on `onInterrupt` (not `onExit`), so the failure path — fetcher already dead —
  does not count debts nobody will pay.
- Registration order is settler → fetcher → canceller, so teardown runs: callers interrupted → canceller
  stopped → fetcher stopped → supply drained → settler closed.

Two tests, both mutation-verified by removing the `onInterrupt`: *"a cancelled caller's element is nacked
even when no other caller ever comes"* (the quiescent case that defeated the orphan counter, asserted inside
the consumer's lifetime) and the old drain test refocused as *"elements claimed for callers that have left
are given back at once"*. That one lost its `nacked.size == 1` assertion: the canceller returns elements one
at a time as they arrive rather than leaving one batched drain call — prompt instead of batched.

`PollConsumer`'s class doc and `docs/architecture/messaging.md` now say plainly that this is the **batched
variant**, that everything in it follows from `Source` being batch-shaped, and that a single-element `Source`
needs none of it. The type is provisional in *naming*, not in behaviour.

**Standing assessment (agreed, 2026-08-23).** Requirement 2 is met; requirement 1 is only *eventually*
consistent — demand exceeds live workers between a cancellation and its redemption. The canceller nacks *an*
element rather than *the* orphan, which is correct by fungibility but means cancel-heavy traffic turns into
claim/nack churn — and a keyed queue over gRPC, where callers are cancelled by design, is precisely that
traffic. The complexity exists to enable batching, which `Batcher` already provides. So: keep the canceller,
but do not build dkq on this variant before the simple one exists, or the choice gets made by inertia.

## Next

1. Single-element `PollConsumer` — claim one, run, settle one, in the caller's own fiber. Point dkq at it.
2. `Batcher`/`Serial.fulfil` disposal hook (the discarded `succeed` boolean), with a test for the
   interrupted-requester case.
3. Let the Postgres `Source` own its batching via `Batcher`.
4. Then decide the naming, with a measurement rather than a preference.
