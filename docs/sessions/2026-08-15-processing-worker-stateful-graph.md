---
title: Processing — Worker, Stateful, Graph (and why the actor designs were abandoned)
type: session
status: current
updated: 2026-08-15
tags: [processing, worker, stateful, graph, actor, pipe, backpressure, design]
---

# Worker, Stateful, Graph

The session that replaced the actor line (`v5` … `v7`) with three small types layered on `Processor`. The
conclusion, reached the long way round: **everything derives from `Processor`**, and the actor semantics that
mattered come from parts that already existed.

Code: `modules/common/src/main/scala/homelab/common/processing/{Processor,Worker,Stateful,Graph,Node}.scala`
Specs: `WorkerSpec` (7), `WorkflowSpec` (15) — `common` green at 83.

## The shape that survived

- **`Worker[E, A, B] extends Processor[E, (A, Promise[E, B])]`** — request/reply over an ordinary pipe. Each
  message carries the promise its reply goes to, so `ask` waits and `send` doesn't. `Worker.Parallel` mixes in
  `Processor.Parallel`.
- **`Stateful[E >: AdapterError <: ApplicationError, K, S, I, O] extends Worker[E, I, O]`** — adds only the
  step: `store.get(key).someOrElseZIO(init)` → `next(state, message)` → `set` or `delete`. Fifteen lines.
- **`Graph`** — the one thing that starts processors. `Graph.run(roots)` builds a default graph, expands each
  root through `Node.children` (children first), and races them to the first failure.
- **`Node(val children: List[Processor[…]])`** — ownership, declared at construction.

Everything else is reused: `Pipe`, `KeyedQueue`, `KeyValueStore`, `Bucket`, `Permit`, `Promise`, `Scope`.

## Why the actor designs were abandoned

Three were built and discarded: `v5` (runner owns store + scheduling), `v7` (a `Worker` per entity, handles
passed around), and `v7.Routed` (routing table as actor state). Each worked. The reason none survived is
one property:

> **An entity addressed by a *handle* has a lifetime independent of its key. Every hard problem came from
> that, and none of them can even be stated when entities are addressed by *key*.**

What the handle design forced into existence, and what deleted it:

| forced by handles | gone with keys |
|---|---|
| `Terminated` refusal + the `refusing` guard | a key is always addressable |
| retry-on-refusal, retire, respawn | the next message re-seeds via `init` |
| duplicate-entity-per-key hazard | nothing to duplicate |
| `running: Map[K, ReplyTo]` + `shutdown` sweep | callers are released by their own worker |
| self-retirement race (`flush` before `close`) | no entity object to retire |

`Distributed` (147 lines) vs `Routed` (177) settled a related question empirically: `Routed` decouples
delivery from the caller, and the two `DistributedSpec` scenarios it *cannot* pass are exactly why `v5` had a
`running` map and a `shutdown` — those weren't over-engineering, they were the cost of that decoupling.
`RoutedSpec` carries the two absent scenarios with the reasons, as the record.

## Findings worth keeping

**Two real bugs in the first `Worker` sketch**, both found by reasoning and pinned by negative controls:

1. *Double drain.* `enqueue` reporting "queue was empty" let a second drain fork while the first still held
   the last task. Fixed by putting `draining` in the same cell as the queue — cleared only by the step that
   finds the queue empty. Reintroducing the bug fails 2 of the serialisation tests.
2. *Torn submission.* An interrupt between `enqueue` and the fork left work queued with no drain and
   `draining` claiming otherwise — permanently wedged. Fixed with `ZIO.uninterruptible` over the bookkeeping,
   `.interruptible` on the drain body.

**Reply routing must use `onExit`.** `flatMap(succeed).catchAll(fail)` routes typed failures only; a defect or
an interrupt leaves the caller parked forever. The negative control *times out* rather than failing — that's
the signature of this bug class, and why the specs carry a per-suite timeout.

**Exclusion after taking a message is the wrong shape.** A `KeyLock` around the step burns a parallelism slot
while it waits, so a hot key starves other keys. A key-serialising *pipe* declines to hand out the message at
all, so slots go to keys that can proceed. Hence `Pipe.KeySafe` as a marker on the pipe rather than a lock in
the actor — and the honest note that a lock gives exclusion but never ordering.

**Backpressure has to reach the producer.** Bounding an internal queue just moves the pile upstream; only the
caller can be made to wait. `Permit` (extracted from `KeyedQueue` this session) is the shared mechanism.

## ZIO facts established by reading 2.1.23 sources

- `Scope#fork` registers the child's `close` in the parent **and** registers the parent's remover in the
  child, so closing a child early deregisters it. Long-lived scopes don't accumulate forked children.
- Plain `ZIO.addFinalizer` has no removal — that *does* accumulate, which is what leaked per-entity in `v5`.
- `forkIn(scope)` uses `forkDaemon`: interrupting the forking fiber does not touch the forked one, and its
  interrupt finalizer skips self-interrupt, so a fiber may close the scope it was forked into.
- Forked fibers inherit the interrupt status of the forking region — hence `.interruptible` on drain bodies
  started inside `uninterruptible` blocks.
- `Semaphore` is FIFO (queue of waiters); `TSemaphore` is not (STM retry, waiters race). `Permit.bounded` uses
  the latter — fairness there is an open question.
- `Promise` registers nowhere; a pending promise holds its waiters, an interrupted waiter deregisters itself.
  The leak is never the promise, always the fiber parked on one nobody will complete.

## Open

- No specs for `Stateful`, `Graph`, `Node`. Cheapest valuable ones: state carried between messages and `Done`
  re-seeding; children start before parents; a shared child starts once (fails loudly if `Processor.key` ever
  slips back to a `def`).
- `Graph.run` doesn't report what it started. A missing *root* is obvious (nothing runs); a missing *child* is
  silent and looks like a backlog. A boot log naming the started processors is the cheap fix.
- `Pipe.KeySafe` is a promise, not a proof: it cannot express that the pipe partitions by the same key the
  state is stored under.
- Enforcing registration structurally — `class X(...)(using Graph) extends Processor` registering in the trait
  constructor — was designed and deferred. Costs: `this` escapes during construction, registry outside the
  effect system. Threading `Graph` through `R` was considered and rejected: viral in signatures, and it
  enforces availability rather than registration.
- `incubator/v5` and `v7` are superseded. `v7`'s specs pass and document the pool comparison.
