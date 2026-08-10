---
title: ActorWorker design review — promotion + known flaws
type: session
status: current
updated: 2026-08-10
tags: [actor, processing, worker, design-review, deferred]
---

# ActorWorker design review

Snapshot of the `ActorWorker` design after promoting it to `common`, plus the flaws we found and
consciously deferred. Read the **Deferred work** section first — that's the reminder list.

Code: `modules/common/src/main/scala/homelab/common/processing/ActorWorker.scala`
Spec: `modules/inmemory/src/test/scala/homelab/inmemory/processing/ActorWorkerSpec.scala` (7 tests, green)

## What it is

A keyed, stateful actor built on the `Worker` node hierarchy.

- `ActorWorker[E, I, S, O]` — single actor, a `Worker.PerItem` (serial drain, `Unit` key).
- `ActorWorker.Pool[E, I, K, S, O]` — keyed pool, a `Worker.Parallel` over a **key-safe** `Pipe.fromKeyedQueue`,
  so `parallelism` concurrent `process` runs land on distinct keys and never race within one. Private
  constructor + `make` make that unforgeable.
- Out-of-band replies: the mailbox carries `Envelope = (I, Option[Promise[E, O]])`; `process` routes the
  transition's `O` to the promise — so a **failed step fails the `ask`, not the drain loop**.
- `Self` = the pool inbox (`send` + `pipeToSelf`, both non-blocking; deliberately no `ask` → no self-deadlock).
- `Logic`: `initial(input): IO[E, S]` and `next(self): (I, S) => IO[E, Step[S, O]]` (the transition is built
  once per entity; `Step = Continue(state, reply) | Passivate(reply)`).
- Persistence today: **stateless reload-per-message** against a `KeyValueStore` (get → step → set/delete),
  race-free via the `KeyedQueue`, crash-safe by default.

## What was done this session

- Promoted `ActorWorker` (single + `Pool`) from the incubator to `homelab.common.processing`, names kept.
- Changed `Logic`: `initial` is now effectful (`IO[E, S]`); `next(self)` **returns** the transition and is
  invoked once (bind `self` once, reuse per message).
- Added `Pipe.fromQueue` / `Pipe.fromKeyedQueue` (in `common`) and `Pool.withParallelism` (+ optional
  `parallelism`, default 1 / `maxBuffer`).
- **Deleted `StateMachine`** (+ its spec + the dead orphan `common/processing/Actor.scala`) — an actor that
  never passivates subsumes it. Incubator `Actor` sketches were left as history (their `[[StateMachine]]`
  scaladoc links are dangling but harmless).

## Design review — the points

Ranked by how load-bearing they are. **Point 3 is the main flaw** (it's the only one baked into the
contract; 1 and 2 sit behind it).

### 3. Single reply type `O` — MAIN FLAW (deferred)

Every message replies one `O`. A rich protocol (`Get → Int`, `GetName → String`) must collapse into a
`Response` sum type and downcast at the call site — a real loss of call-site type safety. It's the main flaw
because it's in the **type signature** (`ActorWorker[E, I, S, O]` and `Logic`), so it propagates into every
protocol built on this foundation, and it's the costliest to change later (breaking).

It is the deliberate **price of out-of-band replies** (which bought framework-routed failure delivery). Akka
Typed made the opposite trade (in-band `ActorRef[Reply]`, per-message types, no failure routing).

Fix (if/when needed): a **GADT request type** — `enum Request[A]: case Get extends Request[Int] …`, `next`
polymorphic in `A`, envelope packs `(Request[A], Promise[E, A])` existentially, `process` routes `A` to its
promise. Gives `ask(Get): IO[E, Int]` statically **with** out-of-band routing. Blast radius is **contained**:
only the reply-typing layer (`I`, `O`, `Step`, envelope, `ask`, `process`) changes — the key-safe pipe,
`Self`, `pipeToSelf`, passivation, and persistence all survive. Leaving it now creates no debt; the seam is
clean. Higher-kinded, non-trivial, so deferred.

### 1. `Worker` inheritance leaks a wide API (partly actionable)

Because the actor *is-a* `Worker`/`Through`, its public surface includes `process`, `input`, `output`, `run`.
Real traps: `process` (calling it directly runs a transition outside the queue → races the drain) and
`input`'s **consume** side (draining it steals messages). `send`/`output` are just noise — `send` only
enqueues (== `fire`), it does **not** bypass serialization (I was wrong about that).

Plan:
- **Remove `Worker.ask`** — the inherited `ask[A2](factory)` overloads `ActorWorker.ask` and lets you build a
  non-replying envelope (→ hang). `ActorWorker.ask(message): O` is the one safe door.
- **Make `process` `protected`** in the base (only `run` calls it).
- **`run` stays public** — it's the app-level "serve" verb (`Node.runAll(nodes*)`). No auto-start:
  construction is inert, running is a separate phase (see rationale). This also dissolves the earlier
  "make-then-forget-run footgun" — it's just centralized wiring, like registering routes.
- **`input`/`output`**: can't be narrowed on the override (public→protected is illegal). They're public on
  `Through` *for topology wiring*. Decide by checking **who reads them outside the `processing` package**: if
  nobody, make them `private[processing]`; if a composition root wires nodes externally, they must stay public
  and only composition hides them. → check before assuming they can be hidden.
- Open: whether `Worker` should extend `Through` at all. It was mostly declared for the actor; "Parallel for
  free" is really just `Processor.parallel(input, n)(process)` — a *free function*. Keeping `Node` and dropping
  `Through` (implementing `run` via `Processor.parallel`) gets the parallel drain without inheriting
  `input`/`output`. Polishing — deferred.

### 2. Reload-per-message persistence (fixable behind the contract)

Every message does `store.get(k)` then `store.set(k, …)` — two round-trips per message (Redis/Postgres). Not
a bug: it's a legitimate **durable-actor** flavor (Orleans-lineage), crash-safe by default, race-free via the
`KeyedQueue`, zero cache machinery. For the **agentic** target it's actually well-suited — a step is an LLM
call, so store I/O is noise, and per-step durability is the *point* (never re-run an expensive/paid call after
a crash). High-throughput → use a batch consumer instead.

Direction (the actor does **not** change): a **`CachedKeyValueStore` decorator** — residency becomes a
property of the store. Evict-and-reload-on-miss preserves correctness. It also subsumes write-elision (a
write-back cache coalesces; a value-check skips dirtying on unchanged), which is **why a `Step.Reply` case
should not exist** — "don't write unchanged state" is a store concern, not domain vocabulary.

- **Write-through** (agentic): durable per step, saves reads. Redis basically free.
- **Write-back / batch** (consumers): coalesce + flush (Postgres-shaped; the `Batcher` earns its keep).
- **Passivate** (explicit, `Logic`): delete state, permanent. **Evict** (implicit, cache): drop from memory,
  keep durable, reload on touch. Distinct verbs — keep both.
- Caveat: a per-process cache is only correct while **one process owns each key** (same assumption the
  in-memory `KeyedQueue` pool already makes; distributed → sticky/sharded routing keeps both sound).

### Minor

- `pipeToSelf` is unbounded (a flood forks unbounded child scopes). Add a `Semaphore(maxPipes)` cap if it
  ever matters. Note this is *only* a concurrency cap — the lifecycle/leak concern is already handled (see
  the 2026-08-10 update).
- Deleting `StateMachine` means the *simple* "serialize keyed transitions" case now pays full actor ceremony
  (`make` + `runAll` + envelope) where `StateMachine.distributed` was one call. Accepted.

## Parked feature: crash-recovery replay (not event sourcing)

State stays the source of truth; a write-ahead log lets a crash resume. Lighter than event sourcing:
`KVStore[K, I]` fits (snapshot-per-message → ≤1 pending event), no pure replay handler (replay through
`next`). Two things to get right: replay **re-fires side effects** (at-least-once; transitions must tolerate
it — otherwise you need the pure handler = event sourcing), and the **commit gap** (crash between set-state
and clear-event double-applies unless state+event are one transaction or state carries a version). Recovery
has a startup step (scan pending, re-enqueue). Seam: the same `Persistence` port. Owner needs to think on it.

## Key decisions & rationale (so future-me remembers why)

- **Out-of-band replies** (Envelope) chosen for framework-routed failure delivery. Price = uniform `O`
  (point 3).
- **No auto-start.** Construction builds, running is app-level (`Node.runAll`). Forking a drain in `make`
  smuggles execution into construction — the effect-system `new User()`-hits-the-DB anti-pattern, worse under
  DI (a layer polls before its graph is wired). This is *why* `Actor` was made a `Node`.
- **Key-safety in the pipe** (`fromKeyedQueue` + private ctor) — correct by construction, not by convention.
- **`Self` = the pool inbox**, keyed routing (a self-message carries its own key), non-blocking only.
- **Sharding across processes**: NATS numbered subjects + StatefulSet ordinals — pin partitions to stable
  identity, sidestep consensus/rebalancing. Repartitioning is the known wrinkle.

## Deferred work (the reminder list)

1. **[main]** Point 3 — GADT `Request[A]` for per-message typed replies. Contract change, contained seam.
2. Point 1 — remove `Worker.ask`; `process` → `protected`; check `input`/`output` external readers before
   hiding; reconsider `Worker extends Through` (vs `Node` + `Processor.parallel`).
3. Point 2 — `CachedKeyValueStore` decorator (write-through for agentic, write-back/batch for consumers);
   keep `Passivate` vs evict distinct.
4. Parked — crash-recovery WAL/replay via a `Persistence` port (mind at-least-once + commit-gap).
5. Minor — `pipeToSelf` backpressure cap (concurrency only; lifecycle done — see update below).
6. Naming nit parked: `Self` → `Inbox`?

## Update — 2026-08-10

**`pipeToSelf` rewritten to a child-scope idiom; the manual fiber bookkeeping is gone.** The old version
tracked in-flight pipes in a `Ref[Map[Long, Fiber]]` + id counter, pruned on `.ensuring`, and interrupted
survivors with one `Fiber.interruptAll` finalizer — because `forkScoped`/`forkIn` register via
`ReleaseMap.addDiscard` (no removal handle), so a long-lived scope would accumulate finalizers. New shape:

```scala
def pipeToSelf(message: UIO[I]): UIO[Unit] =
  scope.fork.flatMap(pipe => message.flatMap(sender.emit).ensuring(pipe.close(Exit.unit)).forkIn(pipe).unit)
```

Each pipe runs in a **child** of the entity's scope: `forkIn` makes it survive the turn; the child is bounded
by the parent, so entity-close interrupts in-flight pipes; and `pipe.close` on completion **self-removes** the
child from the parent — because `Scope.fork` registers via `ReleaseMap.add`, which *returns a removal handle*
(verified in `zio/Scope.scala`). So a long-lived entity never accumulates. Verified by probe: no self-close
deadlock, in-flight pipes interrupted at scope-close (fast, not blocking), 5000 pipes drained clean. Same
idiom now used in the LLM sketch's `peeled` (child scope closed when `rest` ends, parent as abandonment
backstop). Key takeaway banked: **`Scope.fork` self-cleans (`add`); `addFinalizer`/`forkScoped` do not
(`addDiscard`)** — reach for `fork` when forking many short-lived scoped things off a long-lived scope.

Only the *backpressure* cap (#5) remains open for `pipeToSelf`; the leak/lifecycle concern is closed.
