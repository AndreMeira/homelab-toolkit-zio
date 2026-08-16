---
title: Stateful + Runner — the ActorWorker successor (v5)
type: session
status: current
updated: 2026-08-10
tags: [actor, stateful, runner, processing, workflow, design]
---

# Stateful + Runner (v5)

The redesign that supersedes `ActorWorker`. Same actor semantics (keyed, stateful, out-of-band replies,
`pipeToSelf`, passivation), but restructured so the **behaviour is decoupled from the machinery that runs it**,
following the `Workflow` / `Workflow.Runner` model.

Code: `../../modules/incubator/src/main/scala/homelab/incubator/processing/actor/v5/Stateful.scala`
Spec: `modules/incubator/src/test/scala/homelab/incubator/processing/v5/StatefulSpec.scala` (6 tests, green)

## The shape

- **`Stateful[E, I, S, O]`** — behaviour only: `init(message): IO[E, S]` + `next(self): (I, S) => IO[E, Step]`.
  **Not a `Node`** (no `Worker`/`Through`, no `run`/`input`/`output`/`process` surface). This is the review's
  "reconsider `Worker extends Through`" point, resolved by *removing* the hierarchy.
- **`Stateful.Runner[E, I, K, S, O]`** — the substrate: holds the state store + per-key scheduling + scope. The
  behaviour is **passed per call**, not bound in: `runner.fire(actor, message)` / `runner.ask(actor, message)`
  — exactly `Workflow.Runner`'s `runner.run(workflow, input)`.
- **One keyed engine, no single/pool split.** Every entity is a key; a "single actor" is the degenerate
  one-key case (partition everything to `()`). Collapses `ActorWorker` + `ActorWorker.Pool` into one type.

## The three structural decisions

1. **Behaviour ⟂ runner (the `Workflow.Runner` model).** The runner is *specific to a behaviour* only in its
   **type parameters** (`E,I,K,S,O`) — the behaviour **value** travels with each `fire`/`ask`. Payoff: the
   substrate is swappable (in-memory keyed now; a durable/NATS-sharded one later) for the same behaviour, and
   the behaviour is a plain, runner-agnostic value. Cost, accepted: `E,O` aren't inferable from `store` at
   `make`, so the call annotates — `Runner.make[E,I,K,S,O](store)`; and `next(self)` is re-bound per message
   (the "bind `self` once per entity" optimisation is gone — inherent to behaviour-per-call).

2. **Fork-on-send, no `start` (the `Batcher.Serial` strategy, keyed).** No standing drain to boot:
   construction is inert, and the *first* message to a dormant key forks that key's drain into the captured
   scope (`Serial.run` shape — uninterruptible enqueue+fork, `interruptible` drain body). Per-key FIFO = the
   actor guarantee; independent forks across keys = free cross-key parallelism. Dissolves the
   make-then-forget-`run` footgun *without* auto-starting at construction (work starts on first *use*, not
   construction — consistent with "no DB call in `new User()`").

   The load-bearing invariant is `claimNext`: it keeps a key **present** (in flight) while its last message is
   still processing, and only **releases** it on a later step that finds the queue empty. So a concurrent
   `enqueue` during that window appends (`started == false`, no second drain) instead of forking a rival drain
   — **at most one drain per key**. Proven by a 100-way concurrent flood on one key reaching exactly 100.

3. **Typed store, no Serde imposed (behaviour travels in the envelope).** The runner only `get`/`set`/`delete`s
   a `KeyValueStore[K, S]` with native `S`. Serialisation, residency and resource lifecycle are entirely the
   *store's* business (in-memory needs no codec and drops nothing on `delete`; a persistent store carries its
   own; a resource-holding store can acquire in `set`, release in `delete`). This is why `init` needs **no
   `Scope`** — the runner never owns a resource. Because the behaviour isn't bound at `make`, each queued
   message **carries the `Stateful` that should process it**, and a self-`send` re-tags with the same one — so a
   drain always has a behaviour to apply, whatever call (or self-send) enqueued the message it claimed.

   > **Done in `Workflow` too** (2026-08-10). `Workflow.Runner` was migrated the same way: dropped the
   > `Workflow.Serde` trait and the `(String,String)->String` shared store; `Runner` is now typed
   > `Runner[Err, I, S]` (specific to a workflow's input/state), `Default[I, S]` checkpoints native `S` to a
   > `KeyValueStore[(String, I), S]` keyed by `(workflow.name, input)`, and the workflow value is passed to each
   > `run(workflow, input)`. `Runner.Inline` → `Runner.inline[I, S]`. No decode path in the runner — a
   > persistent store owns its own (de)serialisation and surfaces faults as `AdapterError` (so the old
   > "corrupt checkpoint → AdapterError" test was dropped). `name` still namespaces the key, kept native as a
   > tuple rather than encoded. WorkflowSpec green (5).

## Kept from ActorWorker

- **Out-of-band replies.** Envelope carries `Option[Promise]`; a step's reply routes from an *uninterruptible*
  `onExit`, so a failed/interrupted step fails the `ask`, not the drain — one bad message never wedges a key.
  A non-interrupt failure is swallowed (drain continues); an interrupt is re-raised (drain stops on scope
  close). `abandon` finalizer interrupts still-queued callers on close.
- **`Self` = `send` + `pipeToSelf`** (non-blocking, no self-`ask`). `pipeToSelf` forks background work into the
  runner scope via `forkIn(scope)` (the self-cleaning idiom — see the 2026-08-09 review's 2026-08-10 update).

## Supersedes / still open

Supersedes these `ActorWorker` review points (`docs/sessions/2026-08-09-actor-worker-design-review.md`):
- **Point 1** (Worker inheritance leaks `process`/`input`/`run`) — gone; `Stateful` is not a node.
- **Single/Pool split** — gone; one keyed runner.
- **Reload-per-message framing (Point 2)** — now just one store impl behind a neutral port; residency/durability
  is a store choice, not the runner's.

Still open (unchanged, orthogonal):
- **Point 3 — uniform reply `O`** (the main flaw). GADT `Request[A]` for per-message typed replies; contained
  seam (`I`, `O`, `Step`, envelope, `ask`, `process`). Deferred.
- **`pipeToSelf` backpressure cap** (`Semaphore(maxPipes)`) — concurrency only; lifecycle handled.
- **`fire` drops a store error.** A `fire` (no reply channel) whose `set`/`delete` fails with `AdapterError` is
  silently dropped — same as `ActorWorker`. Fine for fire-and-forget; flag-worthy if fire must be durable.
- **Naming.** `Stateful` kept (the deliberate "Actor is too tainted" choice); `actor` is only the *parameter*
  name in `fire`/`ask`. `Self` → `Inbox`? still parked.
