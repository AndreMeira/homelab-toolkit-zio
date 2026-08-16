---
title: Processing — Processor, Worker, Stateful, Workflow, Graph
type: architecture
status: current
updated: 2026-08-16
tags: [processing, processor, worker, stateful, workflow, graph, node, actor]
---

# Processing

`homelab.common.processing` — what runs continuously in a service, and the one thing that starts it.
Everything here derives from `Processor`; the pieces that would be an actor framework elsewhere are assembled
from parts that already existed.

Code: `modules/common/src/main/scala/homelab/common/processing/`
Specs: `WorkerSpec`, `WorkflowSpec`, `MailboxSpec`

```
Processor ──┬── Worker ──── Stateful          (request/reply, then keyed state)
            ├── Mailbox.Incoming              (see mailbox.md)
            └── Parallel / Batched variants
Node ──────── children, expanded by Graph
Workflow ──── a stepper, not a Processor: run by whoever owns it
```

## Processor

```scala
trait Processor[+E <: ApplicationError, A]:
  def input: Consumer[E, A]
  def process(value: A): IO[E, Unit]
  def run: ZIO[Scope, E, Nothing] = Processor.serial(input)(process)
```

Three properties do most of the work:

- **`run` never completes successfully.** Its result type is `Nothing`, so a `Graph` racing its processors is
  fail-fast by construction — the only way a race resolves is a failure.
- **Errors are `ApplicationError`.** A graph has one error channel for everything in it, so an adapter wraps
  whatever a library throws at its edge rather than widening this.
- **Identity is a `val`.** `private[processing] val key` is fresh per instance and compared by reference, so a
  graph meeting the same processor twice — registered directly, then reached again as someone's child —
  starts it once. Two structurally equal processors over different pipes are two processors.

`Processor.Parallel` adds a `parallelism` cap with on-demand listener spawning; `Processor.Batched` consumes
`Consumer.Batched`. Emission is deliberately unspecified: a processor holds whatever producers it needs.

## Worker — request/reply

```scala
trait Worker[E, A, B] extends Processor[E, (A, Promise[E, B])]:
  def receive: A => IO[E, B]
```

**The reply channel travels in the payload.** A message is `(value, promise)`, so `ask` emits and waits on the
promise while `send` emits and doesn't — one queue, both idioms, no second channel. `process` settles the
promise from an `onExit`, so a failure, a defect, or teardown reaches the caller instead of stranding it.

A failing `receive` fails **its own caller only** and the loop carries on. Only a defect stops the worker,
which is the usual bargain: a defect is a bug, not an outcome.

## Stateful — a keyed entity, without the actor baggage

```scala
trait Stateful[E, K, S, I, O] extends Worker[E, I, O]:
  def store: KeyValueStore[K, S]
  def key(message: I): K
  def init(message: I): IO[E, S]
  def next(state: S, message: I): IO[E, Stateful.Next[S, O]]
```

The worker supplies the mailbox, the reply plumbing and the loop; the store supplies the state; this adds only
the step — seed on first sight, advance on every message, `Next.Continue(state, reply)` or `Next.Done(reply)`.

**There are no entity handles, so there is no entity lifetime.** An entity is a key with a row; `Done` deletes
the row; the next message for that key seeds a fresh one. Nothing can hold a reference to something that
ended, so nothing needs refusing, retrying, or replacing — the pile of machinery that killed three earlier
actor designs (see the 2026-08-15 session log) cannot even be stated here.

**Serialisation is the pipe's job.** A plain `Worker` handles one message at a time, which is enough.
`Stateful.Parallel` handles several, and *requires* a `Pipe.KeySafe` — because over a pipe that doesn't keep
one key in flight at a time, two messages for one entity read the same state and both write it back. That
loses writes silently rather than failing, which is why the requirement sits in the type.

Deliberately not called an actor: no address to pass around, no supervision, no self-messaging. Naming it so
would promise all three.

## Workflow — behaviour separated from how it runs

A named stepper with lifecycle `Init → Continue* → Done`. `next` is defined over the *pending* union only, so
it can never be handed a finished workflow — there is no phantom `Done` case to match.

The trait is only the behaviour. How it runs is layered by combinators that each return another `Workflow`,
so a decorated workflow is still a workflow:

| Combinator | Adds |
|---|---|
| `run` | steps in memory |
| `persisted(store)` | durable and resumable |
| `serialised(lock)` | one run at a time per input |
| `intercept` / `tap` | rewrite or observe each step |

This is the shape that replaced a `Runner` abstraction: composition rather than a second type.

## Graph and Node — the only thing that starts anything

`Graph.run(roots)` expands each root through `Node.children` (children first), deduplicates by processor
identity, forks each, and races them — so the first failure takes the graph down and the scope tears the rest
down with it.

`Node(val children: List[Processor[…]])` declares ownership **at construction**, which is what makes cycles
impossible: you cannot pass children you have not built yet.

The point is that a processor need not be started by whoever built it. `run` is called once, at the top, and
nothing else in the application has to remember to fork anything.

## Mailbox

The remote counterpart to `Worker`'s local request/reply: an address you can put in a message, resolved by
whoever ends up holding it. `Mailbox.Incoming` is a `Processor`, so a `Graph` runs it like everything else.
Its own page: [`mailbox.md`](./mailbox.md).

## Deliberately absent

- **No actor system** — no handles, no addresses to living objects, no supervision trees. Addressing by key
  removes the problems those solve.
- **No scheduler or dispatcher.** ZIO's runtime is the scheduler; `Processor.Parallel` is the only knob.
- **No lifecycle callbacks.** A `Scope` closing is the lifecycle; `Stateful.Next.Done` is the ending.
- **No runner type for `Workflow`.** Combinators return workflows instead.
