---
title: "Processor graph — removed as premature, kept for later"
type: research
status: current
updated: 2026-09-29
tags: [processing, processor, graph, node, startup, deferred]
---

# Processor graph — removed as premature, kept for later

On 2026-09-29 `Graph` and `Node` were removed from `homelab.common.processing`, together with the
`Processor.key` identity that existed only for them. Nothing in the homelab used them: no service registered
a processor, no test built a graph, no incubator sketch extended `Node`. This note records what they were and
what would bring them back, so the idea survives the code.

Last present at `0d914d5` on `main`, in
`modules/common/src/main/scala/homelab/common/processing/{Graph,Node}.scala`.

## What it was

Three pieces, answering one question — *who forks a processor's `run`?*

- **`Node(val children: Chunk[Processor[ApplicationError, ?]])`** — a self-type on `Processor` declaring the
  processors this one owns. Ownership was fixed at construction, which made a cycle unstatable: a child has
  to exist before its parent can name it.
- **`Graph.run(roots)`** — expanded each root through its children (children first, so a downstream loop was
  running before its upstream started), collapsed duplicates by reference identity, forked each `run` into the
  calling scope, and raced the joins. A `Processor.run` is `ZIO[Scope, E, Nothing]`, so the race could only
  resolve as a failure: the first failure anywhere took the set down, and the closing scope took the rest.
- **`Graph.register(build)`** — the form for a layer: build the processor and hand it to the `Graph` in the
  environment in one step, so nobody was left holding something they had to remember to start.

Identity was `private[processing] val key: Processor.Key`, fresh per instance and compared by reference: two
structurally equal processors over different pipes were two processors; the same processor reached twice —
registered directly, then again as someone's child — started once.

## Why it went

- **It answered a question no service had asked.** Every processor in the repo is started by whoever owns
  it, forked in the `init` of the module that built it. That is the shape the wiring rules already
  prescribe (*layers acquire; `init` acts*), and a registry on top of it was a second way to do the same
  thing with no caller to choose between them.
- **It taxed every processor for one consumer.** `key` sat on the trait and `Node` on the type hierarchy,
  visible to everyone, for the benefit of a class nobody instantiated. Two places in the docs and five in the
  scaladocs explained themselves in terms of "the `Graph` that runs it" — a dependency in prose on a thing
  that did not run anything.
- **Ownership was decided before there was anything to own.** `Node.children` presumed a processor that
  fronts others (a bridge owning the worker behind it); the one sketch that wanted that shape stayed in the
  incubator.

What stays is the property the graph relied on: `run` never completes successfully, so anyone who forks
several processors under one scope and races them is fail-fast by construction. That is the caller's one
line, not a type.

## What would bring it back

Any of these, observed rather than anticipated:

- A service with enough processors that a forgotten `run` is a bug found late — the accepted trade in the
  wiring rules ("nothing makes `init` impossible to forget") stops being acceptable at some count.
- A real start-order constraint: a downstream loop that must be consuming before its upstream emits, where
  "children first" is the fix and the order is not obvious from the wiring.
- A processor reachable by two owners that must start once, where reference identity is the only sane
  dedup key.

If it returns, the shape above is the one to rebuild, with two things known at the time and worth keeping:
registration after `run` began was not picked up (the graph started the set it had, and did not watch for
more), and start order was not readiness (nothing waited for a processor to be consuming before starting its
neighbour). Both would want stating in its doc from day one.

## Related

- [`../architecture/processing.md`](../architecture/processing.md) — the package as it stands; *Deliberately
  absent* points here.
- `../sessions/2026-08-15-processing-worker-stateful-graph.md` — where the graph landed, alongside `Worker`
  and `Stateful`.
- [`drain-fiber-over-a-state.md`](./drain-fiber-over-a-state.md) — names `Graph` among the things whose fiber
  life is genuinely the scope's; the argument holds for whoever forks processors now.
