---
title: "Loop, Workflow, Recursion — what repeats what"
type: research
status: draft
updated: 2026-09-19
tags: [flow, loop, workflow, recursion, state-machine, duplication, api-design]
---

# Loop, Workflow, Recursion

Three things in this toolkit iterate a state until it answers. This is what each one actually is, which of
them repeat each other, and why nothing was deleted.

## The three

**`flow.Loop`** — `Loop(state)(run: S => ZIO[R, E, Next[S, O]])`, where `Next` is `Continue(S) | Done(O)`.
The step says which of the two it produced, so the driver never inspects the state.

**`processing.Workflow`** — `next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]]`, with `Step` being
`Init | Continue | Done`. Same iteration underneath, and then everything else: `persisted(store)`
checkpoints each state to a `KeyValueStore` so a run resumes after a restart, `serialised` makes it
concurrency-safe per input, `intercept` and `tap` decorate it, and `name` identifies it in storage. The `I`
exists because resuming needs a key to resume *by*.

**`flow.Recursion`** — `state: S` and `next: ZIO[R, E, Recursion[R, E, S]]`, driven by
`Recursion.run(from)(terminal)`, where `terminal: PartialFunction[S, A]` is the caller's question rather
than a property of the family. Two shapes: `Driven` holds a step beside the state, `Reflective` is a state
that carries its own.

## What repeats what

**`Recursion.Driven` and `Loop` are the same abstraction**, differing in where the decision to stop lives.
`Loop` puts it in a wrapper the step returns; `Recursion` puts it in a projection the caller supplies. The
practical differences run one way:

- `Recursion` keeps a step interruptible and the space between two steps uninterruptible, so a step's own
  handlers decide the fate of anything it holds. `Loop` has no such discipline, and a `Loop` whose body
  parks cannot be interrupted safely — which is exactly the case a waiting state machine is.
- `Loop` re-wraps the state at every transition. `Recursion` does not, which is what lets a state *be* the
  answer rather than carry one.
- `terminal` is per-run, so one family answers different questions — stopped at a grant for production,
  stopped at a ticket for a test. `Next.Done` fixes the answer where the step is written.

**`Workflow` is not repeated by either.** Strip `persisted`, `serialised`, `intercept`, `tap` and `name`
and what is left is an iteration — but those are the whole point, and none of them belong in `Recursion`.
A workflow is a *durable* computation with an identity; a recursion is a shape.

## Why nothing was deleted

`Loop` has no caller outside its own spec, so removing it would cost nothing today. It stays because the
toolkit is published and a deletion is a breaking change made on the strength of one week's opinion.
`Recursion` has one real caller (dkq's lock wait, via the shape it was extracted from) and no production
mileage yet; when it has some, `Loop` can be deprecated with something to point at.

`Workflow` keeps its own private `loop`. Rebuilding it on `Recursion` is the obvious next move and the
honest test of whether `Recursion` earns its place in `common` — two callers rather than one. It was left
alone deliberately: `Workflow` has real users (both `Actor` generations and `Stateful` v5), and changing
the engine under them to prove a point about an abstraction is the wrong order.

## What to watch

If `Recursion` acquires a second caller and `Workflow`'s in-memory path folds onto it cleanly, the toolkit
has one iteration primitive and two decorations of it, and `Loop` can go. If it does not fold cleanly, that
is the signal `Recursion` is a good fit for one problem rather than an abstraction — and the incubator's
`v5` still holds `Markov`, the version it grew from, for the comparison.
