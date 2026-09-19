---
title: "A Loop that can release its own state — interruption for state machines that hold something"
type: research
status: draft
updated: 2026-09-16
tags: [loop, flow, interruption, resources, state-machine, scope, zio, api-design]
---

# A Loop that can release its own state

`Loop` threads a state through an effectful step function until it answers `Done`. It has no notion of the
state *holding* anything, so a loop that is interrupted mid-iteration drops its state on the floor. For a
state that is pure data that is correct and cheap. For a state that owns a remote resource — a ticket, a
lease, a reservation, an in-flight batch — it is a leak the caller has to work around, and the workaround
puts back the mutable cell `Loop` exists to remove.

This asks what an interruption model for `Loop` would look like, what it can promise, and the one case it
cannot cover.

## What prompted it

`distributed-keyed-queue`'s lock (see its `docs/research/waiting-as-a-state-machine.md`) waits like this: it
asks for the lock, and when it cannot have it the store mints a **ticket** recording its place in a queue.
The waiter then parks until the next known event, asks again, and either wins, runs out of patience, or
finds its ticket pruned and starts over. Stated as a machine it is two live states:

```
Entering(within)                      -> ask; granted, or queued with a ticket
Queued(ticket, recheckAt)             -> park, ask; granted, wait again, or the ticket is gone
```

`Queued` owns something: a ticket sitting in the store's waiters list. When the caller is interrupted — a
cancelled gRPC call, a shutting-down pod — that ticket must be withdrawn, or the waiters behind it are
delayed until its deadline.

That is the shape this note is about, and it is not specific to locks: *any* loop that waits while holding a
place has it.

## Why the obvious workarounds do not work

- **Bracket the whole loop.** The finaliser cannot see which state the loop had reached, so it cannot know
  which ticket to withdraw. Making it visible means a `Ref` written by the step and read by the finaliser —
  a cell shared between two places, updated sideways, and written by every caller that needs this.
- **Bracket each step.** Wrong lifetime: one ticket spans many iterations (park, ask, park, ask), so a
  per-step release withdraws a ticket that is still in use.
- **Nest two loops** so that each outer iteration is one ticket, with the bracket between them. This works —
  it is what dkq does today — but it is "a scope per state" hand-rolled, it splits one machine across two
  methods, and it only composes when the state hierarchy happens to be two levels deep.

Each workaround is the caller re-deriving something the combinator is better placed to own.

## Candidate shapes

### 1. Release sees the last state and the exit

```scala
def scoped[R, E, S, O](initial: S)(run: S => ZIO[R, E, Next[S, O]])
                                  (release: (S, Exit[E, O]) => URIO[R, Any]): ZIO[R, E, O]
```

The combinator keeps the current state in a `Ref` it owns, and brackets the loop so the release reads it.
The cell still exists; it stops being anyone else's problem, exactly as `acquireRelease` hides the resource
it tracks. At the call site the lock becomes one flat loop over the state enum, with the release deciding
from the state it is handed:

```scala
Loop.scoped(State.Entering(patience))(stepping(waiter)):
  case (State.Queued(ticket, _), exit) if !granted(exit) => store.withdraw(name, ticket).ignore
  case _                                                 => ZIO.unit
```

Simple, covers the motivating case, and the state enum carries enough to decide. It says nothing about
*when* a state was left — only what the last one was.

### 2. Release per transition

```scala
(from: S, to: Next[S, O] | Interrupted) => URIO[R, Any]
```

Fires whenever the loop leaves a state, so a ticket is withdrawn the moment it is abandoned rather than at
the end of the whole wait. Strictly more precise, and it subsumes 1. The cost is vocabulary: the release
needs to distinguish "moved on", "finished", and "the caller vanished", and most states are left for reasons
that need no cleanup, so every caller writes a catch-all branch.

### 3. A scope per state

```scala
def scopedStates[R, E, S, O](initial: S)(run: S => ZIO[R & Scope, E, Next[S, O]]): ZIO[R, E, O]
```

The most ZIO-native: a state's resources are acquired in the scope of the step that produced it and die when
that state is left. No bespoke hook at all. It only fits when **one iteration is one state** — the lock's
`Queued` iterates many times while parking and asking, so this shape would want the machine restructured so
that each iteration ends its state. Worth knowing that dkq's current nesting *is* this shape, reached by
hand.

## Semantics to pin down before implementing

- **Release runs on every exit**, including `Done`, with the `Exit` so it can decline — a state that finished
  by succeeding usually needs no cleanup, and the caller should express that rather than the combinator
  assuming it.
- **Release is uninterruptible**, as `acquireRelease`'s is, or the cleanup can be cancelled by the same
  interruption that made it necessary.
- **Failure ordering**: a step that fails must still release the state it failed in, and the step's error
  must win over anything the release reports.
- **Nested loops**: releases run innermost first, which falls out of bracket nesting but should be stated.
- **What the release sees is the last *recorded* state**, which matters for the hole below.

## Two answers to interruption, at different failure scopes

A bracket only runs if the process lives long enough to run it. `Workflow.persisted` answers a different
failure: it writes the state to a `KeyValueStore` after every transition, so the state outlives the fiber
*and the process*, and a later run — or a sweeper — can finish or compensate it. The two are complementary
rather than competing:

| what happens | what can clean up |
|---|---|
| a step fails, or the caller is cancelled, with the process alive | a bracket: local, immediate, costs nothing |
| the process dies | something durable: checkpointed state and a recovery pass, or the resource's own deadline |

This matters for how the scoped variant is described. It covers the first row and none of the second, and a
loop that holds a remote resource has both failures.

**The motivating case already answers the second row without a checkpoint**, which is worth noticing before
reaching for persistence. dkq's ticket *is* durable state: it lives in the store's waiters list, it carries
the caller's deadline, and the granting script prunes expired ones from the head. That is a recovery process
in the protocol rather than in the runtime. Routing the same waiter through `Workflow.persisted` would write
the state again, once per park-and-ask cycle, to replace a mechanism that currently costs nothing — so for
this shape the durable half is already paid for, and only the local half is missing.

The general rule that falls out: **checkpoint when the resource has no deadline of its own**. A lease, a
ticket or a reservation that expires needs a bracket for the live-process case and nothing more. A resource
that persists until someone explicitly releases it needs the durable half too.

## The hole this cannot close

The state is recorded after the step returns. Interrupt *during* a step that has already had an effect —
`acquire.lua` has minted a ticket, the reply is in flight, the fiber is cancelled before the loop records
`Queued(ticket)` — and the release sees the previous state. The ticket exists, unreleased, and nothing local
knows its id.

That is the at-least-once shape: a remote effect and a local record that cannot commit together. No loop
combinator can fix it. It belongs to the protocol, and the protocol's answer is the one dkq already uses —
tickets carry deadlines and the store prunes expired ones, so the worst case is bounded rather than
permanent.

Worth stating plainly in whatever ships, because the alternative is someone concluding the combinator
guarantees more than it does.

## Evidence from building it by hand

`distributed-keyed-queue` now runs its lock's wait on a hand-rolled guarded loop, which is the shape this
note argues for. Two things came out of writing it that belong here, because both are arguments for the
combinator living in one place rather than at each call site.

**The obvious implementation is wrong, and it fails silently.** The seam has to be uninterruptible while the
step stays interruptible, and the naive form does not do that:

```scala
ZIO.uninterruptibleMask: restore =>
  restore(run(state)).flatMap:
    case Next.Continue(next) => loop(next)(run)     // wrong: the recursion runs inside the mask
```

From the second iteration on, the fiber is uninterruptible, so the nested mask's `restore` restores
*uninterruptibility*. Two consequences, neither of which reads as the cause: a park built on `timeout` never
ends, because `timeout` works by interrupting the loser; and `Fiber#interrupt` on the waiter blocks forever.
The fix is one word — `restore(loop(next)(run))` — and the symptom before it was two tests hanging for a
minute each with no error.

**So the semantics need stating as a property, not a description.** "The seam is uninterruptible" is not
enough: what a caller depends on is *"a step is interruptible, and nothing between steps is"*. Anything that
parks with a timeout — which is what waiting loops are made of — depends on the first half as much as the
resource safety depends on the second.

**And the loop is worth its keep for a reason unrelated to interruption.** The same machine written as
mutual recursion — each state's handler wrapping the recursive call — never discharges a handler until the
whole wait ends, so handlers accumulate one per transition. A waiter that parks three hundred times holds
three hundred finalisers and pays three hundred withdrawals when cancelled, all but one of them finding
nothing. A loop makes each step a complete effect, so one handler is live at a time. Neither version looks
different from the other on the page.

## Open questions

- **Is 1 enough?** It covers the motivating case; 2 is more precise but heavier, and no second use case has
  asked for it yet. One example is not enough to choose an API.
- **Should this be `Loop` at all**, or a sibling? `Loop`'s appeal is that it is twenty lines with nothing to
  learn. A scoped variant doubles the surface; a separate name keeps the simple thing simple.
- **Does `Workflow` want the same thing?** It has the identical problem one level up: `persisted` keeps the
  state alive across a process death, but nothing releases what a step acquired when a workflow is
  interrupted while the process is still running. It needs the local half as much as `Loop` does, which
  argues for the model living below both rather than inside either.
- **Does anything else in the toolkit hold across iterations?** `Batcher` accumulates, `Mailbox` parks,
  `Processor` loops over a consumer. Whether any of them owns a remote resource *between* steps would say
  whether this is a general capability or one library's need.
