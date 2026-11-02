---
title: "ZIO interruption regions and restore — where a held cancel takes effect, and what it can drop"
type: learning-material
status: current
updated: 2026-11-02
tags: [zio, interruption, uninterruptible, uninterruptibleMask, restore, scope, acquireRelease, recursion, finalizers]
---

# ZIO interruption regions and restore

[`zio-interruption-and-threads.md`](zio-interruption-and-threads.md) covers what an interrupt can stop: a
fiber is not a thread, a blocking call ends only if something wakes it, and `interrupt` waits for the fiber it
stops. This page is about the other half: **where** a cancel takes effect once it has been sent, and what it
can throw away when it does.

It came out of a real leak. In `distributed-keyed-queue`, a lock granted to a caller who had just cancelled
stayed held until its lease ran out, although every step involved was written carefully. The rules below
explain why, and the patterns at the end are what fixed it.

Everything here is **ZIO 2.1.23** on the JVM. The results come from throwaway specs run against that version,
and the source references are to ZIO's own files (`zio/internal/FiberRuntime.scala`, `zio/ZIO.scala`,
`zio/Scope.scala`).

## The model in one paragraph

A cancel is a message to a fiber. While the fiber is **interruptible**, it acts on the message at its next
check point. While it is **uninterruptible**, the message is **held**, and acted on at the first check point
after the fiber becomes interruptible again. Nothing is ever cut in the middle of an instruction. Everything
on this page follows from where those check points are.

## Regions, and what `restore` is

Interruptibility is a property of a **region** of code:

- `ZIO.uninterruptible(effect)` runs `effect` uninterruptibly.
- `effect.interruptible` runs `effect` interruptibly.
- `ZIO.uninterruptibleMask(restore => …)` runs its body uninterruptibly and hands you `restore`, which turns
  one piece of the body back to what the surrounding code was.

`restore` is not magic: it **builds a node** around the effect you give it. In ZIO 2.1.23
(`ZIO.scala`, `InterruptibilityRestorer`):

```scala
case object MakeInterruptible extends InterruptibilityRestorer {
  def apply[R, E, A](effect: => ZIO[R, E, A]) =
    ZIO.UpdateRuntimeFlagsWithin(trace, RuntimeFlags.enableInterruption, _ => effect)
}
```

So `restore(step)` is a value meaning "switch interruption on, run `step`, switch it back". The other restorer,
`MakeUninterruptible`, builds the same node with interruption switched **off**. **Which one you get is decided
when the mask is created**, from the interruptibility at that moment: `restore` puts back what the code around
the mask was, not "interruptible". A mask opened inside an uninterruptible region hands back the restorer that
switches interruption off. Around code that is already uninterruptible it changes nothing, but inside an
`.interruptible` region nested in the mask's body it makes the step uninterruptible again.

## Where a held cancel takes effect

There are three check points. Two of them are easy to miss.

**1. Running an instruction while interruptible.** This is the obvious one: between two instructions of an
interruptible region, the fiber looks at its messages.

**2. Entering an interruptible region.** When the fiber evaluates a node that would switch interruption on,
it checks first, and if a cancel is held, it never enters (`FiberRuntime.runLoop`):

```scala
case update0: UpdateRuntimeFlagsWithin.DynamicNoBox[...] =>
  …
  } else if (RuntimeFlags.interruptible(newRuntimeFlags) && isInterrupted()) {
    // One more chance to short circuit: if we're immediately going to interrupt.
    cur = Exit.Failure(getInterruptedCause())     // the effect inside is never run
  }
```

The effect inside the `restore` doesn't start, so neither its body nor its handlers (`onExit`, `ensuring`,
an `acquireRelease`) ever run.

**3. Leaving an uninterruptible region back into an interruptible one.** When a region ends and the flags are
put back to interruptible, the fiber checks again, and a held cancel **replaces the region's result**
(`FiberRuntime.patchRuntimeFlagsCause`):

```scala
if (changed && interruptEnabled && shouldInterrupt()) getInterruptedCause() …
```

There is one exception (`ignoreFlagsUpdate`, from zio#9973 and zio#9974): if the very next frame on the stack
switches interruption off again, the switch back is skipped, and so is the check. Two regions nested directly
inside each other therefore don't create a check point between them.

The third check point is the one that surprises. An uninterruptible step can **finish its work**, produce a
value, and see its own `onExit` report success, and the value is still thrown away a moment later, because the
code it returns into is interruptible.

## What a throwaway spec showed

A step fully uninterruptible, a cancel sent while it runs, then the step allowed to finish:

| Setup | Step finished | Next line ran | Result |
|---|---|---|---|
| `uninterruptibleMask(restore => restore(step).flatMap(next))` | yes | yes | interrupted when the mask ends |
| the same, with a `.map` after the step *inside* `restore` | yes | no | interrupted at the `.map` |
| `Recursion.run` over that step, which reaches the answer | yes | — | interrupted; the answer is lost |
| `Recursion.run`, the step does not answer, a second step follows | yes | the second step never starts | interrupted |

In the first row, the step's region exits straight into `restore`'s own exit, which switches interruption off
again, so ZIO skips the check (the exception above). The `.flatMap(next)` continuation then runs masked, and the
value survives until the outer mask ends and exits into the forked fiber's interruptible top: check point 3. In
the second row, the step's region exits into `restore`'s interruptible region, and the next frame is the
`.map`, not a frame that switches interruption off. Check point 3 fires there, and the step's value is replaced.
In the last row, entering `restore` around the second step is check point 2: the step is never entered.

## Case study: `flow.Recursion`

`Recursion.run` restores each step and runs everything around the steps masked:

```scala
ZIO.uninterruptibleMask: restore =>
  def loop(current: Recursion[R, E, S]): ZIO[R, E, A] =
    terminal.lift(current.state) match
      case Some(answer) => ZIO.succeed(answer)
      case None         => restore(current.next).flatMap(loop)
  loop(from)
```

With a step that is uninterruptible (a store call) and a cancel arriving during it, where the cancel takes
effect depends on the kind of step:

- **A driven step** (`Recursion(state)(transition)`) is `transition(state).map(…)`, and that `.map` runs
  inside `restore(current.next)`. The transition exits into the restored region with the `.map` as the next
  frame, so the cancel takes effect right there (check point 3, row 2 of the table above), inside the step and
  before the question is asked.
- **A reflective step**, whose whole `next` is uninterruptible, has no such frame: its region exits straight
  into `restore`'s own exit, and the result reaches the masked `loop`. If the state reached does not answer,
  the cancel takes effect as the next step starts (check point 2), so that step never starts. If it answers,
  the cancel takes effect as the answer is handed back, because `run` returns into its caller's
  interruptibility (check point 3).

Either way, the state the step reached is lost. Anything the step put in it, for the next step to release or
as the answer, is released by nobody.

**No version of `Recursion` can do better for an interruptible caller.** An earlier `run` opened a mask per
step and restored the recursive call, so the cancel took effect at other places, such as the entry of
`restore(run(reached))`, before the question was asked. Rewriting it as above moved those places and changed
nothing a caller can see: the next step still never started, and the answer was still lost on the way out of
`run`. The rewrite was kept because it reads better. An experiment that also masked `Driven`'s `.map` changed
nothing a caller can see either, and was not kept: `Driven.next` is still an interruptible `.map`. The only
way to receive the answer is to call `run` from an uninterruptible region. But then the mask inside `run` hands
out the restorer that switches interruption off, and no step, not even a park, can be cancelled.

`RecursionSpec` pins this behaviour: "a cancel held by a step that does not answer stops the run before the next
step", "a cancel held by the step that reaches the answer ends the run interrupted: the answer is lost".

## The pattern: release from where the result is consumed

If a step acquires something and hands it on, in the state it returns or as the answer, its own handlers are
the wrong place to release it: they see success. The release has to sit **around the code that consumes the
result**, and decide from **how that code ended**. In practice that is a `Scope` around the whole call:

```scala
def apply(request: DequeueRequest): IO[ApplicationError, DequeueResponse] =
  ZIO.scoped:                                  // closes with the exit of the whole call
    for
      …
      grant <- claim(demand, asked, signal)    // the loop; somewhere inside, the store claims
    yield response(grant)

private def acquireClaim(demand: Demand): ZIO[Scope, AdapterError, Option[Grant]] =
  ZIO.acquireReleaseExit(store.attemptClaim(demand.queue, demand.batch)):
    case Some(grant) -> Exit.Failure(_) => handBack(grant)
    case _                              => ZIO.unit
```

Three facts make this correct:

- **`acquireReleaseExit` is an uninterruptible acquire plus a finalizer.** In ZIO 2.1.23 it is literally
  `ZIO.uninterruptible(acquire.tap(a => ZIO.addFinalizerExit(exit => release(a, exit))))`. A claim the store
  made is always seen, and its release is registered before anything interruptible runs.
- **The finalizer receives the scope's exit, not the step's.** `ZIO.scoped(zio)` is
  `scope.extend(zio).onExit(scope.close(_))`: the scope closes with the exit of the whole body. `Exit.Failure`
  there means the call did not answer, whatever the step that claimed saw.
- **`Exit` has exactly two cases.** `Exit.Success(value)` and `Exit.Failure(cause)`. An interruption is a
  `Failure` whose cause is interrupted, so matching on `Exit.Failure(_)` covers failing and being cancelled.

`DequeueUseCase` (queue claims) and `LockAcquireUseCase` / `LockTryAcquireUseCase` (lock grants) in
`distributed-keyed-queue` use this pattern. `DequeueHandBackSpec` and `LockHandBackSpec` show the leak without
it and its absence with it.

What remains is outside any code here: a response already on its way back over the network is lost, and the
claim lapses on its lease.

## Traps

Each of these was tried, and each one leaks.

- **A scope around the step instead of the call.** The scope closes as the step returns, with success, and
  the hand-back never runs. The cancel then takes effect in `Recursion` and drops the claim. In
  `DequeueHandBackSpec`'s randomised test this stranded 2000 claims out of 2000.
- **A bracket (`acquireReleaseExitWith(acquire)(release)(use)`) inside the step.** It runs `release` as soon as
  `use` ends, with `use`'s exit. Inside the step, `use` is "return the state", which succeeds, so it is the
  same leak. At the top of the call, `acquire` would be the whole wait, and a bracket runs its `acquire`
  uninterruptibly, so the wait could not be cancelled.
- **`onExit` on the loop.** In the case that matters, the loop ends with `Exit.Failure(cause)`, and a `Failure`
  carries no value: the handler doesn't know what to hand back. Recording the claim in a `Ref` for the handler
  to read rebuilds a `Scope` by hand.
- **Registering the release after the loop returns.** The cancel takes effect before the loop returns, so the
  registering line never runs.
- **Wrapping the loop in `ZIO.uninterruptible` to close the gaps.** Every mask inside it now hands out the
  restorer that switches interruption off, so the parks can't be cancelled either. A run that waits forever can then never
  be stopped.
- **Trusting a step's handlers for something the step hands on.** They see success, and the value is dropped
  after them.
- **A test named for interruption that never interrupts.** `RecursionSpec` had one ("a step already past its
  handlers is not undone by an interrupt arriving after it") that only joined the fiber. Its claim went
  unchecked until a spec sent a real cancel at the right moment.

## Checklist

- **Find the check points.** A held cancel takes effect when an interruptible instruction runs, when an
  interruptible region is entered, or when an uninterruptible region exits into an interruptible one.
- **A finished uninterruptible step can still lose its result.** Its handlers saw success; the code it returns
  into may not.
- **`restore` builds a region, and entering it is a check point.** The effect inside may never start.
- **`restore` puts back the interruptibility the mask found, not "interruptible".** A mask opened inside an
  uninterruptible region restores uninterruptibility. Check the interruptibility at the place the mask is
  created.
- **Release a handed-on resource from a scope around its consumer**, keyed on that scope's `Exit`, with
  `acquireReleaseExit` at the place it is acquired.
- **Prove an interruption claim with a cancel sent at the exact moment**: two promises, one the step completes
  when it starts and one it waits on, the cancel sent in between. Repeat it (`nonFlaky`), and randomise the
  interleaving when the window is small.
