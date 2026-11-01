---
title: "ZIO interruption and threads — what an interrupt can stop, and what it waits for"
type: learning-material
status: current
updated: 2026-11-01
tags: [zio, interruption, fibers, threads, blocking, attemptBlocking, timeout, race, shutdown, jnats]
---

# ZIO interruption and threads

A fiber is not a thread, and interrupting a fiber does not interrupt a thread. Most of the surprises with
shutdown, timeouts and blocking clients follow from that one fact. This page explains how ZIO maps fibers
onto threads, what an interrupt can and cannot stop, and which tool to reach for when a blocking call has to
end.

Everything here is **ZIO 2.1.23** on the JVM. The numbers in the tables come from throwaway specs run against
that version: a fiber interrupted 200 ms into a 1.5 s body, unless stated otherwise. The source references are
to ZIO's own files.

## Fibers run on a pool of threads

ZIO runs fibers on a small pool of worker threads (`ZScheduler-Worker-N`, about one per core). A fiber keeps
its thread only while it has work to do: when it waits (a `sleep`, a `Promise.await`, a `Queue.take`), it gives
the thread back, and it may resume on another one. Observed, in one fiber:

```
plain effect: ZScheduler-Worker-0 | after sleep: ZScheduler-Worker-1 | after yield: ZScheduler-Worker-1
```

Blocking code must not run on those workers, or it starves every fiber sharing them. `ZIO.blocking` and the
`attemptBlocking` family move the effect onto a separate, growable pool (`zio-default-blocking-N`). Observed:
inside `attemptBlocking` the fiber ran on `zio-default-blocking-1`, **and so did the next step after it**. So
leaving the blocking region does not necessarily move the fiber straight back to a worker.

So there is no fixed fiber-to-thread mapping. A thread is borrowed for each run of steps.

## An interrupt is a message, handled between steps

Interrupting a fiber sends it a message. The fiber acts on it **between two steps**, and only while it is
interruptible. A single step is never cut in half: ZIO has no way to stop JVM code that is running.

That includes any synchronous code wrapped in one effect: `ZIO.succeed(...)`, `ZIO.attempt(...)`, or the body
of `ZIO.attemptBlocking(...)`. ZIO sees the whole body as one step. Until it returns, the interrupt waits.
Observed, with no blocking pool involved:

```
interrupt on ZIO.succeed(spin 800 ms)    returned after 694 ms    -> body done: true
```

Two more rules complete the picture:

- **Uninterruptible regions defer the interrupt.** Inside `ZIO.uninterruptible`, in finalizers, in the
  acquisition of `acquireRelease`, and in `uninterruptibleMask` outside its `restore`, the message is held.
  It is acted on at the **first interruptible step after the region**. That step is often not where you'd
  expect: a `logic <* signal.succeed(())` whose `logic` finished uninterruptibly was interrupted on the
  `signal` step, so the adapter settled finished work as interrupted (`Consumer.once`, before the fix).
- **A forked fiber inherits the interruptibility of the place it was forked.** A `ZIO.never.fork` inside an
  acquisition is uninterruptible, so interrupting it waits forever. `.interruptible` on the forked effect is
  the fix (`Shutdown.make`; `ScopeOrderingSpec` shows the inheritance).

## `interrupt` waits; `interruptFork` and `disconnect` don't

`fiber.interrupt` sends the message **and waits for the fiber to end**. If the fiber is in the middle of a
long step, `interrupt` takes as long as the step. The operators built on interruption inherit that wait:
`timeout` and `race` interrupt the loser and wait for it before returning.

Ending includes the fiber's **finalizers**, which run uninterruptibly: an `ensuring`, the release of an
`acquireRelease`, a scope closing. So an effect that owns a resource takes as long to interrupt as its
cleanup takes, even with no long step in sight. Observed, with a 1 s finalizer:

```
ZIO.never.ensuring(sleep 1 s).timeout(200 ms)              returned after 1215 ms
fiber.interrupt on ZIO.never.ensuring(sleep 1 s)           returned after 1006 ms
ZIO.never.ensuring(sleep 1 s).disconnect.timeout(200 ms)   returned after  206 ms
```

| Operation, over a 1.5 s blocking body that ignores `Thread.interrupt` | Returned after | Body |
|---|---|---|
| `fiber.interrupt` | 1296 ms | ran to the end |
| `fiber.interruptFork` | 0 ms | still running; `fiber.await` returned 1393 ms later |
| `.timeout(200.millis)` | 1505 ms | ran to the end; result `None` |
| `.race(ZIO.sleep(200.millis))` | 1501 ms | ran to the end; the sleep won |
| `.disconnect.timeout(200.millis)` | 206 ms | still running in the background, finished later |

- **`interruptFork`** sends the message without waiting. The fiber still ends only when its step ends.
- **`disconnect`** makes an effect's interruption return at once, and lets the effect finish on its own in
  the background. It is the only way to "detach" a blocked call: the thread is abandoned, not stopped, and
  keeps running until the call returns.

The table also shows a detail that is easy to miss: an interrupted fiber reports `interrupted` even when its
body ran to completion. The body's result is thrown away, and any side effect it had already made stays made.

## Making a blocking call stop

Since ZIO cannot stop running JVM code, a blocking call only ends early if the library behind it can be told
to stop. ZIO offers one variant per way of telling it:

| Variant | How it asks the call to stop | Interrupt returned after (1.5 s body) |
|---|---|---|
| `attemptBlocking(body)` | it doesn't | 1296 ms, body ignoring interrupts<br>1300 ms, body honouring them |
| `attemptBlockingInterrupt(body)` | `Thread.interrupt` on the thread running the body | **1 ms**, body honouring interrupts<br>1299 ms, body ignoring them |
| `attemptBlockingCancelable(body)(cancel)` | runs your `cancel` effect | as fast as `cancel` makes the body return (with a no-op `cancel`: 1295 ms) |
| `attemptBlocking(body).disconnect` | it doesn't; it stops waiting | 0 ms, but the body keeps running |

How each one works (`ZIOCompanionVersionSpecific`, `ZIOPlatformSpecific`):

- **`attemptBlocking`** is `ZIO.blocking(ZIO.attempt(body))`. The body runs as one step of the fiber, on the
  blocking pool. An interrupt waits for it.
- **`attemptBlockingInterrupt`** runs the body on a daemon fiber and, on interruption, calls `Thread.interrupt`
  on its thread, repeating every few milliseconds until the body ends, then waits for it. A body that answers
  with `InterruptedException` (`Thread.sleep`, `Object.wait`, `BlockingQueue.take`, `Lock.lockInterruptibly`,
  a client method declared `throws InterruptedException`) ends at once, and the effect ends interrupted. A
  body that never checks the interrupt flag (a busy loop, most socket reads on the classic `java.io` streams)
  is not stopped: the interrupt waits for it, as with `attemptBlocking`.
- **`attemptBlockingCancelable`** runs your `cancel` effect on interruption, then waits for the body. It is for
  calls that are woken by closing something: closing a socket, a channel, a connection, or a library's
  `cancel()`.
- **`.disconnect`** is a last resort, for a call that nothing can wake. The thread stays busy until the call
  returns, so a call that never returns leaks a blocking-pool thread.

Choose by the library, not by habit: find out how the call can be woken, and use the variant that does that.
When nothing can wake it, bound the call itself (a timeout argument on the call) and accept that an interrupt
waits for that long.

## A worked example: a JetStream pull at shutdown

A JetStream pull asks the server for a message and blocks until one arrives or the pull's `expiry` passes
(30 s by default). Written with plain `attemptBlocking`, the pull is one step of the consumer's fiber:

```scala
ZIO.attemptBlocking(context.next(config.expiry))   // interrupting this waits up to `expiry`
```

When the application shuts down, its scope interrupts that fiber, and the interrupt waits until the pull
returns. On a quiet stream that is the full `expiry`: every pod takes up to 30 s longer to stop than it needs
to, and a shutdown with several consumers waits for each of them.

The toolkit's JetStream consumers pull with `attemptBlockingInterrupt` instead (`stream.Consumer`,
`stream.BatchConsumer`), so the interrupt reaches the thread blocked inside jnats, and the pull ends without
waiting out the expiry. That relies on jnats answering `Thread.interrupt` while it waits, as the consumers'
scaladoc states; the measurements on this page did not exercise jnats. The settlement calls (`ack`, `nak`, `term`) stay on plain `attemptBlocking`: they are
short, and they run in an uninterruptible step anyway, because a settlement must not be cut in half.

## Checklist

- **A long synchronous body is one step.** Nothing interrupts it; the interrupt waits. Keep such bodies short,
  or run them with a variant that can wake them.
- **`interrupt`, `timeout` and `race` wait for the interrupted fiber to end.** Their latency is the step
  being interrupted plus the fiber's finalizers, which run uninterruptibly. Use `interruptFork` or
  `.disconnect` only when not waiting is really what you want.
- **Pick the blocking variant from how the call can be woken:** `Thread.interrupt` →
  `attemptBlockingInterrupt`; closing or cancelling something → `attemptBlockingCancelable`; nothing →
  bound the call, or `.disconnect` and accept the leaked thread.
- **A pending interrupt lands on the first interruptible step after an uninterruptible region.** Put whatever
  must not be split from the region inside it, not after it.
- **Forking inside an uninterruptible region makes an uninterruptible fiber.** Mark the forked effect
  `.interruptible` when it must be stoppable.
- **An interrupted effect may still have done its work.** "Interrupted" says the result was discarded, not that
  nothing happened.
