---
title: "A one-element signal — telling a reader to look, not what to see"
type: research
status: draft
updated: 2026-09-21
tags: [signal, wake, readiness, coordination, idempotence, polling, sliding, design]
---

# A one-element signal

A recurring shape, written four times across three repos before anyone named it. Two parties share some
state. One changes it; the other has to notice. The obvious move is to send *what changed*, which makes the
channel a queue and puts the reader's correctness at the mercy of the channel's delivery. The better one
sends nothing at all — **a signal that says "look here again", with the truth read fresh from wherever it
already lives.**

Everything else follows from the signal carrying no information.

## The shape

```scala
// the writer
state.change(...) *> signal.offer(())       // write the truth, then say so

// the reader
signal.take *> state.read.flatMap(act)      // wake, then find out what is actually true
```

Because the signal says only "something happened here", it is idempotent, collapsible and order-free. Two
signals mean what one means. A signal that arrives after the reader has already looked costs one wasted
look. A signal that arrives before the reader is ready is not lost, because the change it refers to is
durable in the state, and any later signal will do just as well.

So the buffer holds **one element per subject** and drops the rest:

```scala
Queue.sliding[Unit](1)      // in process
ZADD ready NX <key>         // across instances, set semantics doing the same job
```

More than one would be waste, since the reader reads the whole truth regardless.

## Four things are load-bearing

### 1. The failure modes are not symmetric

> A signal is a hint that may be wrong, never a promise that may be lost.

A **spurious** signal costs one wasted look, and cannot accumulate, because the buffer collapses. A **lost**
signal costs silence with work outstanding — nothing fails, nothing retries, and the system is simply quiet
when it should not be. That is the worst shape a failure can take, and it is the reason the asymmetry has to
be deliberate rather than discovered.

The practical consequence: every path that could swallow a signal puts one back, and those recoveries are
**unconditional**. A patience elapsing, an effect failing, a caller interrupted — each hands the token on
rather than reasoning about whether it was needed. Reasoning about it is how one gets dropped.

### 2. Write the truth first, then signal

Always in that order, and never in one transaction. A signal that arrives before the state it refers to
sends the reader to look at something that is not there yet — survivable, since it will be signalled again,
but it is a wasted look that is easy to avoid. The reverse ordering is what makes the pair safe without a
transaction across two systems.

Where the writer can die between the two, the pair belongs somewhere that is retried. A request handler has
no second chance and will leave state with no signal behind it; a consumer under at-least-once delivery
simply does both again, which costs one duplicate signal, and duplicates are free.

### 3. The signal must not become the work list

The moment a signal carries payload, none of the above holds: collapsing loses a message, duplicates are no
longer free, and order starts to matter. The discipline is not "keep the message small" — it is that the
channel carries *no* information the reader needs, so the reader is correct even if the channel is at its
sloppiest.

This is the line that gets crossed by accident. Putting the payload in "just this once, so the client only
has to do one write" turns a signal back into a queue and takes the collapse with it.

### 4. Fan-out is a separate axis from cardinality

One element per subject says how much is buffered. It does not say how many readers a signal wakes, and the
answer differs by what the readers are asking:

| | one reader wakes | every reader wakes |
|---|---|---|
| when | any reader can serve the work | each reader must re-check its own position |
| cost of the other choice | a thundering herd: all wake, one wins, the rest spend a round trip to learn there is nothing for them | a reader that would have been served waits out its patience for no reason |

dkq has both, built on the same primitive: `QueueReadiness` hands out one token per announcement, because
any consumer can serve any key; `LockReadiness` holds a mailbox per waiting name, because grants go by
ticket order and every waiter must re-ask whether it is the head.

The one-reader form needs one extra rule: a reader that looks and **finds** work hands the token on before
returning, so a burst drains one reader at a time. A reader that finds nothing does not hand on, which is
what stops the chain.

## Where it is used

- **`flow/KeyedQueue`** (this repo). `ready: Queue[K]` carries *keys*, never values — the values are in
  the state. A key is published exactly once per claimable period, and `ready.take` wakes exactly one taker.
- **dkq's `QueueReadiness` and `LockReadiness`.** `Queue.sliding[Unit](1)`, one token and one mailbox-per-
  waiter respectively; a parked `dequeue` holds no store connection and does not poll.
- **dkq's `ready` set and wake streams.** The same idea across instances: `ZADD NX` makes a key present at
  most once however many messages arrive for it, and a wake stream entry names a queue that *may* have work.
  The reader then asks the store what is true.
- **dkq's client, as an API.** `Provider.signalProducer(queue)` and `Provider.signalConsumer(config)` over a
  `Ready(id)` whose id is a key. Nothing is encoded or decoded: the producer writes the key into the
  envelope and the consumer reads it back off, so there is no payload for the two halves to disagree about.
  A claim's worth of announcements folds to the distinct keys they name, and the logic runs once per key.
- **The agent loop sketched in [`llm-conversation-model.md`](./llm-conversation-model.md)** — unbuilt, and
  now with the above to stand on. One contentless message per conversation, its key the conversation id;
  the transcript is the truth, and the message only says to read it again.

The `Ref`-based election in [`drain-fiber-over-a-state.md`](./drain-fiber-over-a-state.md) is a near
relative rather than an instance: its state is *both* the signal and the work list, which is exactly the
coupling §3 warns against — permissible there because both live behind one atomic `modify`, and impossible
the moment they are two systems.

## A signal may also grant the subject

The in-process forms above wake a fiber and nothing more: what the woken reader does about the subject is
between it and whatever else is running. A signal delivered as a **claim** does something stronger — it
hands over the subject exclusively, and nothing else may act on it until the claim settles.

That is dkq's queue rather than a property of the pattern, but it changes what the pattern is good for
enough to be worth separating. A reader woken this way can read state, decide and write it back without
guarding any of it, because there is no second reader inside that key. A fan-out notification cannot offer
it: every subscriber wakes, they race for the same state, and keeping one out is a lock added afterwards
with its own lease and its own failure modes. Here the wake and the exclusion are one act, since what is
handed over is the key.

Two things follow that the plain form does not have:

- **A signal for a subject already claimed waits.** It is not fanned out to a second reader and not
  dropped; it is delivered when the claim ends, which is what makes announcing into someone else's work
  safe rather than merely harmless.
- **The exclusion outlives a process.** It is a lease in a store, so a reader that dies releases the
  subject rather than stranding it — where an in-process election survives exactly as long as the process
  holding it.

The cost is the lease being real: a reader whose work outlives one must renew it, and must stop when told
its claim is stale. That obligation is the price of not needing a lock.

## What it solves

- **No polling.** Reads happen on change, so their number is bounded by real events rather than by elapsed
  time. This is the property most often claimed and least often true: a design that nacks-and-retries, or
  that wakes on a timer "just in case", has a poll with better manners.
- **No thundering herd**, where one reader can serve the work.
- **No coupling between the channel and correctness.** The channel may duplicate, reorder, or run late. It
  may not lose — and that is the single obligation to design against, rather than a list of them.
- **No work in transit.** Nothing is in flight anywhere: the state is the only copy, so there is no
  reconciliation between what a channel holds and what a store holds.

## When not to reach for it

- **When the reader cannot cheaply re-derive the truth.** A signal costs a full read; if that read is
  expensive, sending what changed is genuinely cheaper.
- **When the reader needs the content.** Then it is a queue, and it should be one.
- **When the order of events is itself the information.** Signals collapse, and collapsing is what destroys
  order.

## What it does not give

- **No liveness on its own.** Lose the last signal and the state waits forever, quietly. Something must
  bound that: a patience, a deadline, a periodic reconcile. Which reintroduces a timer — but as a *backstop*
  measured in minutes rather than as the mechanism, and that difference is the whole point.
- **No progress guarantee for a reader that cannot act.** A reader woken into a state it cannot yet act on
  must do nothing and wait for the next signal. Where the unblocking event is not itself signalled, that is
  a stall, and the arrangement is wrong.
- **No ordering, and no count.** A reader never learns how many changes it is answering, only that there was
  at least one.
