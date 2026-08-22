---
title: Messaging — the two ports everything else is built from
type: architecture
status: current
updated: 2026-08-22
tags: [messaging, producer, consumer, pipe, partitioner, hub, router, inmemory, pollconsumer, polling]
---

# Messaging

`homelab.common.messaging` — the transport seam. Two ports, a conduit that is both, three producers that do
something other than send, an in-memory family that implements all of it without a broker, and one consumer
for substrates that never call you.

Code: `modules/common/src/main/scala/homelab/common/messaging/`
Adapters: `homelab.nats` (Core + JetStream), `messaging/inmemory/` (queues), `PollConsumer` (leased stores)

## The two ports

```scala
trait Producer[+E, -A]:  def emit(value: A): IO[E, Unit]
trait Consumer[+E, +A]:  def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit]
```

**`Consumer` takes the logic rather than handing back a value**, and that is the load-bearing choice. The
adapter wraps `logic` in whatever commit boundary the substrate has — a JetStream ack, a nak, a term — so
offsets and acknowledgement never surface in a caller's code. One `consume` processes one message; a run loop
calls it repeatedly. The cost is stated on the port: a message may be redelivered, so **`logic` must be
idempotent**.

**Variance is not decoration.** `Producer` is contravariant in what it accepts and `Consumer` covariant in
what it yields, which is what makes `contramap` and `map` type-check and lets a `Producer[Nothing, A]` stand
in anywhere. It is also why `Pipe[+E, A]` — both ends over one type — must be **invariant** in `A`: the same
parameter sits in both positions, so neither variance survives.

Combinators are the whole API surface: `contramap` on the emitting side, `map` / `mapZIO` on the receiving
side, and `serial` to force one `consume` at a time. Layering a codec over a transport is `contramap` one way
and `mapZIO` the other — which is exactly how `homelab.nats` turns a `Producer[E, Message]` into a
`Producer[E, A]`.

## Emission is where topology lives

Three of the types here are `Producer`s that do something other than send, which means fan-out and routing
compose with everything that already accepts a producer — no separate concept:

| Type | `emit(v)` does |
|---|---|
| `Hub[E, A]` | sends to *every* subscribed producer, uninterruptibly — all or the fan-out fails |
| `Router[E, A]` | sends to *one* producer chosen per value by `route`, possibly rewriting the value |
| `Producer.noop` | discards |

A dead-letter sink, a mailbox's `forward`, a tee for observability: all of them are "somewhere to emit", so
none of them needs a new abstraction.

## Keying

**`emit` never carries a key.** Where a substrate partitions, the key is derived from the message itself by a
pure `A => K` fixed at construction — so a caller cannot accidentally send two related messages to different
partitions, and a producer's signature stays the same on every substrate. `Partitioner[K, V]` is the port for
that derivation, with `Partitioner.Key[A]` as its typeclass form.

`Pipe.KeySafe[+E, A]` is a marker with no members: a pipe that claims **it keeps at most one value per key in
flight**. It exists so that a type can *require* the guarantee — `Stateful.Parallel` does — rather than
trusting a comment. `Pipe.fromKeyedQueue` is the in-memory pipe that can honestly claim it.

## The in-memory family

Not a test double — the same ports, over ZIO queues, used wherever a process talks to itself:

- **`Wire`** — one queue, both ends: the in-process stand-in for a broker topic.
- **`QueueProducer` / `QueueConsumer` / `QueueSource`** — the halves separately. A source is a tree: `Pure`
  (one queue), `Mapped` (transformed), `Merged` (a fair interleave behind one buffer).
- **`Distributer`** — the keyed channel, the `Producer`/`Consumer` face of a `KeyedQueue`: `emit` enqueues
  under the value's key, and one `consume` claims the head of one claimable key, holds the key while `logic`
  runs, and frees it after. Per-key FIFO with concurrency across keys — this is what `KeySafe` promises.

None of them fail (`E = Nothing`), which is why they compose with any error type.

## Polling a store that will not call you

`PollConsumer[E, A]` is a `Consumer` over a substrate with no delivery: a table used as a work queue, rows
claimed with `SELECT … FOR UPDATE SKIP LOCKED`, a `claimed_until` lease and a `visible_at` retry delay. It
asks for work rather than being told about it, and everything below exists to make asking cheap.

**Reach for it only when the substrate has no consumer client.** NATS, Kafka and SQS already solve delivery,
flow control and liveness; wrapping them here would buy a second layer of queues and nothing else.

The store side is one port, three non-blocking methods, one statement each:

```scala
trait Source[E, A]:
  def claim(upTo: Int): IO[E, List[A]]
  def ack(elements: List[A]): IO[E, Unit]
  def nack(elements: List[A], wait: Duration): IO[E, Unit]
```

**Three queues, three owners**, and the consumer itself touches the store not at all — a caller's fiber owns
nothing but the work:

| Fiber | Takes | Owns |
|---|---|---|
| fetcher | demand | the read, batched by `LIMIT` |
| caller | supply | `logic`, then files a verdict |
| settler | verdicts | the write, batched by `WHERE id = ANY` |

**Capacity is a token held, not a number computed.** A caller ready to run offers a *demand* token and the
fetcher claims only as much demand as it holds, so no row is ever marked claimed with nobody free to run it.
Because `consume` returns only once the outcome is *written*, a caller cannot spend a second token while its
first element is recorded only in memory — so **outstanding leases never exceed `concurrency`**, end to end.

**Batching is `takeBetween(1, batchSize)` and nothing else** — no timer, no flush interval. A quiet consumer
writes one element per statement with no added latency; a busy one batches exactly as hard as it is being
pushed, because the batch is whatever finished while the previous write was in flight.

**Nothing calls a polling consumer**, so liveness is the caller's to arrange: a periodic tick is what keeps it
correct, and `wakeUp` is the latency shortcut for a writer in the same process (call it after the insert
commits). With neither, it polls once, finds nothing, and parks forever.

**Teardown is ordered, and the order is load-bearing**: callers interrupted → fetcher stopped → claims nobody
received handed back → settler closed. Every stage that can still produce a verdict stops before the stage
that writes them, which is what lets a caller interrupted mid-wait still have its verdict recorded. The
settler is the one fiber here that is *never* interrupted — a queue hands an offered item straight to a parked
taker, so interrupting there would destroy the very verdict being delivered; it is stopped through its own
FIFO instead. A dead fetcher or settler fails every caller rather than going quiet, because a silent settler
would process work forever and record none of it.

Specs: `PollConsumerSpec`, `PollConsumerTeardownSpec`, `PollConsumerConcurrencySpec`,
`PollConsumerTeardownStressSpec`, plus `ScopeOrderingSpec` for the two ZIO behaviours the teardown rests on.

## Batched

`Consumer.Batched[+E, +A] extends Consumer[E, List[A]]` — the batch shape is carried by the *type*, not by a
size parameter at the call site, because the size is fixed where the adapter is built. `aggregate` folds a
batch to a single value. Note that `map`/`mapZIO` on a `Batched` return a plain `Consumer` — the batched
subtype is erased by the combinators, so an adapter that wants to keep it re-wraps (as `homelab.nats` does).

## Deliberately absent

- **No streams in the ports.** A `ZStream` would put the run loop in the caller's hands and the commit
  boundary out of the adapter's. Adapters use streams internally where it helps; it never surfaces.
- **No ack/offset surface.** Settlement is the adapter's, driven by whether `logic` succeeded.
- **No key parameter on `emit`.** See *Keying*.
- **No topology description.** There is no graph of channels here — wiring is ordinary code, and what *runs*
  the endpoints is [`processing.md`](./processing.md)'s `Graph`.
