---
title: Messaging — the two ports everything else is built from
type: architecture
status: current
updated: 2026-08-16
tags: [messaging, producer, consumer, pipe, partitioner, hub, router, inmemory]
---

# Messaging

`homelab.common.messaging` — the transport seam. Two ports, a conduit that is both, three producers that do
something other than send, and an in-memory family that implements all of it without a broker.

Code: `modules/common/src/main/scala/homelab/common/messaging/`
Adapters: `homelab.nats` (Core + JetStream), `messaging/inmemory/` (queues)

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
