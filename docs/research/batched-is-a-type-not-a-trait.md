---
title: "Batched is a type, not a trait"
type: research
status: current
updated: 2026-09-29
tags: [messaging, processing, consumer, batched, api-design, breaking-change]
---

# Batched is a type, not a trait

Asked on 2026-09-28: was it a mistake to make `Consumer.Batched`, `Processor.Batched` and `Pipe.Batched`
traits rather than type aliases? Yes — and the note that introduced them already said so.

## The principle they were born under

`messaging-design.md`, under "design principles worth preserving verbatim":

> **Traits earn their keep** — a trait exists only when it factors something non-trivial (`Source.Tick`
> earns it; a `Sink` sub-trait did not).

and two lines above it:

> **Batching is a type, not a method.** `Consumer.Batched[E, A] <: Consumer[E, List[A]]`; batch size is
> decided *at the adapter*, not the call site.

The intent — batching carried by the type, size fixed at the adapter — is exactly what an alias gives. The
`<:` was the overreach; `=` was what the principle asked for.

## The test they fail

`messaging.md` states it for `Pipe.KeySafe`: a marker trait is justified when it asserts something the
structural type cannot — "at most one value per key in flight" is a guarantee no signature expresses, so a
type that needs it can *require* the marker rather than trust a comment.

`Chunk[A]` in the type position already says "batched". There is nothing left for a `Batched` trait to
assert.

## What each trait factors

| trait | its own members | implementors (toolkit + DKQ, incubator excluded) |
|---|---|---|
| `Consumer.Batched[+E, +A]` | `aggregate` — a synonym for `map` | four; `aggregate` has **zero call sites** |
| `Processor.Batched[E, A]` | `input` narrowed to `Consumer.Batched` — a narrowing to nothing, since the marker asserts nothing | one, `ReadinessSignalProcessor` in DKQ |
| `Processor.Batched.Parallel[E, A]` | re-declares `input`; a diamond over `Parallel` and `Batched` | **none — dead code** |
| `Pipe.Batched[+E, A]` | `send`, an alias for `emit` | one, `Distributer` |

`QueueConsumer.Batched` is different in kind: a concrete class with its own `consume` over `takeUpTo` and a
real member, `perItem`. It stays a class whatever happens above it.

## What they cost

**Erasure under the combinators, and the code written to undo it.** `map` and `mapZIO` return the parent
type, so a `Batched` loses its subtype the moment it is transformed. `messaging.md` records this as a fact of
life — "an adapter that wants to keep it re-wraps (as `homelab.nats` does)" — but the erasure exists *only
because it is a trait*. Both nats adapters carry the same six lines:

```scala
private def decoded[A: Decoder](consumer: ConsumerContract.Batched[NatsError, Message]) =
  val values = consumer.mapZIO(decode[A])
  new ConsumerContract.Batched[NatsError, A]:
    override def consume[E2 >: NatsError](logic: Chunk[A] => IO[E2, Unit]) = values.consume(logic)
```

The anonymous class exists solely to restore a type an alias never loses. Under the alias, `decoded` is
`consumer.mapZIO(decode[A])`.

**A doc that documents the trait's cost as the design.** The paragraph quoted above describes a workaround
in the voice of a property. Once the trait goes, so does the paragraph.

**Three names for what `Chunk[A]` says once.** A reader meeting `Processor.Batched.Parallel` has to
discover that it is `Processor.Parallel[E, Chunk[A]]` with nothing added.

## The alias

One rule: **`X.Batched[E, A]` names X over `Chunk[A]`, for every X that carries a value.**

```scala
object Consumer:  type Batched[+E, +A] = Consumer[E, Chunk[A]]
object Processor: type Batched[E <: ApplicationError, A] = Processor[E, Chunk[A]]
object Pipe:      type Batched[+E, A]  = Consumer[E, Chunk[A]] & Producer[E, A]
```

`Processor.Batched.Parallel` alone gets no alias — not because it cannot have one, but because it is a
composition the rule already covers: `Processor.Parallel[E, Chunk[A]]`, `Parallel` being an X. It has no
implementors, and a name for a nested combination would be a fourth name for one idea.

`Pipe.Batched` is the one with a genuine shape — it consumes `Chunk[A]` and produces `A`, so it is not
`Pipe[E, A]` (invariant, one type both ways). An intersection says that exactly, and `send` was a synonym.

## Source compatibility

- `extends Consumer.Batched[E, A]` and `extends Processor.Batched[E, A]` **still compile** — Scala 3 allows
  extending an alias that dealiases to a trait type. So `ReadinessSignalConsumer`,
  `ReadinessSignalProcessor`, DKQ's `ManagedBatch`, and the client's `batchedMessages` signature are
  unchanged as text.
- `extends Pipe.Batched[E, A]` **does not** — an intersection cannot be extended. `Distributer` names both
  parents. One site.
- `aggregate` is removed. Zero callers.
- A runtime match on `Consumer.Batched` would break. None exists; an alias has no runtime identity, which is
  the point.
- **Binary-incompatible**: the `Consumer$Batched` class disappears. Consumers of the published artifacts
  recompile — which, pre-1.0, is the expected contract and is already the case for the change below.

## When it ships

It rides a breaking release that is already queued. DKQ pins toolkit `0.0.5`; the `Chunk` migration is
unreleased on toolkit `main`, and `ReadinessSignalProcessor.process(wakes: List[…])` has to become `Chunk`
on the next bump regardless. Same release, same section of the notes, one recompile for DKQ instead of two.

## Sizing

Toolkit: `Consumer.scala` (trait → alias, drop `aggregate`), `Processor.scala` (`Batched` → alias, drop
`Batched.Parallel`), `Pipe.scala` (`Batched` → intersection), `inmemory/Distributer.scala` (two parents),
`inmemory/QueueConsumer.scala` (parent type), the two nats `BatchConsumer.scala` files (delete `decoded`'s
re-wrap), and three docs — `messaging.md` § Batched, `processing.md`, and a stamp on `messaging-design.md`,
which still says `List[A]` from before the `Chunk` migration. The nats specs already exercise both adapters.

DKQ, on its next toolkit bump: `ReadinessSignalProcessor.process` takes a `Chunk`. Its
`extends Processor.Batched[…]` and everything else are unchanged as text.

## What an alias gives up

A name to hang future batch-specific members on. If a `flush`, a `batchSize` reader, or a batched-only
combinator ever appears, *that* is the day a trait is earned — the `Source.Tick` case from the original
note — and introducing it then, over the alias, costs nothing. `HOMELAB.md`'s rule is to decide then.

## Conclusion

Replace the three traits with three aliases under one rule, and delete the nested `Batched.Parallel`. It
removes two anonymous classes, one dead trait, one diamond, one paragraph that documents a workaround as a
design, and a hop for every reader — and adds nothing anyone has to learn, because `Chunk[A]` was already
saying it. Bundle it with the `Chunk` release so DKQ recompiles once.
