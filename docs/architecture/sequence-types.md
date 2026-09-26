---
title: "Which sequence type a public signature uses, and when not to"
type: architecture
status: current
updated: 2026-09-26
tags: [collections, chunk, list, conventions, api, zio]
---

# Which sequence type a public signature uses

**`Chunk` is the default for every sequence in a public signature.** Another type needs a reason, and the
reasons that count are listed below.

This page exists because the toolkit spent its first year without the rule and ended up with three
conventions: `common` and `nats` on `List`, `llm` mixed, the adapters on `List` for wire types. That cost
37 conversion sites and one port that spells a collection of tool calls three different ways.

## Why `Chunk`

**A public surface should speak the language of its neighbours.** Every caller of this toolkit holds ZIO
primitives, and every ZIO primitive hands back a `Chunk` — `Queue.takeAll`, `ZIO.foreach`,
`ZStream.runCollect`, `Ref.modify`. A `List` on the boundary taxes each of those crossings; a `Chunk` taxes
none.

The performance argument is secondary but points the same way. Measured over the published modules at the
time this was written:

| operation | uses | on `List` | on `Chunk` |
|---|---|---|---|
| `:+` append | 18 | O(n) | O(1) amortised |
| `+:` prepend | 0 | O(1) | O(n) |
| indexed access | 11 | O(n) | O(1) |
| `.size` | 12 | O(n) | O(1) |
| `map` / `flatMap` / `foreach` | 185 | the same | the same |

`List` is a `LinearSeq`. Its one structural advantage is the prepend this codebase never performs.

There is no codec reason to prefer `List`, which is the belief that kept it in the adapters. Both
derivation paths were tested: zio-json encodes a `Chunk` field to the same JSON as a `List` field and
round-trips it, and `schema.Generator` produces a byte-identical JSON Schema for either.

## What `Chunk` costs

**Pattern matching is not exhaustivity-checked.** This is the real loss and it is worth stating plainly:

```scala
xs match                      // List: the compiler knows this is total
  case Nil    => …
  case h :: t => …

xs match                      // Chunk: "match may not be exhaustive"
  case Chunk() => …
  case h +: t  => …
```

The mitigation is the shape the codebase already uses — structural cases plus a meaningful default:

```scala
messages.foldLeft(Chunk.empty[Message]) {
  case (before :+ System(said)) -> System(more) => before :+ System(said ++ more)
  case kept -> next                             => kept :+ next
}
```

Two smaller things: `case h :: t` becomes `case h +: t`, and repeated concatenation builds a tree, so
`Chunk.materialize` exists for the case where a profile says it matters.

## When to use something else

Four reasons, and a signature using another type should be readable as one of them.

**`NonEmptyChunk` when non-emptiness is part of the contract.** `Progress.AwaitingTools(pending:
NonEmptyChunk[Tool.Call.Raw])` — a turn waiting on no tools is a state that should not be representable.
Prefer this over a `Chunk` the caller has to check.

**`ListMap` when insertion order is part of the contract.** `JsonSchema.definitions` and `Registry.entries`
both use it so that what is rendered is stable and reproducible. A plain `Map` would be a different
promise.

**`List` when the name says so** — but derive it, do not build on it. `Batch.toList` stays a `List`, because
a method called `toList` that returns something else is a lie. What matters is which one is primary:
`toChunk` is the abstract member every implementation provides, and `toList` is one line of convenience
on top of it.

```scala
def toChunk: Chunk[Either[E, A]]
def toList: List[Either[E, A]] = toChunk.toList
```

Written the other way round — `toList` abstract, everything else converting from it — the default type is
whatever the conversion happens to produce, and `values` and `errors` each pay a `Chunk.fromIterable` to
get back. This is the only place `List` survives in a public signature, and it survives as an output, never
as a source.

**A type a third party demands.** An API that takes `java.util.List` or a `Seq` decides for you.

Anything else — "it was already a `List`", "prepending reads better" — is not a reason. Note what is
*absent* from this list: performance. If a profile ever shows a sequence type mattering, that measurement
is the reason, and it belongs in the code as a comment naming the measurement.

## Scope

This rule is about **sequences**. `Map` and `Set` need no rule, because ZIO publishes no competing type and
the standard library's are the obvious default. The 37 `Map` uses in `common` are not drift.

It applies to **public signatures** — what a caller names. A private local can be whatever reads best; a
`foldLeft` accumulating with `::` and reversing once is fine inside a method body, though
[building in the order you return](../sessions/2026-09-26-cutting-v0-0-5.md) is usually better anyway.

## The hazard when migrating an existing signature

Changing a `List` to a `Chunk` can silently kill a branch, and the compiler says so only as a warning.

```scala
source.claim(upTo = tokens.size).flatMap {
  case Nil    => channel.demand.offerAll(tokens) *> channel.signal.take.unit
  case claims => channel.supply.offerAll(claims) *> channel.demand.offerAll(tokens.drop(claims.size)).unit
}
```

Once `claim` answers a `Chunk`, `case Nil` matches nothing. The empty case does not fail — it falls through
to the second branch, which re-offers every token and polls again at once. A back-pressured wait became a
spin loop, and the only signal was `[E030] Match case Unreachable Warning`, which this build does not
escalate to an error.

Three of these appeared in one migration — in `PollConsumer`, and in both `nats` batch consumers. **After
changing any sequence type, compile clean and grep the output for `Unreachable` and `may not be
exhaustive`.** A dead `case Nil` is the shape to look for: it means an empty collection now takes the branch
written for a full one.

The replacement is an explicit test, which is also what the exhaustivity note above recommends:

```scala
source.claim(upTo = tokens.size).flatMap { claims =>
  if claims.isEmpty then …
  else …
}
```

## Migration

Adopted 2026-09-26, after the toolkit had already published `0.0.5`. 73 non-private signatures used `List`
at that point. The decision was to move rather than live with it, on the grounds that the only consumer —
`distributed-keyed-queue` — calls none of the affected methods, so the breaking surface is real but
unexercised. It will never be cheaper.

The move is per-module rather than one change, so each is reviewable and each can ship on its own.
