---
title: "Toolkit code review — guideline adherence and style drift"
type: research
status: current
updated: 2026-09-26
tags: [review, style, conventions, collections, chunk, list, naming, scaladoc]
---

# Guideline adherence and style drift

> One of three: [performance and security](code-review-performance-and-security.md) · [readability](code-review-readability.md).
>
> Measured at `3582a7e`, 2026-09-26. Counts are reproducible with the greps described; like every
> note here they record a point in time and are not edited to stay true.

Measured against the repository's own rules in `CLAUDE.md`, across the eight published modules. The
interesting result is the split: **the rules that are written down are followed with unusual rigour, and
almost all the drift is in choices no rule covers.**

That shape is worth stating before the detail, because it changes what to do about it. This is not a
discipline problem. It is a set of decisions that were never made once, so they were made repeatedly.

## Where the written rules hold

Every mechanically checkable rule, counted over 128 main source files:

| rule | occurrences |
|---|---|
| `throw` | 0 |
| `orElseThrow` | 0 |
| `asInstanceOf` | 0 |
| `Option.get` | 0 |
| `*Impl` / `*Live` class names | 0 |
| non-package wildcard imports | 0 |

`.get` appears 49 times and **none of them is the partial one** — they are `Ref.get`, `ZEnvironment.get[T]`
and `JwtProvider.get`, which is a port's own method. For a codebase of this size with no linter enforcing it, zero
unchecked partiality is the strongest single result in this review.

### The six exceptions, and why they are not violations

`.head` appears six times. Each sits behind something that makes it total:

| site | what enforces it |
|---|---|
| `llm/schema/Generator.scala:198` | an explicit `if cases.isEmpty then Left(…)` two lines above |
| `common/flow/batching/Serial.scala:59` | `Batch.single(in)` — one element by construction |
| `common/flow/batching/DeduplicatedSerial.scala:66` | the same |
| `common/messaging/inmemory/QueueSource.scala:138` | the `else` of an emptiness test |

`CLAUDE.md` says *"if nothing enforces it, say what the absence means and return it"* — and here something
does. Two of them even document it. They are named so the next reader does not have to re-derive the
argument, and so a future edit that removes a guard is visible as what it is.

## Where it drifts

### Bold in scaladoc: 124 markers the rule forbids absolutely

`CLAUDE.md` is unusually firm here: *"No bold in a scaladoc — none. … The rule is absolute so it needs no
judgement and can be enforced by grepping a diff for `'''`."*

| module | `'''` markers | files |
|---|---|---|
| `common` | 90 | 6 |
| `nats` | 26 | 7 |
| `telemetry` | 4 | 1 |
| `llm` | 4 | 1 |
| `auth`, `postgres`, `llm-openai`, `llm-anthropic` | 0 | 0 |

124 markers is 62 bold spans across 15 files. The distribution is the useful part: the four modules with
none are the ones written most recently. This is not drift — it is a rule adopted after `common` and `nats`
existed, and never backfilled. Every module written since complies.

And the samples are exactly what the rule describes — a claim addressed to a reviewer rather than a
statement addressed to a caller:

```scala
* '''A value rather than an effect.''' Accumulation is what validation is for …
* '''Two methods, one question: is this worth a metric?''' Both open a span …
```

**The stated enforcement has a blind spot.** Grepping `'''` misses markdown-style bold, which the codebase
also uses — 15 spans, including four in `auth` and four in `llm`, both of which score zero on the `'''`
check:

| module | `**bold**` spans |
|---|---|
| `common` | 6 |
| `auth` | 4 |
| `llm` | 4 |
| `telemetry` | 1 |

So two modules that look clean are not. If the rule is to stay grep-enforceable, the grep is
`'''\|\*\*` — and it is worth saying so in `CLAUDE.md`, since a rule whose stated check misses a quarter of
its violations trains people to trust the check.

### Collections: three conventions, no rule

`CLAUDE.md` says nothing about which sequence type to use, and the codebase has settled on different
answers in different places.

| module | `Chunk[` | `List[` | `Set[` | `Map[` | `NonEmptyChunk[` |
|---|---|---|---|---|---|
| `common` | 8 | 43 | 4 | 37 | 5 |
| `auth` | 0 | 1 | 0 | 2 | 0 |
| `nats` | 0 | 16 | 0 | 0 | 0 |
| `postgres` | 0 | 1 | 0 | 2 | 0 |
| `llm` | 21 | 15 | 14 | 0 | 2 |
| `llm-openai` | 4 | 12 | 0 | 4 | 0 |
| `llm-anthropic` | 7 | 14 | 0 | 0 | 0 |

Three conventions are visible:

- **`common` and `nats` use `List`** in ports, consistently.
- **`llm` leans `Chunk`** for conversation types and `List` for tool collections.
- **The adapters use `List`** for wire DTOs. This looked justified and is not: zio-json derives `Chunk`
  natively and emits identical JSON, and `Generator` produces a byte-identical JSON Schema for a `Chunk`
  field and a `List` field. Both were tested. There is no codec reason to prefer `List` anywhere here.

ZIO's own API is the fourth convention, and it speaks `Chunk`. So `common` converts away from it, and `llm`
converts back. 37 conversion sites result (31 `.toList`, 6 `Chunk.fromIterable`).

The clearest illustration is one file holding both answers — `common/messaging/inmemory/QueueSource.scala`:

```scala
def takeUpTo(n: Int): UIO[List[A]] = queue.takeBetween(1, n).map(_.toList)   // public port
private[messaging] def pollUpTo(n: Int): UIO[Chunk[A]] = queue.takeUpTo(n)   // internal
```

The copy exists only because the public port chose `List` and the queue underneath produces `Chunk`.

### One concept, three types, in one module

Worse than a per-module convention is a per-*type* one. A collection of tool calls is spelled three ways
inside `llm`:

| | |
|---|---|
| `Message.Assistant.calls` | `Chunk[Tool.Call.Raw]` |
| `Progress.AwaitingTools.pending` | `NonEmptyChunk[Tool.Call.Raw]` |
| `Session.dispatchAll(calls)` | `List[Tool.Call.Raw]` |

These are consecutive steps of the same loop, so the conversion is forced on every caller. Both playground
agents contain the identical round trip, once per iteration:

```scala
outcomes <- session.dispatchAll(pending.toList)                // NonEmptyChunk → List
yield Step.Continue(messages ++ Chunk.fromIterable(outcomes))  // List → Chunk
```

`NonEmptyChunk` earns its place — `AwaitingTools` with no calls is a state that should not be
representable. The `List` in `dispatchAll` does not: nothing about dispatching wants a linked list, and it
is the only reason both conversions exist.

### One case class, two collection types

`llm/Model.scala:181`

```scala
final case class Request(
  messages: Chunk[Message],
  tools: List[Advertised] = Nil,
)
```

Both are domain collections; neither is a wire type. And the same field is given a different empty in the
convenience overload thirty lines earlier:

```scala
def complete(messages: Chunk[Message], tools: List[Advertised] = List.empty)   // Model.scala:127
tools: List[Advertised] = Nil                                                  // Model.scala:182
```

`Nil` and `List.empty` for one field, in one file. Nothing breaks; it is the kind of thing that makes a
reader look twice for a distinction that is not there.

### Factory naming

| module | `make` | `apply` | `from` |
|---|---|---|---|
| `common` | 25 | 8 | 13 |
| `nats` | 17 | 12 | 0 |
| `auth` | 6 | 0 | 1 |
| `postgres` | 3 | 3 | 1 |
| `llm` | 0 | 5 | 5 |
| `llm-openai` | 0 | 0 | 4 |
| `llm-anthropic` | 4 | 0 | 2 |

The two LLM adapters are the sharpest case: same shape, same layering, written days apart, and they
disagree. `AnthropicClient.make(apiKey)` against `ChatCompletionClient.openRouter(apiKey, …)` — one names
the constructor after construction, the other after the provider.

There is a defensible rule hiding in the data: `from` for a total conversion from one type
(`ToolRequest.from(advertised)`), `make` for an effectful acquisition, `apply` for plain construction. That
is roughly what `common` does. It just has not been written down, so each new module re-derives it.

This is the [vocabulary drift](../sessions/2026-09-25-two-adapters-and-a-vocabulary-problem.md) task under
another name, and it is the second time it has surfaced.

## A deviation that is deliberate

There are no `lazy val layer` members and no `Module` objects in the published modules, though `CLAUDE.md`
prescribes both. That is not drift: the toolkit is a library, not a service, and the decision was to keep
`ZLayer` out of its API and expose `make` / scoped-effect factories instead, so a consumer wires its own
graph.

Recording it here so a future reader measuring against `CLAUDE.md` does not "fix" it. The DI section of
`CLAUDE.md` is written for services; a line saying so would save the next person the same trip.

## What to do

1. **Extend the bold grep to `**` and backfill.** 62 spans plus 15, mechanically findable, no judgement
   needed per site. The four compliant modules prove the rule is livable.
2. **Write the collection rule down.** One sentence — *`Chunk` in domain types and ports, `List` only where
   a wire codec wants it* — settles 37 conversion sites and stops the next module re-deciding. Then change
   `dispatchAll` to take a `Chunk`, which removes both conversions from every agent loop.
3. **Make `Model.Request.tools` a `Chunk`** and pick one empty spelling.
4. **Write the factory-naming rule down**, as part of the vocabulary page that is already owed.

None of this is urgent and none of it is a defect. It is the difference between a codebase that is
consistent and one that is consistently *explained* — and given how much of this repository is explanation,
the second is the one it is already paying for.
