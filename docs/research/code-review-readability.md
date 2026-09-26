---
title: "Toolkit code review — readability for a senior ZIO developer"
type: research
status: current
updated: 2026-09-26
tags: [review, readability, scaladoc, types, zio, scala3, onboarding]
---

# Readability, for someone who already knows ZIO

> One of three: [performance and security](code-review-performance-and-security.md) · [guideline adherence and style drift](code-review-guidelines-and-style.md).
>
> Measured at `3582a7e`, 2026-09-26. Counts are reproducible with the greps described; like every
> note here they record a point in time and are not edited to stay true.

The question this asks is narrow: a senior Scala developer who knows ZIO well opens this repository for the
first time. What helps, what slows them down, and where would they be actively misled?

Not "is it documented" — it is, heavily. Whether that documentation *pays* is one of the findings.

## What a reader gets for free

### The Scala 3 is plain

Counted across the eight published modules:

| feature | occurrences |
|---|---|
| `inline` | 1 |
| `Mirror` | 1 |
| `summonFrom` | 0 |
| `erasedValue` / `constValue` | 0 |
| match types | 0 |
| `derives` | 37 |
| `using` | 27 |
| `opaque type` | 8 |

This is the single biggest thing in the codebase's favour. A library doing schema derivation and JSON
codecs could easily have been a macro thicket; this one stays inside the features a working Scala 3
developer already uses. There is nothing here that requires reading the compiler's documentation to follow.
The type-level experiments exist, but they are in `incubator`, which is not published and not on anyone's
path.

### Union error types, used the same way every time

Eighteen signatures pair errors with `|`, and the pairing is consistent:

```scala
IO[AdapterError | UnauthorisedError, JwtClaim]
IO[AdapterError | UnauthorisedError, User.Authenticated]
```

"This fails because the infrastructure broke, or because the caller is not allowed." A reader meets the
idiom once and then reads every subsequent signature at a glance. This is the kind of Scala 3 feature use
that earns its novelty — the alternative is a sealed hierarchy that says less.

### The shape repeats

One type per file, `request/` and `response/` where there is a protocol, `error/` where there are adapter
failures, the same `Client` → `Model` layering in both LLM adapters. After two modules a reader can predict
where something lives in the third. That is worth more to a newcomer than any individual doc.

## What it costs

### A doc block every seven lines of code

| | |
|---|---|
| scaladoc blocks | 1,052 |
| lines of code | ~6,900 |
| lines of comment | ~7,000 |
| comment share | 47%–60% per module |

There is more prose than code in every module. `common` runs 1.54 comment lines per code line.

**The interesting part is that this is not verbosity.** Measured against the repo's own *"three to five
lines of prose, and then stop"*:

| prose lines per block | blocks | share |
|---|---|---|
| 0 (one-liner or tags only) | 125 | 11% |
| 1–5 (within the rule) | 829 | 78% |
| 6–10 | 73 | 6% |
| 11–20 | 22 | 2% |
| 21+ | 3 | 0% |

89% of blocks are at or under the limit. The density comes from *breadth* — nearly every member documented,
each with `@param` and `@return` — not from any block running long. The convention asks for that
explicitly, so the codebase is doing what it was told.

The cost is still real, and it is navigational rather than intellectual. A reader scanning `PollConsumer`
for the settle path scrolls through 659 lines to find perhaps 250 of code. Folding is the practical answer
and every IDE does it, which is presumably why this has not bitten. But a reader on GitHub, in a diff, or
in a terminal has no folding, and that is where a lot of first contact happens.

No change is recommended. This is a deliberate trade the repository has made consistently, and reversing it
would cost more than it returns. It is recorded so the trade is known rather than assumed.

### The 25 long blocks are where reasoning leaked into source

| block | file |
|---|---|
| 40 lines | `common/messaging/PollConsumer.scala:119` |
| 24 lines | `common/messaging/PollConsumer.scala:45` |
| 21 lines | `telemetry/OtelMonitor.scala:16` |
| 17 lines | `telemetry/OtelMonitor.scala:212` |
| 17 lines | `common/processing/Mailbox.scala:12` |
| 15 lines | `common/messaging/PollConsumer.scala:448` |

Three of the top six are one file. These are the components where the design genuinely is hard — a
demand-driven poll consumer with four queues and an interruption story is not going to be self-evident —
and the prose is good. But a 40-line comment is a design document that has been pasted above a method, and
`CLAUDE.md`'s own answer is that such reasoning belongs in `docs/`, where it can be dated and superseded.

The practical cost to a reader: the argument for why the design is correct sits in the one place it cannot
be revised independently of the code, and cannot be found by someone who does not already know to open that
file.

Moving the three `PollConsumer` blocks into an architecture page, leaving a one-line pointer, would make
the file readable and the argument findable. That is the single highest-value readability change available.

### Five type parameters, twice

```scala
trait Workflow[-R, +E, I, S, +O]
trait Stateful[E >: AdapterError <: ApplicationError, K, S, I, O]
```

Five is a lot to hold, and `I`, `S`, `O` are single letters carrying real meaning. In `Workflow`'s favour,
all five are documented with `@tparam` immediately above, and the variance is correct and deliberate. A
senior reader will manage. A reader meeting `Workflow[-R, +E, I, S, +O]` in *another* file's signature,
without the doc in view, will not reconstruct which is which.

`Stateful`'s `E >: AdapterError <: ApplicationError` is the harder one — a doubly-bounded error parameter
is unusual enough that it reads as a puzzle before it reads as a constraint.

Neither is worth changing for its own sake. Both are worth knowing about if either is ever the model for a
third type.

### One real defect

`llm/Tool.scala:271`

```scala
/** What writes a failure. Built once, like the schema it comes from. */
/** What writes a failure. Built once, from a schema derived once. */
private val errorEncoder = …
```

Two scaladoc blocks, differently worded, for one `val`. Scala accepts it — the first becomes a floating
comment — so nothing catches it. It is the only instance in the repository, and it is a leftover from an
edit rather than a pattern.

### Where a reader would be misled

Two places where the text is not wrong but points somewhere unhelpful:

- **`common`'s DI conventions do not apply to `common`.** `CLAUDE.md` prescribes `Module` objects and
  `lazy val layer`; the toolkit deliberately has neither, because it is a library. A reader who arrives via
  the conventions will look for wiring that was intentionally left out. The style review covers this; it
  belongs here too, because the confusion is a reading problem before it is a style one.
- **`Session.dispatchAll` takes a `List`** while everything around it is `Chunk`. A reader will look for
  the reason and there is not one. Unexplained inconsistency costs more attention than explained
  complexity.

## Verdict

For its intended reader this is a comfortable codebase. The features are plain, the shape repeats, the
errors are typed honestly and the invariants are written down. A senior ZIO developer would be productive
in it quickly, and would rarely have to guess.

The friction is concentrated and cheap to remove:

1. **Move `PollConsumer`'s three long doc blocks into `docs/architecture/`** and leave pointers. Biggest
   single improvement, and it makes the argument revisable.
2. **Delete the duplicate scaladoc** at `Tool.scala:271`.
3. **Make the collection types consistent** — the readability cost of `dispatchAll` taking a `List` is
   paid by every reader of the agent loop. See the style review.
4. **Add a line to `CLAUDE.md`** saying the DI section is for services, not for the toolkit.

What is deliberately *not* recommended: reducing the documentation. It is disciplined, it is within its own
stated limits 89% of the time, and it is the reason a reader can pick up an unfamiliar module and know what
a type is for. The density is the price of that, and it is worth paying.
