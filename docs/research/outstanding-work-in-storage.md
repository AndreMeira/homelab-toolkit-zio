---
title: "Outstanding work, kept in storage — three shapes for the feature Standing was reaching for"
type: research
status: draft
updated: 2026-09-23
tags: [llm, agent, tool, transcript, subagent, persistence, what-if]
---

# Outstanding work, kept in storage

[`Standing`](tool-result-standing-removed.md) was removed because it put a fact in a domain type that
belongs in storage. The *feature* it was reaching for is still real. This note states that feature
plainly and lays out three ways to build it out of a store, with what each costs.

Nothing here is chosen. It exists so that the next person to want this does not re-derive it.

## The feature

A tool can start work that outlives the call that started it — a subagent, a long render, a job queued
elsewhere. The call answers immediately ("working; the result will follow"), the conversation carries on,
and the result arrives later through a nudge.

The question nobody can answer today is:

> **Does this conversation still owe results from work it started?** And if so, which work?

## Two questions, very different costs

They look like one question and they are not:

- **"Is *this* conversation owed anything?"** — asked while handling a conversation, with its id in hand.
  Every shape below answers it cheaply.
- **"*Which* conversations are owed anything?"** — asked by a sweep looking for work whose nudge never
  arrived. This one needs an index, and it is the only question that justifies building anything: without
  it, a lost nudge is a conversation that waits forever and nobody notices.

Design for the second, or do not build it. The first is satisfiable by reading the transcript you already
have.

## Shape A — a side store

A store of its own, keyed for the sweep:

```scala
trait PromiseStore:
  def promise(conversation: Conversation.Id, handle: Handle, call: Tool.Call.Id): IO[E, Unit]
  def settle(conversation: Conversation.Id, handle: Handle): IO[E, Unit]
  def outstanding(conversation: Conversation.Id): IO[E, Set[Handle]]
  def stale(olderThan: Duration): IO[E, Chunk[Conversation.Id]]
```

**Buys.** The sweep is a first-class query rather than a scan. The rows can carry what the conversation
has no business holding — when it started, which child, how many attempts, last seen alive — which is
exactly the state a sweep needs to decide whether to re-nudge or give up. The transcript stays words.

**Costs.** Two writes that are not one. Appending the tool's answer and recording the promise are separate
stores, so a crash lands between them. The direction matters:

- promise first, then message → a promise with no answer in the conversation: a false positive, which the
  sweep can see and clean.
- message first, then promise → work running that nothing recorded: a false negative, invisible to the
  sweep, recoverable only because the nudge itself carries the call id.

Message-first is the safer order, and only because the nudge can reconstruct. Also: a second port, a
second adapter per backend, a second thing to GC.

## Shape B — a column on the message store

The transcript's rows gain the field; the domain type does not. The store takes `(Model.Message, Standing)`
on write and returns `Model.Message` on read, and answers `outstanding(id)` as a filter on its own table.

**Buys.** One write, atomic by construction — the promise and the words it accompanies cannot land apart.
No drift is representable. No new port.

**Costs.** The store's row is richer than the value it hands back, so the write path and the read path
stop being mirror images. That is ordinary in DDD and still the thing a reader trips over. Every
transcript backend has to implement it, in-memory adapter included. The sweep is a scan unless the column
is indexed, and indexing a sparse column across every conversation is the wrong shape for a sweep that
expects a handful of hits.

Adding it later is a column with a default, so this is the cheapest option to defer.

## Shape C — a second row in the same stream

The promise is not a field on a message; it is a row of its own, in the conversation's own stream, of a
kind that is not a message:

```scala
enum Row:
  case Said(message: Model.Message)
  case Promised(handle: Handle, call: Tool.Call.Id)
  case Settled(handle: Handle)
```

Reading the conversation for a request keeps only `Said`. What is owed is a fold over the rest: promised,
less settled — a pure function of the conversation, with no store involved in answering it.

Note what this costs that it did not when the domain had its own entry type: the toolkit's transcript is
`Chunk[Model.Message]` today, so this shape reintroduces a wrapper, and the projection to a request stops
being the identity. That is the real price of C now, and it is the same price the `Entry` type was paying
for `Standing` before both were removed.

**Buys.** Atomic if the pair is appended in one batch, like B. One store, no nullable column, no separate
lifecycle. The reading stays a pure function of the conversation, which is the property the transcript is
built around.

And the objection that killed `Standing` — a second record shadowing the tool's own row — **goes away if
the stream is where the tool keeps its record.** A tool that writes `Promised` into the conversation
instead of into a private table has one record, not two. That only holds while the tool needs nothing
beyond the handle and the call id; the moment it wants liveness or attempts, it has a private table again
and the shadow is back.

**Costs.** Rows that never reach a model live in the conversation, so anyone reading the store raw sees
things a transcript "shouldn't" contain, and every projection has to filter. The sweep is the same scan
problem as B. And the fold is over the whole conversation, so the cost of asking grows with its length —
fine at chat scale, not at log scale.

## Shape D — nothing in the toolkit

Today's answer. The tool owns a table keyed by call id; the nudge carries the call id; the runner reads
the row and appends the result as a message.

**Buys.** No toolkit machinery, no port, no schema. The tool's table holds the real state and can check
whether the child is actually alive, which none of A–C can.

**Costs.** No generic sweep. A supervisor has to ask every tool's storage separately, and a tool nobody
wrote a sweep for leaks stalled conversations silently.

## Side by side

| | A: side store | B: column | C: second row | D: nothing |
|---|---|---|---|---|
| Atomic with the answer | no | yes | yes | n/a |
| Sweep across conversations | query | scan | scan | per tool |
| New port | yes | no | no | no |
| Drift possible | yes | no | no | n/a |
| Answering costs | O(1) | O(1) | O(conversation) | n/a |
| Carries liveness | yes | no | no | yes |
| Cost to add later | new port | column + default | a wrapper type + migration | — |

## What to reach for

**C first**, if the trigger is "a conversation can stall and nobody notices" and the promises are simple.
It keeps the one property worth protecting — what is owed is a function of the conversation, not a second
record to keep in step. Since the transcript is now a plain `Chunk[Model.Message]`, the cost is a wrapper
type around the message plus the two promise rows, which is a real cost rather than a case on a type that
already existed.

**A** if the sweep is the point, or the promises need liveness. Once you want to ask "which of these
children is still running", you are describing rows with their own lifecycle, and pretending they are
conversation events makes both worse.

**B** only if the sweep never materialises and you want the answer available without widening the domain
type. It is the cheapest to add and the least useful.

The honest trigger for any of this is the same: **a nudge that never arrives.** Until a lost nudge has
actually stranded a conversation, D is not costing anything, and the thing to build first is whatever
makes a lost nudge visible — which may be a metric rather than a store.
