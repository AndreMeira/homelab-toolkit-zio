---
title: "2026-09-26 — the layer the LLM adapters are missing, and where DKQ got it right"
type: session
status: current
updated: 2026-09-26
tags: [llm, adapters, ports, layering, dkq, task]
---

# The layer the LLM adapters are missing

> Done. The shape this argued for is now built and described in
> [how an LLM adapter is laid out](../architecture/llm-adapters.md); this page is the reasoning that
> got there.

## The observation

Both LLM adapters drop things, and the reason is not that the port is too thin. It is that there is nothing
underneath the port to drop them *to*.

`ChatCompletionModel` is a client and an adapter in one class. Its only method is `Model.complete`, so
anything the port cannot carry has nowhere to go and is lost at the wire:

- **`n`** — OpenAI returns several choices; the first is taken and the rest dropped.
- **`max_tokens`** — Anthropic requires it; a default is invented inside the adapter.
- **`is_error`** — Anthropic carries it on a tool result; nothing holds it.

Each was written down as an adapter quirk. They are one thing: a capability with no home.

## Where the pattern already exists

`distributed-keyed-queue`'s client is three layers, and its middle one is what the LLM modules skipped:

| | |
|---|---|
| `queue/model/` + `codec/` | Scala types for the protocol |
| `QueueClient` | one method per RPC, **nothing withheld** |
| `queue/managed/` | `ManagedConsumer`, `ManagedProducer` — implements `common`'s ports |

`QueueClient`'s own doc states the rule: *"One method per RPC and nothing withheld: outcomes are reported
per message rather than per claim, the retry delay is the caller's, and the batch size is stated rather
than inferred."*

And `Provider` hands out the ports while `QueueClient` stays reachable, so a caller who needs a raw
`dequeue` has one.

## What this means for the LLM modules

```scala
// close to the wire — the provider's call, everything it takes and everything it returns
trait ChatCompletionClient:
  def complete(request: CompletionRequest): IO[ChatCompletionError, CompletionResponse]

// the port, built on it — the general case
final class ChatCompletionModel(client: ChatCompletionClient) extends Model[ChatCompletionError]
```

`n` is then not dropped: the client returns every choice, and the *model* takes the first because a
`Model.Completion` holds one. A caller wanting alternatives holds the client. `max_tokens` is a field on
`CompletionRequest` rather than a default hidden in an adapter. What the port cannot express stays
reachable by whoever holds the concrete type.

This subsumes the note about the port being too thin. The answer is not to widen `Model` — it is that the
layer below `Model` should have been there from the start, and writing the port first is what hid its
absence.

## The order to work in

DKQ was built wire-first: the client came before the adapters, so the adapters were written against
something that already had everything. The LLM modules were built port-first, and each adapter then
discovered what the port could not hold, one field at a time.

**Wire first, then the port over it.** The port is for the general case; the adapter keeps the provider's
API as it is.

## Tomorrow

Split `llm-openai` into `ChatCompletionClient` and `ChatCompletionModel`, then `llm-anthropic` the same
way. The specs mostly move down a layer — the stub backend tests the client, and the model gets small tests
about what it narrows and why. `is_error` is the one that still needs a decision above the client, since
`Message.ToolResult` has nowhere for it.

## State

`llm-anthropic-adapter`, three commits, PR #7 open and green. 317 tests across seven modules.

Parked with this: the [vocabulary page](2026-09-25-two-adapters-and-a-vocabulary-problem.md), which is the
same kind of problem — a shape settling one task at a time instead of being written down once.
