---
title: "2026-09-25 — two adapters, and a vocabulary that drifts one task at a time"
type: session
status: current
updated: 2026-09-25
tags: [llm, adapters, openai, anthropic, naming, vocabulary, task]
---

# Two adapters, and a vocabulary that drifts one task at a time

## What was built

**`homelab-llm-openai`** — the chat-completions protocol, not one provider. The DTOs are a strict subset of
what OpenAI defined and the same shape is served by the gateways, the fast-inference hosts and local
servers; a provider is a preset on `ChatCompletionModel`'s companion. Merged as #6.

**`homelab-llm-anthropic`** — the Messages API, which shapes a conversation differently rather than
dialectally: two roles, the instructions in a field of their own, a tool's answer as a block inside a user
turn, a tool's arguments as an object rather than a string. `Message` survived all of it, which is the
answer to whether it was chat-completions-shaped. Open as #7.

**The port change the second one forced.** `Registered.advertised` was emitting
`{"type":"function","function":{…}}` — one provider's spelling, sitting in a port. It answers an
`Advertised` now, and each adapter shapes it. Without that, the Anthropic adapter would have parsed the
first adapter's JSON back apart to re-emit it.

That is what a second implementation is for, and it is the argument the
[module boundaries](../architecture/module-boundaries.md) note makes for not lifting anything into `common`
yet.

## The problem worth acting on

**New wording is being invented per task, and every invention costs a mapping.** Not one bad name — a
pattern of small divergences, each defensible where it was written and collectively a vocabulary a reader
has to learn twice.

What the session turned up, in order of how much it cost:

- **Two states of one thing, named three ways.** `Tool.Call` has `Raw` / `Decoded`. `Message.Content` has
  `Text` / `Raw`. The Anthropic response block was written as `Read` / `Unread` before being renamed to
  `Raw` / `Decoded` — the pair that already existed, for the identical distinction.
- **Three words for what came back.** `Result` (what a tool produced), `Outcome` (a dispatch), `Completion`
  (a model call). Each is defensible; together they are three nouns a reader has to keep apart, and two of
  them wrap the third.
- **The same failure under two names.** `Basic.Exhausted` and `Chat.BudgetExhausted`, in two files of the
  same package.
- **A conversion named eight ways.** `from`, `fromCompletion`, `fromOutcome`, `fromRecord`, `body`,
  `blocks`, `parts`, `completion` — all "turn this into that", differing by module and by the day.
- **`MessagesRequest` and `MessageRequest`** existed in one package, one letter apart, until this session.

Where it went right is as informative: both adapters' failures are `Unavailable` / `Refused` / `Malformed`
/ `Rejected`, and that was not coordination — the second copied the first because the first was there to
copy. **The vocabulary holds when a precedent is visible and drifts when it is not.**

## The task

Write down the toolkit's vocabulary, as a page in `docs/architecture/`, and check new code against it
rather than against memory. It should settle at least:

- the pair for *as it arrived* versus *what it means* — `Raw` / `Decoded`, everywhere, including where a
  third state exists
- one noun for what an operation produced, and what the wrappers around it are for
- the shape of a conversion — `from<Source>` and when a plainer verb is right instead
- the shape of a factory — `make` versus a name per preset
- what a failure of a given kind is called, since the four adapter kinds are already stable

Two rules make it stick rather than rot: **a new word needs a reason the page does not already answer**,
and **a name that differs from an existing one by a letter is a bug, not a variation**.

The cost of not doing it is not ugliness. It is that every pair of modules needs a mapping in the reader's
head, and that is the thing the toolkit exists to avoid.

## State

`llm-anthropic-adapter`, two commits, PR #7 open and green. Uncommitted on top: the derivation pass that
took the hand-written encoders out of both adapters, the `Raw` / `Decoded` rename, and the shape parity
between the two modules.

317 tests across seven modules.
