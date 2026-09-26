---
title: "2026-09-26 — the first LLM iteration, closed"
type: session
status: current
updated: 2026-09-26
tags: [llm, adapters, ports, openai, anthropic, iteration, task]
---

# The first LLM iteration, closed

Five PRs took the LLM work from a sketch in `incubator` to three published-shaped modules with two working
adapters. This records what settled, what each step cost, and what is left.

## What shipped

| | |
|---|---|
| #5 | `llm/v4` out of the incubator into `homelab-llm`. Nothing into `common`. |
| #6 | `homelab-llm-openai` — the chat-completions protocol, a preset per provider |
| #7 | `homelab-llm-anthropic`, and the client layer both adapters were missing |
| #8 | inlining pass |
| #9 | `is_error` — the last thing the port was dropping |

`homelab-llm` 14 files, each adapter 13. 341 tests across seven modules, no network anywhere.

## The three things the second adapter found

This is the part worth keeping, because it is the argument
[module boundaries](../architecture/module-boundaries.md) makes, playing out exactly as written: a shape is
only known to have settled once a **second** implementation has been written against it.

**A provider's spelling had leaked into a port.** `Registered.advertised` emitted
`{"type":"function",…}` — OpenAI's wrapper. Anthropic takes the same three things flat, so its adapter
would have had to parse that back apart to re-emit it. Became `Advertised(name, description, arguments)`,
with each adapter doing its own wrapping. One adapter could never have found this; it looked correct.

**The port was the only way through.** Both adapters were a client and a port in one class, so anything
`Model` could not carry was lost at the wire — `n`, `max_tokens`, every sampling knob. The fix was a layer,
not a wider port: see [llm adapters](../architecture/llm-adapters.md). `distributed-keyed-queue` had the
shape already and the LLM modules had skipped it.

**Two escape hatches were incoherent and one was missing.** `Model.Request.extra` could only be filled by
someone who knew which provider was behind the port, which is what the port hides — removed. `is_error`
was the reverse: the information existed (`Tool.Result.failed`) and the port had nowhere to put it, so it
survived only as text inside its own content. `Message.ToolResult` carries `failed` now.

## What it cost to get the layering wrong first

The modules were built port-first. Each adapter then discovered what the port could not hold, one field at
a time, and each discovery was filed as an adapter quirk before anyone noticed they were one thing. The
rule that came out of it — **wire first, then the port over it** — is cheap to state and was not cheap to
find.

## A correction worth remembering

Three documents and a PR description said `is_error` was carried by *both* APIs. Only Anthropic has it.
The 2026-09-25 note had it right and singular; it was broadened to "both" while being summarised into the
next day's note, and every later document inherited that without anyone rechecking the API.

A restatement is not a verification, and a summary is where the broadening happens.

## What is still open

- **No real call has been made to either provider.** Both decode paths are read off documentation. This is
  now the highest-value next thing — more than a third adapter.
- **Consecutive user turns.** `turns` emits two user turns in a row when a tool result is followed by a
  genuine user message, which its own doc says the API will not take. Needs a check of whether the API
  rejects or concatenates before it is called a bug.
- **The vocabulary page.** Recorded as a task on 2026-09-25 and still unwritten — new wording keeps being
  invented per task, and each invention costs a mapping between modules.

## The promotion question

`module-boundaries.md` says two adapters is what settles a shape, and there are now two. So `Tool`, `Model`,
`Message` and `Registry` are candidates for `common/llm/`, with `schema/` and `Registered` staying behind.

The argument against moving yet is the first open item: nothing here has spoken to a real provider. A
published artifact is a semver contract, and `is_error` was a reminder of how a missing field is found —
by using the thing, not by reading it.
