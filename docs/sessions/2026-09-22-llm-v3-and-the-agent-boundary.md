---
title: "LLM v3 — the conversation design, a robot policy read for comparison, and where the module would live"
type: session
status: current
updated: 2026-09-22
tags: [llm, agent, tool-calling, incubator, promotion, modules, session]
---

# 2026-09-22 — v3, and the boundary an agent sits on

A long design cycle over the LLM incubator: what a conversation is, how a loop reads a tool's answer, and —
after reading a published robot-control framework that solves the same loop — what the toolkit's version
should keep. Nothing promoted. Two branches of commits, one sketch that compiles, three research notes and
an architecture article.

## What landed in `llm/v3`

`Tool` grew the parts a loop needs around a registry, and most of it came from an argument that changed
direction at least once.

- **`Result` is an enum.** A failure is `Failed(reason)`, not an `"error: "` prefix, so "did this fail" is a
  property rather than a substring — and a failure *structurally* cannot carry a standing, because it
  started nothing. The `{isError, reason}` envelope goes on at `render`, once. Making `render` lazy
  immediately double-enveloped every failure until the raw reason moved inside `Failed`, which is a better
  shape anyway: a loop inspecting a failure reads prose, not JSON.
- **`Result` carries its own `Schema`,** so `Outcome` holds a `Result[?]` and needs nothing else. The model
  reads `result.render`; a loop that knows what it wants matches the value the tool made. One field, two
  readers, no second decode.
- **`Standing`** — `Promised` / `Delivered` — so what a conversation still owes is promised less delivered.
- **`permits` is effectful.** A permission is a lookup, and a pure signature forces every answer into `Ctx`
  before anyone knows which tools will be asked about. `forSession` asks once, at binding, so the tools
  advertised and the tools callable are the same list; a lookup that cannot answer refuses the session
  rather than guessing either way.
- **An aborted handler tells the model only that it failed.** The message on an `ApplicationError` is
  written for an operator and can name a table or a credential. Arguments that will not parse are the
  exception — those are the model's own to fix.
- **`Model`**, the chat-completions port, plus the glue from a completion and its outcomes to the turn a
  transcript keeps. **`Transcript`**, which reads what a conversation is waiting for from its messages and
  from nothing else.

## Three arguments worth keeping

**Terminal tools.** Ending a run is not something a `Tool` can do: `dispatchAll` runs what was asked and
returns, and only the loop can stop the loop. A `terminal` flag on `Tool` was proposed and rejected —
whether a call ends a run has to be weighed against what the run still owes, which is the loop's to decide
and no property of a tool could answer. A `Terminal` interface was built and then deleted, because the
better answer needed neither: the tool returns an `Ending` enum, dispatch keeps the value beside its
rendering, and the loop matches on it. Endings are data, the same way failures are.

**What survives, and what does not.** The produced value lives one turn, in memory; the rendering is what
reaches the transcript. So a standing is on the message and survives a restart, while a produced value is
beside it and does not. Anything a loop must still know after a resume belongs in the first. The rule that
comes with matching by type: a type the loop acts on must be produced by exactly one tool, or the name is
back as the disambiguator.

**Who wrote the message.** The API returns exactly one message per call — the assistant's. Everything else
in a transcript we wrote ourselves. That settles why `standing` sits comfortably on a tool result (a message
no provider authors) and why the verbatim-replay risk is confined to assistant turns alone.

## GPT-Policy, read for comparison

[`cheng-haha/GPT-Policy`](https://github.com/cheng-haha/GPT-Policy) drives a robot arm with a VLM. Written
up in `research/agent/gpt-policy.md`; the loop is the same shape as ours and the differences all trace to
one operator and one arm. What it does better, and we took:

- **The tool surface is shaped around what the model cannot do.** `locate_point` exists because a VLM cannot
  read depth off one RGB frame; the measured pose is in every observation so it need not integrate its own
  motion; `check_path` lets it test before acting. Our tools are shaped by what the *domain* offers, which is
  a different design input.
- **Re-check authority between deciding and acting** — `check()` before dispatch, so a late answer never
  reaches the executor. The same rule dkq states for a consumer whose lease has lapsed, and we have it on
  only one side.
- **Say the non-replay guarantee out loud.** Their retry path states no robot action is replayed; our notes
  discuss at-least-once at length without ever writing the equivalent sentence.

The sketch in `llm/v3/playground` expresses their loop with our pieces. It compiles, implements no hardware,
and found two things prose had not: `Transcript.progress` reads a turn with no tool calls as *finished*,
which is a text-agent assumption and wrong for a loop whose world moves on its own; and the observation must
be re-read from the rig each step, so the transcript is not the whole state once the world moves.

## Where the module would live

Assessed for promotion and measured rather than estimated. v3 compiles clean under the production scalac
flags (`-Wvalue-discard`, `-Wnonunit-statement`, the `-Wconf` escalation) that the incubator disables, has
no partial escapes, and carries 31 specs.

**It should not go in `common`.** The dependency test decides it — `Tool` needs `zio-schema-json` and
`zio-json.ast`, and `common` has neither; pulling zio-json 0.10 into the published core for one use case is
what quarantine is for. But the stronger reason is what `common` claims: everything in it is infrastructure
any service might need, and only an agent needs this. The port is also chat-completions-shaped rather than
neutral — `Consumer` earns its place by being transport-agnostic with NATS quarantined, and `Model` has no
equivalent neutral form. Promoting it would publish one provider's vocabulary as the toolkit's.

So: **an `llm` module depending on `common`**, the same shape as `nats`, `auth`, `postgres` and `telemetry`.

**One module, not two, for now.** An OpenRouter adapter brings sttp, which by the quarantine rule argues for
splitting ports from adapter — the right shape if the homelab grows, premature today. What keeps the split
cheap is organising packages along that seam from the start: `homelab.llm.*` for the ports, `Tool` and
`Transcript`; `homelab.llm.openrouter.*` for the HTTP adapter. Then the split is a directory move and a
build stanza. What would make it expensive is the usual leak — an sttp type in a port signature — which
nothing will warn about while it is all one compilation unit.

## What promotion still needs

- **It is not only v3.** `Tool` imports `v2.JsonSchema`, which brings `JsonSchemaDerivation` and `Schemas`:
  ~739 more lines and 18 more specs to port. About 1,200 lines move, not 467.
- **`Model` has no adapter and no spec.** The port exists and nothing implements it; the glue has real logic
  and is untested. The toolkit's own convention is that a port ships with an in-memory counterpart, and the
  design note calls a record/replay model adapter the highest-leverage test infrastructure here.
- **`Transcript.progress` encodes a text-agent assumption** the playground exposed. Promoting it would
  publish that as a general algorithm.
- **Naming is open** — `Model.Request` / `Model.Completion` nest request and response in an umbrella, which
  the conventions warn against. Cheap now, annoying after publication.
- **An ADR under `docs/decisions/`** when the module actually moves: the boundary and the zio-json version it
  carries are what the next person will want the reasoning for.

## Also

An IntelliJ rename with "search in comments and strings" left on rewrote a word inside scaladoc and string
literals across 33 files — `messaging/nats` v1–v5, `processing/actor`, `auth`, `llm/v1`, `llm/v2` and three
research docs. Nothing broke the build; it was all prose reading as nonsense, and one string literal that a
test caught. Reverted rather than committed. The tell is a diff where every changed line differs from its
original by exactly one word, in files nobody opened.
