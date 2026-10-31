---
title: "2026-10-31 — removing the \"wire\" terminology (carry-over todo list)"
type: session
status: current
updated: 2026-10-31
tags: [naming, refactoring, terminology, llm, todo]
---

# Removing the "wire" terminology

**Why:** the homelab `CLAUDE.md` rule is *no "wire" in DTO names*. A DTO-like type, its package, or a
module holding such types is named after the concrete protocol. The same goes for comments and docs where
"wire" would mean a DTO: name the specific type, or the protocol's format ("the chat-completions JSON")
when no single type fits. **When the right name isn't obvious, propose one and get approval before using
it.**

**Out of scope, and staying:**
- `Wire[A]` in `common/messaging/inmemory`: it has real wire semantics (one queue joining a producer and a
  consumer), not a data shape.
- "Wire" as transport ("reaches the wire", "crosses the wire", "travels on the wire"). The one
  "over the wire" became "over the network": `docs/architecture/where-the-agent-sits.md` 104, where the
  tool schema goes to the provider in the HTTP request. Done on this branch.
- Byte codecs and serdes ("wire codecs", "wire bytes" in `Codec`, `Serde`, `MessageCodec`).
- Protocol rules ("where a tool meets the wire", "the wire's one structural demand").
- "Wiring" in the DI sense.

**State:** nothing changed in this repo yet. Line numbers are as of 2026-10-31 (`main` after #47).

## 1. Name: approved and renamed

| Where | Now | Proposed | Why | Status |
|---|---|---|---|---|
| `docs/learning-material/tool-calling-wire-format.md` | file name, title "…on the wire", tag `wire-format`, table header "Wire fact" | `tool-calling-chat-completions-format.md`, title "What a tool call actually looks like in the chat-completions JSON", tag `chat-completions`, header "JSON fact" | the page documents the request/response JSON, i.e. the DTOs | done |

Renamed with `git mv`. Both links to it are updated, including the one in a dated session:
`docs/README.md` 15 and `docs/sessions/2026-08-17-llm-tool-calling.md` 24. (Line 81 of that session is prose, not a link, so it stays.)

## 2. Comments where "wire" means a DTO

The LLM adapters' request/response types describe themselves as "as the wire carries it", or their fields
as "which the wire spells `type`". Name the JSON instead.

- [x] `llm-openai/request/MessageRequest.scala` 62 "One message of a conversation, as the wire carries it" → "as the chat-completions JSON carries it"
- [x] `llm-openai/request/CallRequest.scala` 15 "which the wire spells `type`" → "which the JSON spells `type`"; 36 "as the wire carries it" → "as the chat-completions JSON carries it"
- [x] `llm-openai/request/ContentPart.scala` 16 "which this is the wire's spelling of" → "which this is the chat-completions spelling of"; 33 "as the wire carries them" → "as the chat-completions JSON carries them"
- [x] `llm-openai/request/ToolChoice.scala` 38 "The wire is a union of a string and an object here" → "The JSON is a union of…"
- [x] `llm-openai` test `CompletionRequestSpec.scala`:
  - 12 "What a request looks like on the wire" → "What a request looks like as JSON"
  - test name 18 "the name the wire uses" → "the name the JSON uses"
  - test name 69 "a raw part reaches the wire as it was written" → "… reaches the JSON as it was written"
- [x] `llm-mistral/request/CallRequest.scala` 17, `Prediction.scala` 11, `response/FailureResponse.scala` 17 "which the wire spells `type`" → "which the JSON spells `type`"
- [x] `llm-mistral/request/ContentChunk.scala` 147 "The wire takes either a bare string or an object here" → "Mistral's JSON takes…"
- [x] `llm-mistral/request/ToolChoice.scala` 41 "The wire is a union…" → "The JSON is a union…"
- [x] incubator `llm/v1/sketchOne.scala` 68 "Wire model (subset of the OpenRouter streaming chunk…)" → "Streaming-chunk model (subset of…)"; 187 "one wire chunk" → "one streaming chunk"

## 3. Docs where "wire" means a DTO

- [x] `docs/architecture/sequence-types.md` 15 "the adapters on `List` for wire types" → "… for their request/response types"
- [x] `docs/architecture/llm-adapters.md` 20 "the one built wire first: the client and its types came before `MistralModel`" → "the one built client first"; 42 "**wire first, then the port over it**" → "**the client and its types first, then the port over it**"
- [x] `docs/learning-material/tool-calling-chat-completions-format.md` (was `tool-calling-wire-format.md`): title, heading, tag and the "JSON fact" header, with the §1 rename

Bump `updated:` in the frontmatter of each doc you edit.

## 4. Dated records and research drafts: decision needed

By the docs rules these are written once and not edited to stay true (`docs/research/README.md` says so
explicitly). **Default: leave as written**, even where "wire" means a DTO, except the two links in §1.
Confirm or override before touching them.

- `docs/research/`:
  - `a-mistral-adapter.md` (6, 135, 140, 160)
  - `code-review-guidelines-and-style.md` (120, 170, 219)
  - `grpc-workload-module.md` (23, 43, 139, 153, 175)
  - `llm-conversation-model.md` (13 lines)
  - `llm-design-exploration.md` (43)
  - `mailbox-design.md` (45, 84, 329)
  - `messaging-design.md` (81, 108, 223, 225, 300)
  - `reading-what-the-model-asked-for.md` (50)
  - `workflow-executor-store-projection.md` (384)
- `docs/sessions/`:
  - `2026-08-09-actor-worker-design-review.md` (82)
  - `2026-08-17-llm-tool-calling.md` (81; its link on 24 is updated, see §1)
  - `2026-09-25-v4-shapes-and-the-module-question.md` (14)
  - `2026-09-26-llm-first-iteration-closed.md` (38, 51)
  - `2026-09-26-the-missing-client-layer.md` (21, 49, 68, 72)

## 5. Done when

- [x] The §1 rename is approved and done.
- [x] §2 and §3 are done (branch `rename-wire-dto-comments`).
- [ ] `sbt testFull` passes.
- [ ] Everything this finds is out of scope (see the top) or in §4:
  ```bash
  grep -rnE '\b[Ww]ires?\b|\b[Ww]ire-[a-z]|[a-z]Wire|Wire[A-Z]|wire[A-Z]|\bWire\b' --exclude-dir={target,.git,.bloop,.metals,.bsp} .
  ```
