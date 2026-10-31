---
title: "2026-10-31 — removing the \"wire\" terminology (carry-over todo list)"
type: session
status: current
updated: 2026-10-31
tags: [naming, refactoring, terminology, messaging, llm, todo]
---

# Removing the "wire" terminology

**Why:** the homelab `CLAUDE.md` now says *don't say "wire"*. Name the concrete protocol instead (`proto`
for protobuf-generated types, HTTP/JSON, NATS, the chat-completions JSON, …), in identifiers, packages,
artifacts and prose. `Wire` is never acceptable for a DTO, a DTO package or a module holding DTOs. **When
the right name isn't obvious, propose one and get approval before using it.** "Wiring" in the
dependency-injection sense is fine ("Wire your code against `common`'s ports" stays).

**State:** nothing changed in this repo yet. This page is the inventory, so the work can resume after
something else. Line numbers are as of 2026-10-31 (`main` after #46).

## 1. Names: need approval before renaming

| Where | Now | Proposed | Why | Status |
|---|---|---|---|---|
| `modules/common/.../messaging/inmemory/Wire.scala` | `final case class Wire[A]` (+ companion `make` / `connect` / `fromQueue`) | `QueuePipe[A]` (file `QueuePipe.scala`) | One queue with both ends, extends `Pipe`. Matches its siblings `QueueProducer` / `QueueConsumer` / `QueueSource`. Alternative: `Channel`. | awaiting approval |
| `docs/learning-material/tool-calling-wire-format.md` | file name, title "…on the wire", tag `wire-format` | `tool-calling-chat-completions-format.md`, title "What a tool call actually looks like in the chat-completions JSON", tag `chat-completions` | The page documents the OpenAI chat-completions JSON shape. | awaiting approval |

**`Wire` is published** in `homelab-common` (0.0.5), so renaming it is a breaking change for the next
release. Nothing outside this repo uses it (checked in `distributed-keyed-queue` and
`homelab-registration-service`). References to update along with it:
- `InMemoryMessagingSpec.scala` 4, 9, 16, 18, 20, 21, 27, 29, 30
- `README.md` 86
- `docs/architecture/messaging.md` 70
- the cross-repo `research/library-design/` notes (tracked separately)

**The doc rename breaks two links**, which must be updated even though one of them is in a dated session:
`docs/README.md` 15 and `docs/sessions/2026-08-17-llm-tool-calling.md` 24, 81.

## 2. Published modules: code comments

Rewrite each to the concrete protocol. Suggested wording in brackets.

- [ ] `common/data/Codec.scala` 7, 12, 17, 32, 49 — "wire codecs", "putting a value on a wire" [byte codecs; to bytes / from bytes, the transport's format]
- [ ] `llm/Message.scala` 59, 83; `Model.scala` 85; `Registered.scala` 11; `Registry.scala` 15; `Tool.scala` 394; `schema/Generator.scala` 38 [the provider's request JSON / the API]
- [ ] `llm-openai/request/CallRequest.scala` 15, 36; `ContentPart.scala` 16, 33; `MessageRequest.scala` 62; `ToolChoice.scala` 38 [the chat-completions JSON]
- [ ] `llm-openai` test `CompletionRequestSpec.scala` 12, 18, 69, including the test **names** "the name the wire uses" and "reaches the wire" [the chat-completions JSON]
- [ ] `llm-mistral/request/CallRequest.scala` 17; `ContentChunk.scala` 147; `Prediction.scala` 11; `ToolChoice.scala` 41; `response/FailureResponse.scala` 17 [Mistral's JSON]
- [ ] `nats/mailbox/Mailbox.scala` 34; `MessageCodec.scala` 11; tests `NatsSpec.scala` 603, `mailbox/MailboxSpec.scala` 57 [the NATS message bytes / over NATS]

## 3. Incubator (not published): code comments

Same rule, lower priority.

- [ ] `llm/v1/sketchOne.scala` 68, 187
- [ ] `llm/v2/JsonSchemaDerivation.scala` 40; `Schemas.scala` 58; `Tool.scala` 15, 100, 148, 207; test `ToolSpec.scala` 12
- [ ] `llm/v3/Model.scala` 39, 54, 178; `Tool.scala` 17, 82, 275, 352; test `ToolSpec.scala` 15
- [ ] `llm/v3/playground/RigTools.scala` 152; `llm/v4/playground/robot/RigTools.scala` 152
- [ ] `messaging/mailbox/InMemoryMailbox.scala` 22; `Mailbox.scala` 50; `MailboxBridge.scala` 40; `Serde.scala` 14, 17
- [ ] `messaging/nats/v1/NatsConsumer.scala` 25, 64; `NatsProducer.scala` 19, 40; `Serde.scala` 7, 17, 25
- [ ] `messaging/nats/v2/NatsConsumer.scala` 16; `NatsProducer.scala` 16, 37; `NatsSubscriber.scala` 35, 44; `Serde.scala` 7, 14, 22
- [ ] `messaging/nats/v3/NatsConsumer.scala` 247, 273; `Serde.scala` 7, 16, 24
- [ ] `messaging/nats/v4/Serde.scala` 7, 15, 23; `messaging/nats/v5/Serde.scala` 7, 15, 23
- [ ] `processing/mailbox/v1/Mailbox.scala` 77, 105, 108

## 4. Living docs

- [ ] `README.md` 86 (the `Wire` rename), 106 ("wire codecs")
- [ ] `docs/README.md` 15 (link to the renamed doc)
- [ ] `docs/architecture/llm-adapters.md` 20, 31, 42 — "built wire first", "wire first, then the port over it" [the provider's JSON first / the client first]
- [ ] `docs/architecture/mailbox.md` 190 — "if protobuf becomes the wire format" [the message format]. Line 99 is the verb; leave it.
- [ ] `docs/architecture/messaging.md` 70 (the `Wire` rename)
- [ ] `docs/architecture/module-boundaries.md` 76; `sequence-types.md` 15; `where-the-agent-sits.md` 104
- [ ] `docs/learning-material/json-schema-as-scala.md` 15, 177 [in the JSON Schema / in the JSON]
- [ ] `docs/learning-material/tool-calling-wire-format.md` 2, 6, 9, 150 (with the §1 rename)

Bump `updated:` in the frontmatter of each doc you edit.

## 5. Dated records and research drafts: decision needed

By the docs rules these are written once and not edited to stay true (`docs/research/README.md` says so
explicitly). **Default: leave as written**, except the two links in §1. Confirm or override before touching
them.

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
  - `2026-08-17-llm-tool-calling.md` (24, 81: links only, see §1)
  - `2026-09-25-v4-shapes-and-the-module-question.md` (14)
  - `2026-09-26-llm-first-iteration-closed.md` (38, 51)
  - `2026-09-26-the-missing-client-layer.md` (21, 49, 68, 72)

## 6. Done when

- [ ] Both names in §1 are approved and renamed, and the GitHub release notes call out the `Wire` rename as breaking.
- [ ] §2–§4 are done.
- [ ] `sbt testFull` passes.
- [ ] This finds nothing outside §5:
  ```bash
  grep -rnE '\b[Ww]ires?\b|\b[Ww]ire-[a-z]|[a-z]Wire|Wire[A-Z]|wire[A-Z]|\bWire\b' --exclude-dir={target,.git,.bloop,.metals,.bsp} .
  ```
  The grep also matches the verb ("wires X into Y"); leave those.
