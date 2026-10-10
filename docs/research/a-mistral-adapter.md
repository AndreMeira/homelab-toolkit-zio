---
title: "A Mistral adapter — a dialect of chat completions, and what a preset would lose"
type: research
status: current
updated: 2026-10-10
tags: [llm, adapters, mistral, openai, chat-completions, wire-first, tool-calls, reasoning]
---

# A Mistral adapter

The toolkit has two LLM adapters. `homelab-llm-openai` is the chat-completions protocol rather than one
provider, and its build comment gives the rule for adding a provider to it: *a provider is a preset on
`ChatCompletionModel`'s companion, since what differs between them is an endpoint and a header or two.*
`homelab-llm-anthropic` is the other kind — a protocol that shapes a conversation differently.

Mistral sits between the two. Its own migration guide says to point the OpenAI SDK at
`https://api.mistral.ai/v1` and change nothing else. So the question is whether Mistral is a preset in
`llm-openai` or a module of its own.

**Short answer: a module of its own, `homelab-llm-mistral`.** Mistral is a *dialect* of chat completions. Its
schema refuses every field it does not name, it names some of the shared ones differently, it has fields of
its own that `extra` would have to carry for good, and its answers carry content the `llm-openai` decoder
cannot read. By the toolkit's own rule — the client keeps the provider's API as it is, and `extra` is for a
field shipped last week rather than one already known — none of that belongs in a preset.

A preset still works for the simplest case, and it costs nothing, because `ChatCompletionModel.compatible`
already exists. The section below says how far it goes.

Everything here was read from Mistral's OpenAPI spec, its Python SDK and its docs on 2026-10-10. **Nothing
was run against the API** — there is no key in the homelab yet. [What to check before building](#what-to-check-before-building)
lists what only a live call can settle.

## What `compatible` already does

`ChatCompletionModel.compatible(uri"https://api.mistral.ai/v1/chat/completions", Some(key))` sends a bearer
token and a `CompletionRequest`. With the default `Config`, the request carries `model`, `messages` and, when
the session offers tools, `tools`. Checked against the spec, field by field:

| what `llm-openai` sends | Mistral |
|---|---|
| `model`, `messages`, `tools` | accepted |
| system and user content as an array of `{"type":"text"}` parts | accepted — content is a string or an array of chunks |
| assistant content as parts, `tool_calls` as `{id, type, function: {name, arguments}}` | accepted |
| a tool message as `{role, tool_call_id, content}` with string content | accepted |
| `tool_choice` `auto` / `none` / `required` / named | accepted |
| `max_tokens`, `temperature`, `top_p`, `stop`, `response_format`, `parallel_tool_calls`, the two penalties | accepted — `temperature` stops at 1.5 rather than 2 |
| **`seed`** | **422** — Mistral calls it `random_seed` |
| **`user`**, **`logit_bias`** | **422** — not in Mistral's schema |

And on the way back:

| what Mistral answers | `llm-openai`'s decoder |
|---|---|
| `content` as a string, `tool_calls` with `arguments` as a string, `finish_reason` `stop` / `tool_calls` / `length` | read |
| `usage.prompt_tokens`, `completion_tokens` | read; `cost` absent, which is how a direct provider reports |
| **`content` as an array of chunks** — what a reasoning model answers with | **`Malformed`** |
| **`arguments` as an object** — the schema allows it, the docs' examples send a string | **`Malformed`** |
| `finish_reason` `model_length` or `error` | `Other("model_length")`, `Other("error")` |
| an error body `{"object":"error","message":…,"type":…,"code":…}` | the status is mapped right; the detail is the first 200 characters of the body, since `FailureResponse` expects OpenAI's `{"error":{…}}` |

So a preset serves a conversation that starts and ends on Mistral, uses a model that answers in plain
strings, and leaves `seed`, `user` and `logitBias` unset in `Config`. That is a real subset — plain chat and
tool calls on Mistral Large 3, say — and it is the zero-code route for anyone who wants one today.

Through OpenRouter, which already has a preset, Mistral's models are reachable as well. The tool-call id
rule below still applies there — it has been reported through OpenRouter
([openclaw#57672](https://github.com/openclaw/openclaw/issues/57672)). Whether OpenRouter turns Mistral's
thinking chunks into the string-content shape `llm-openai` reads is unverified.

## What breaks, in the order a caller would meet it

**1. Reasoning answers do not decode.** With `reasoning_effort` set, `message.content` comes back as
`[{"type":"thinking",…},{"type":"text",…}]`. Some models — GLM 5.3 on Mistral's platform — always answer
that way. `CompletionResponse.AssistantMessage.content` is an `Option[String]`, so the whole response is `Malformed`.
Mistral's reasoning guide also asks for the thinking chunk to be sent back on the next turn: *"always replay
the full assistant message (including ThinkChunk) back into the message history."*

**2. A conversation started elsewhere is refused.** Mistral's tokenizers before v13 encode a tool-call id as
exactly nine characters of `[A-Za-z0-9]`, and the hosted API refuses anything else with a 400:

```json
{"object":"error","message":"Tool call id was toolu_01VpEm654HvfMoRcrE3Hgdc6 but must be a-z, A-Z, 0-9, with a length of 9.","type":"invalid_function_call","code":"3280"}
```

The rule lives in `mistral-common`'s validator and is reported against the hosted API through 2026-09, for
OpenAI's `call_…` ids, Anthropic's `toolu_…` ids and UUIDs. Mistral mints conforming ids itself, so a
Mistral-only conversation never meets it. A conversation recorded with another provider's ids — or a runner
that falls back from one provider to another mid-conversation — does.

**3. Three `Config` fields are traps.** `seed`, `user` and `logitBias` each make the request a 422, because
the schema is `additionalProperties: false` at every level. Repeatability is reachable only by writing
`random_seed` into `extra`.

**4. What Mistral offers beyond OpenAI is reachable only through `extra`.** `reasoning_effort`,
`prompt_mode`, `prediction`, `guardrails`, `prompt_cache_key`, `service_tier`, `safe_prompt`, and the
`tool_choice` value `any`. `extra` exists as *a hedge against the protocol moving*, and the
[adapter layout](../architecture/llm-adapters.md) says a field the type already names belongs in the field.
These are known fields of a known API, so living in `extra` would be permanent rather than a hedge.

**5. The failure detail is degraded, and two finish reasons are unnamed.** Neither breaks a call. Status
mapping is right as it stands — 401/403 `Refused`, 429/5xx `Unavailable`, the rest `Rejected` — but the
detail a log shows is raw JSON. A 422 comes in two observed shapes: `message` holding a nested
`{"detail":[…]}`, or a bare `{"detail":[…]}`. A 429 carries no `Retry-After`; the remaining-request count is
in `x-ratelimit-remaining-req-minute`.

**6. Message order is validated.** Mistral refuses a conversation whose last message is not a user or tool
message (or an assistant message with `prefix: true`), and has refused a user message directly after a
tool message:

```json
{"object":"error","message":"Unexpected role 'user' after role 'tool'","type":"invalid_request_message_order","code":"3230"}
```

`mistral-common` started allowing user-after-tool on 2026-04-27, and whether the hosted API followed is
unverified. In the toolkit's terms, `Progress.from` answers `AwaitingModel` for both a conversation that is
only a system message and one where a user speaks after the tools answered, and Mistral may refuse either.

## Why a module rather than a wider `llm-openai`

The alternative is to teach `llm-openai`'s types the dialect: decode content as a string *or* chunks,
add `random_seed` beside `seed`, add `reasoning_effort`, and so on. Three reasons against:

- **It breaks the layer's promise.** `ChatCompletionClient` is "one method, and nothing withheld" for the
  chat-completions protocol. A `CompletionRequest` holding both `seed` and `random_seed` is no longer one
  protocol's request; it is a union of dialects, where a caller has to know which provider is behind the
  client to know which field to fill. That is the knowledge `Model.Request` refuses to carry, moved down a
  layer.
- **Mistral's strictness reaches every field.** A request type shared with OpenAI is free to grow fields
  OpenAI accepts, and each one becomes a 422 the moment it is filled for Mistral. Separate types keep that
  failure impossible to write.
- **The rule already written says so.** "An endpoint and a header or two" is the preset criterion, and
  Mistral differs in request names, response shape and error shape.

The cost is duplication: `llm-openai` is about 1,100 lines, and most of a Mistral module would be the same
DTOs spelled again. `llm-anthropic` is the precedent for paying it, and two wire contracts that evolve on
separate schedules are a reason to keep them apart rather than a repetition to factor out.

## The module, sketched

The same three layers as the other two adapters — wire first, then the port over it:

| | `llm-mistral` |
|---|---|
| the protocol's types | `request/`, `response/` — from Mistral's OpenAPI spec, every field |
| the call, nothing withheld | `MistralClient`, `HttpMistralClient` |
| the port, over the client | `MistralModel`, with `MistralModel.Config` |
| what it refuses with | `MistralError` — `Unavailable` / `Refused` / `Malformed` / `Rejected`, the kinds the other two use |

What each layer has to get right, beyond copying `llm-openai`:

**Request.** `CompletionRequest` names Mistral's fields — `random_seed`, `reasoning_effort` (`none`,
`minimal`, `low`, `medium`, `high`, `xhigh`), `prompt_mode`, `prediction`, `guardrails`, `prompt_cache_key`,
`service_tier`, `safe_prompt` — and none of OpenAI's that Mistral refuses. `ToolChoice` adds `Any`.
`MessageRequest.Assistant` carries `prefix`. A content chunk the adapter does not model goes out as written,
which is how `llm-openai` treats `Message.Content.Raw` already.

**Tool-call ids.** `MessageRequest.from` rewrites an id that is not nine alphanumerics into one that is,
deterministically — a pure function of the original, so a call and the result that answers it map to the
same id within one request. Ids Mistral minted pass through untouched, and the stored conversation keeps
whatever it held: the rewrite exists only on the wire. No reverse mapping is needed, because the ids in
Mistral's answer are its own. With 62⁹ ≈ 1.4 × 10¹⁶ ids, a collision between two calls in one conversation
is not a practical concern.

**Response.** `content` decodes as a string, null, or an array of chunks. Text chunks become
`Message.Content.Text`; every other chunk — thinking, reference — becomes `Message.Content.Raw`, so it rides
back on the next turn unaltered. That is what [`Message.Content`](../../modules/llm/src/main/scala/homelab/llm/Message.scala)
was shaped for: its doc already names reasoning blocks that a provider wants handed back. `arguments`
decodes as a string or an object, and an object is rendered to compact JSON, since `Tool.Call.Raw` holds the
arguments as the string the model wrote. `finish_reason` maps `model_length` to `Length` — the name says
the answer stopped on the model's context length, though Mistral does not document it — and `error` to
`Other("error")`.

**Errors.** `FailureResponse` decodes `{"object":"error","message":…,"type":…,"code":…}`, with `message`
either a string or a nested `{"detail":[…]}`, and the bare `{"detail":[…]}`. The detail kept for a
`Rejected` names the field a 422 points at, which is what a reader of the log needs.

**Config.** What an instance pins: tool choice, `maxTokens`, `temperature`, `topP`, `stop`, `randomSeed`,
`responseFormat`, the penalties, `parallelToolCalls`, `reasoningEffort`, `promptMode`, `promptCacheKey`,
`serviceTier`, `extra`. `n` stays on the client for the reason it does in `llm-openai`.

**Not in the first cut:** streaming (neither existing client streams), the Agents and Conversations API
(beta, server-side history, and not needed for tool use — function calling is documented on chat
completions), and `guardrails` beyond carrying its JSON.

## What a third adapter says about the port

The [module boundaries](../architecture/module-boundaries.md) rule is that a second adapter tests the ports.
A third one tests them again, and these are its findings so far:

- **`Message.Content.Raw` holds up.** Mistral's thinking chunks are the case it was designed for, and they
  round-trip with no change to `homelab-llm`.
- **A tool-call id is the provider's, not the conversation's.** `Tool.Call.Id` carries no promise about its
  format, and Mistral is the first provider found that refuses another's. The adapter can absorb it, as
  above. What this establishes for the port is that a stored conversation is not portable between providers
  as stored. An adapter makes it portable on the way out.
- **`Model.Usage` has no field for cached tokens.** Mistral reports them in
  `usage.prompt_tokens_details.cached_tokens`, as OpenAI does. A cost calculation needs them, and neither
  adapter keeps them today.
- **Message order is a provider constraint the port does not express.** If the hosted API still refuses a
  user message after a tool message, the translation from `Message` is no longer total for Mistral. That
  would be the first conversation `Progress` calls sendable that a provider refuses. Settle it with a live
  call before designing around it.

## What to check before building

Each of these needs one call with a key, and each changes the design if the answer is the unexpected one:

1. **Does `mistral-medium-latest` answer in chunks when `reasoning_effort` is not sent?** If it does, the
   preset route is closed even for plain chat.
2. **Is user-after-tool still refused?** Send `user → assistant(call) → tool → user`.
3. **Do models on tokenizer v13 and later still refuse a foreign id?** Mistral Medium 3.5 and Large 4 are
   the candidates; `mistral-common` dropped the format check for v13.
4. **Is a `thinking` chunk accepted back without its `signature`?** The spec says `signature` is "to replay
   some reasoning blocks across turns".
5. **What does `model_length` mean?** The mapping to `Length` above is inferred from the name.

## Sources

Read on 2026-10-10.

- Mistral's Python SDK v3.2.0, released 2026-10-09 and generated from Mistral's own spec:
  <https://github.com/mistralai/client-python/tree/v3.2.0/src/mistralai/client/models>. Where it and the
  OpenAPI file below disagree, the module follows the SDK, which is the newer of the two. On 2026-10-10 they
  disagreed three ways: the SDK's request has seven fields the file does not (`min_tokens`,
  `repetition_penalty`, `top_k` and four for logprobs), its content chunks add `resource` and
  `resource_link`, and its chat-completions tools no longer include web search or the code interpreter.
- Mistral's OpenAPI spec, <https://docs.mistral.ai/openapi.yaml> — `ChatCompletionRequest`,
  `AssistantMessage`, `ToolMessage`, `ToolCall`, `FunctionCall`, `ContentChunk`, `ThinkChunk`,
  `ChatCompletionChoice` and `ReasoningEffort` were checked against it directly.
- Docs: [migration guide](https://docs.mistral.ai/resources/migration-guides),
  [function calling](https://docs.mistral.ai/studio/conversations/function-calling),
  [reasoning](https://docs.mistral.ai/studio/conversations/reasoning),
  [error glossary](https://docs.mistral.ai/resources/error-glossary),
  [known limitations](https://docs.mistral.ai/resources/known-limitations),
  [models](https://docs.mistral.ai/models).
- `mistral-common` v1.12.0, the request validator Mistral's tokenizers use:
  <https://github.com/mistralai/mistral-common/blob/v1.12.0/src/mistral_common/protocol/instruct/validator.py>.
- The hosted API's actual error bodies come from issue reports, which are secondary sources:
  - tool-call ids: [zed#53034](https://github.com/zed-industries/zed/issues/53034),
    [opencode#1680](https://github.com/anomalyco/opencode/issues/1680),
    [mistral-vibe#1075](https://github.com/mistralai/mistral-vibe/issues/1075);
  - message order: [goose#5998](https://github.com/aaif-goose/goose/issues/5998),
    [mistral-vibe#1114](https://github.com/mistralai/mistral-vibe/issues/1114);
  - 422 on unknown fields: [agentgateway#3835](https://github.com/agentgateway/agentgateway/issues/3835),
    [openclaw#47079](https://github.com/openclaw/openclaw/issues/47079);
  - 429 without `Retry-After`: [mistral-vibe#1133](https://github.com/mistralai/mistral-vibe/issues/1133).
