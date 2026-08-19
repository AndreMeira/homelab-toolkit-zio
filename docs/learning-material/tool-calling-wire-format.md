---
title: "What a tool call actually looks like on the wire"
type: learning-material
status: current
updated: 2026-08-17
tags: [llm, openrouter, tool-calling, json-schema, streaming, wire-format]
---

# What a tool call actually looks like on the wire

The OpenAI chat-completions shape, which OpenRouter speaks and normalises across providers. Written down
because the design decisions in `incubator/llm/v2` — why a tool result is text, why every call needs an
answer, why the schema root must be an object — are all consequences of this format rather than choices.

Design rationale lives elsewhere: [`../../../research/agent/agent-architecture.md`](../../../research/agent/agent-architecture.md)
for the MCP boundary and the loop, and
[`../../../research/library-design/llm-design-exploration.md`](../../../research/library-design/llm-design-exploration.md)
for the port and registry shape.

## 1. What you send: tools are advertised per request

There is no registration step. Every request carries the whole tool list again.

```jsonc
POST /api/v1/chat/completions
{
  "model": "anthropic/claude-sonnet-4.5",
  "messages": [ { "role": "user", "content": "what's the weather in Hamburg?" } ],
  "tools": [
    {
      "type": "function",
      "function": {
        "name": "get_weather",
        "description": "Current weather for a city",
        "parameters": {                      // ← a JSON Schema, and it must describe an object
          "type": "object",
          "properties": { "city": { "type": "string", "description": "city name" } },
          "required": ["city"],
          "additionalProperties": false
        }
      }
    }
  ],
  "tool_choice": "auto"                      // "none" | "auto" | "required" | {"type":"function","function":{"name":…}}
}
```

Three things fall out of this, and they are constraints, not preferences:

- **`parameters` must be an object schema.** A tool taking a bare string or array has nowhere to put it —
  arguments are named. Our deriver happily produces `{"type":"string"}` for `derive[String]`, which is a
  perfectly good schema and an invalid `parameters`; a tool should reject a non-object root.
- **Descriptions are part of the payload**, both the function's and each property's. They are prompt text
  that happens to live in a schema, which is why derived descriptions matter and why a stray Scaladoc
  leaking into one is a real (if funny) bug.
- **The advertised list is per-request**, so a per-session registry filtering which tools exist costs nothing
  extra — you are rebuilding the list every call anyway.

## 2. What comes back: a request to call, not a call

```jsonc
{
  "choices": [{
    "message": {
      "role": "assistant",
      "content": null,                        // may also be non-null: prose *and* calls
      "tool_calls": [
        { "id": "call_abc123", "type": "function",
          "function": { "name": "get_weather", "arguments": "{\"city\":\"Hamburg\"}" } }
      ]
    },
    "finish_reason": "tool_calls"
  }]
}
```

**`arguments` is a JSON *string*, not an object.** It is double-encoded, so it must be parsed, and it is
produced by a model — malformed JSON, missing fields and invented enum values are ordinary occurrences, not
exceptional ones. That is the whole reason decode failures are treated as data to hand back rather than as
effects that fail the run.

A turn may contain **several calls**, which is what makes `parallel_tool_calls` (and a concurrency cap on
dispatch) a real concern rather than a hypothetical one.

## 3. How you answer: echo, then one result per call

```jsonc
"messages": [
  { "role": "user", "content": "what's the weather in Hamburg?" },
  { "role": "assistant", "content": null, "tool_calls": [ …verbatim, ids included… ] },
  { "role": "tool", "tool_call_id": "call_abc123", "content": "{\"tempC\":18}" }
]
```

- **The assistant message goes back verbatim**, `tool_calls` and all. It is part of the conversation; you
  cannot reconstruct or summarise it.
- **`content` of a `tool` message is a string.** Whatever the tool returned is serialised into it — which is
  precisely why a tool's result type never has to escape dispatch, and why `Tool` needs no output schema.
- **Every `tool_call_id` needs a matching result before the next assistant turn.** Providers reject a
  conversation with an unanswered call. So a batch of calls cannot short-circuit on the first failure: a
  failed tool still owes a message, carrying its error as text.

That last point is the one people get wrong, and it is invisible until a tool fails in production.

## 4. Streaming: arguments arrive in pieces

Under `"stream": true`, tool calls come as deltas:

```jsonc
{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","function":{"name":"get_weather","arguments":""}}]}}]}
{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"ci"}}]}}]}
{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\":\"Hamburg\"}"}}]}}]}
```

- `index` identifies which call a fragment belongs to; `id` and `name` usually appear only on the first.
- **Argument fragments do not align with anything** — not with JSON values, not with SSE event boundaries,
  not with HTTP chunks. They must be accumulated per index until the call closes.
- The stream terminates with `finish_reason`, then `data: [DONE]`.

`incubator/llm/v1/sketchOne.scala` already handles this: SSE parsing that buffers partial events, plus a
`mapAccum` that folds argument fragments and emits a `ToolCall` only when complete.

## 5. Strict mode, and what it forbids

`"strict": true` on a function (OpenAI structured outputs, honoured by some providers through OpenRouter)
buys schema-conformant arguments, at the price of a narrower subset:

- `additionalProperties: false` on **every** object,
- **every** property listed in `required` — optionality is expressed as a nullable union (`["string","null"]`
  or an `anyOf` with a null branch), not by omission,
- a limited keyword vocabulary; `$defs`/`$ref` are supported, recursion included,
- bounded nesting depth and total property count.

Our `JsonSchema` ADT already bakes in the first (`additionalProperties` is not a field) and can express the
second (`nullable`), but the deriver currently renders optional fields the classic way — absent from
`required`. That is a render-time switch, and strict mode is the reason it exists.

## 6. Practical notes for OpenRouter

- **Not every model supports tools.** OpenRouter exposes supported parameters per model; a request with
  `tools` to a model without them is a runtime surprise, not a compile-time one.
- **`tool_choice` support varies** by upstream provider even when tools work.
- **Cost comes back with usage** — the reason to keep it in the response value rather than logging it, so a
  budget can be enforced against real numbers instead of token estimates.
- Tool names are constrained (roughly `[a-zA-Z0-9_-]`, bounded length), so a name derived from a Scala type
  cannot be assumed valid — packages and type parameters need sanitising.

## What this pins down in our design

| Wire fact | Consequence in `llm/v2` |
|---|---|
| `parameters` must describe an object | a tool must reject a non-object schema root |
| `arguments` is a string, model-generated | decode failures are data fed back, never run failures |
| result is a string | no output schema; the result type never escapes dispatch |
| every call needs an answer | `dispatchAll` keeps every outcome, failures included |
| tools re-advertised per request | a per-session registry costs nothing |
| fragments in streaming | accumulate per `index`, never per chunk |
