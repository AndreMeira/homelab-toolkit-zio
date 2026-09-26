---
title: "How an LLM adapter is laid out — the client under the port"
type: architecture
status: current
updated: 2026-09-26
tags: [llm, adapters, ports, layering, openai, anthropic]
---

# How an LLM adapter is laid out

`homelab-llm` holds the ports. Each provider is its own artifact — `homelab-llm-openai`,
`homelab-llm-anthropic` — and both are built the same way, in three layers:

| | `llm-openai` | `llm-anthropic` |
|---|---|---|
| the protocol's types | `request/`, `response/` | `request/`, `response/` |
| the call, nothing withheld | `ChatCompletionClient` | `AnthropicClient` |
| the port, over the client | `ChatCompletionModel` | `AnthropicModel` |

The middle layer is the one worth explaining, because a reader coming from the port will not expect it.

## Why the client exists

`Model.complete` is the general case: one conversation in, one completion out. A provider's API takes more
than that and answers with more than that. If the adapter is a single class implementing the port, the
difference has nowhere to go and is lost at the wire.

Three capabilities were lost that way before the split: `n` (the protocol returns several choices),
`max_tokens` (Anthropic requires it, so a default was invented inside the adapter), and every sampling knob
the two protocols take.

So the client comes first and keeps the provider's API as it is — one method per call, every field it
takes, everything it answers. The port is then written over it, and what the port narrows stays reachable
by whoever holds the client. `n` is the worked example: the client hands back every choice, and the model
takes the first *because a `Model.Completion` holds one*. A caller who wants the rest holds the client.

The order matters as much as the shape: **wire first, then the port over it.** `distributed-keyed-queue`
was built that way — `QueueClient` before `queue/managed/` — and its ports did not lose anything. The LLM
modules were built port-first, and each adapter then discovered what the port could not hold, one field at
a time.

## The three ways past the port

A caller who needs something `Model.Request` does not carry has three places to go, and they differ by who
knows which provider is behind the call:

| what | where |
|---|---|
| this instance always asks for X | `Config` on the adapter |
| this call needs X | hold the client |
| the protocol shipped a field last week | the client's `extra` parameter |

Each is filled by someone who already knows the provider. `Model.Request` carries no escape hatch of its
own, because filling one would mean naming a field only one provider reads — `max_tokens` on Anthropic, `n`
on OpenAI — which is what the port exists to hide.

`Config` holds what an instance pins: a cooler model for classification, a forced tool for extraction, a
ceiling on what one answer may cost. Tools are deliberately not in it — they come from the session, which
decides what a caller may use. Nor is `n`, because an instance asking for three would pay for three and
discard two on every call.

## Where the layering is broken on purpose

`request/CompletionRequest.from(model, request, config)` takes the adapter's `Config`, so
`request/CompletionRequest.scala` imports `AnthropicModel` / `ChatCompletionModel` while those import
`CompletionRequest` back. It is the only place in either module where the protocol layer reaches up to the
port layer.

The reason is consistency at the call site: every other protocol type builds itself the same way —
`ToolRequest.from`, `MessageRequest.from`, `CallRequest.from` — and `CompletionRequest` is the one a reader
looks for first. Keeping the construction on the model instead would have been the only `from` missing from
the package that holds the rest.

What it costs: the two files no longer read independently, and `CompletionRequest.from` cannot be used
without the model class. That is harmless while both live in one artifact, and is the thing to revisit if a
client ever moves into one of its own. Putting the builder on `Config` — `config.asked(model, request)` —
is the change that would restore the direction, and it is a small one.

## Where the two protocols differ on a tool's answer

Anthropic's `tool_result` block carries `is_error`; the chat-completions `tool` message has no such field,
only `role`, `tool_call_id` and `content`. So the same fact reaches the two providers by different routes,
and `Message.ToolResult` carries it in a way that serves both:

```scala
case ToolResult(callId: Tool.Call.Id, content: Chunk[Content], failed: Boolean = false)
```

`Tool.Result.render` writes an `{isError, reason}` envelope into the text, which is what a provider with no
flag has to read it from. `failed` carries the same fact as a value, which is what a provider with a flag
sets from — so the Anthropic adapter marks the block without parsing its own content back open.

The flag is a fact about what happened, and the text is how the model is told. Keeping them apart is what
stops an adapter having to recover one from the other.
