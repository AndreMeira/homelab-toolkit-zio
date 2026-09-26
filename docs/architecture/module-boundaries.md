---
title: "Module boundaries — what belongs in common, and what belongs in a module of its own"
type: architecture
status: current
updated: 2026-09-26
tags: [modules, common, ports, adapters, publishing, promotion]
---

# Module boundaries

The README states the shape in one line: *`homelab-common` holds the ports and everything that needs no
third-party library; each adapter is a separate artifact, so depending on one never drags in the others'
dependencies.* This page is the part that sentence leaves out — **which** ports, since not all of them go to
common.

## The line that was actually drawn

`auth` is the worked example, and it does not split on "port versus implementation":

| `common/auth/` | `auth/` |
|---|---|
| `Requester`, `ServiceAuthenticator`, `UserAuthenticator` | `TokenVerifier`, `JwksSource`, and 13 implementations |
| 3 files | 15 files |

`TokenVerifier` and `JwksSource` are ports, and they stayed in the module. What went to common is what a
*service* types its own code against: it holds a `Requester` and calls a `ServiceAuthenticator`. It never
names a `JwksSource` — that is machinery, and machinery lives with the things that implement it.

So the test is: **would an application mention this type in its own signatures?** If yes it is a candidate
for common; if it is only reached through something else, it stays in its module however abstract it is.

## Why the test is not enough on its own

**`common` is published.** A type there is a semver contract, and the whole point of the module system is
that a consumer can depend on one artifact without the others. Getting a port wrong in common is a breaking
change to the artifact everything else depends on.

That makes promotion to common a claim that the shape has stopped moving — and a shape is only known to have
stopped moving once **two** implementations have been written against it. One proves it compiles; the second
is what finds the field nobody thought of. `auth` had K8s and JWKS before its ports settled.

## The rule, then

1. **A new area starts as one module** — its ports and its first adapter together, depending on `common`
   for `ApplicationError` and the primitives.
2. **A second adapter is what tests the ports.** Whatever it forces to change was not settled.
3. **Once it has stopped moving, lift into `common/<area>/` the types an application names** — and only
   those. Intermediate ports stay behind.

Going the other way is cheap while an area is in `incubator`, which is `publish / skip := true`, and
expensive afterwards.

## Applied: the LLM toolkit (2026-09-25)

`llm/v4` in the incubator was a tool-calling agent — `Tool`, `Registry`, `Session`, `Registered`, `Model`,
`Message`, `Progress`, `Outcome`, and a `schema/` package rendering the JSON Schema subset a tool's
arguments are described in. **All of it went to one `homelab-llm` module. Nothing entered `common`.** The
three playground examples stayed in the incubator, since what they are for is showing how the module reads
at a call site, and they depend on it exactly as an application would.

The types are generic enough for common — the question was asked directly and the answer is yes. `Message`
translates to a provider that shapes a conversation differently: Anthropic and Gemini have no system role
and no tool role, so an adapter lifts `Message.System` into the request's own field and folds a
`Message.ToolResult` into a user turn, which is what `Message.merged` is there for. That transformation is
mechanical and total.

### What the two adapters found (2026-09-26)

`homelab-llm-openai` and `homelab-llm-anthropic` now both exist, which is what step 2 of the rule asks for.
Four things changed because of them:

- **`Registered.advertised` emitted `{"type":"function",…}`** — OpenAI's spelling inside a port. Anthropic's
  Messages API takes the same three things flat, so an adapter handed that JSON would have had to parse it
  apart to re-emit it. It answers an `Advertised` now, and each adapter does its own wrapping.
- **The port was the only way through.** Both adapters were a client and an adapter in one class, so
  anything `Model` could not carry was lost at the wire. Each module has a client under the port now — see
  [llm adapters](llm-adapters.md).
- **`Model.Request.extra` was incoherent.** Filling it meant naming a field only one provider reads, which
  is what the port hides. Removed; the hatches that remain all sit where the caller knows the provider.
- **`is_error` had nowhere to go.** Anthropic's `tool_result` block carries it, the chat-completions `tool`
  message does not, and `Message.ToolResult` had no field — so the fact that a tool failed survived only as
  text inside its own content, which an adapter would have had to parse back open. `ToolResult` carries
  `failed` now. A missing field rather than a wrong shape, and exactly what a published artifact cannot
  afford to add later.

The lift into `common/llm/` is then likely to be `Tool`, `Model`, `Message` and whatever `Registry` has
become — the types an application writes down. `schema/` and `Registered` stay behind, in the `JwksSource`
position.
