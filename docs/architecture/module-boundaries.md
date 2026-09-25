---
title: "Module boundaries — what belongs in common, and what belongs in a module of its own"
type: architecture
status: current
updated: 2026-09-25
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

`llm/v4` in the incubator is a tool-calling agent — `Tool`, `Registry`, `Session`, `Registered`, `Model`,
`Message`, `Progress`, `Outcome`, and a `schema/` package rendering the JSON Schema subset a tool's
arguments are described in. **All of it goes to one `llm` module. Nothing enters `common`.**

The types are generic enough for common — the question was asked directly and the answer is yes. `Message`
translates to a provider that shapes a conversation differently: Anthropic and Gemini have no system role
and no tool role, so an adapter lifts `Message.System` into the request's own field and folds a
`Message.ToolResult` into a user turn, which is what `Message.merged` is there for. That transformation is
mechanical and total.

What stops the promotion is that **no adapter exists at all**. Five minutes of reading Anthropic's shape
found one gap already: a `tool_result` block carries `is_error`, `Outcome` knows `result.failed`, and
`Message.fromOutcome` throws it away — so an adapter could not set the flag even though the information was
there. That is a missing field rather than a wrong shape, and it is exactly the kind of thing a second
adapter finds and a published artifact cannot afford.

When a second adapter exists, the lift into `common/llm/` is likely to be `Tool`, `Model`, `Message` and
whatever `Registry` has become — the types an application writes down. `schema/` and `Registered` would stay
behind, in the `JwksSource` position. `Tool` currently reaches into `schema/` through `validateInput` and
`Registered.advertised`, and `advertised` builds `{"type":"function",…}`, which is one provider's spelling
sitting inside what wants to be a port. That knot is worth untying before the lift, not during it.
