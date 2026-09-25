---
title: "2026-09-25 — v4's shapes settle, and where the toolkit will live"
type: session
status: current
updated: 2026-09-25
tags: [llm, v4, tool, message, progress, modules, promotion]
---

# v4's shapes settle, and where the toolkit will live

## What changed in the code

**A call has two states.** `Tool.Call` became `Raw | Decoded[A]`, so a loop can read what the model *asked
for* rather than only what a tool produced. The wire is forced to `Raw` — `Message.Assistant`,
`Progress.AwaitingTools` and `Session.dispatch` all take one — because a provider only ever sends unparsed
arguments and a stored conversation has to be sendable as it stands.

The reason it is worth having is in [reading what the model asked for](../research/reading-what-the-model-asked-for.md):
a tool's *output* schema is never advertised, so structured output only ever served a loop doing a type test
on it. `RigTools.terminate` had become the identity function to feed that test, and no longer is.

**A tool decodes its own arguments.** `Tool.decoded` reads a call's JSON as the type `handle` takes, which
is where the schema already is. `Registered` delegates and lost its own decoder, its message for a parse
failure, and its `Schema[In]` bound with them.

**Smaller shapes.** `Progress.Finished` carries the whole `Message.Assistant` rather than loose content
parts. `Message` gained `system`/`user`/`assistant`/`toolResult` constructors returning the narrow type —
Scala 3 widens an enum case's inferred type to the parent, which had been forcing annotations at call sites
— and `merged`, which joins adjacent system or user messages for a provider that will not take them apart.
`Model.Name` is opaque and `complete` takes it as a parameter, since which model answers is a choice about
where a conversation goes rather than part of the conversation.

**`Chat` writes once.** The repository-backed example accumulates what a run adds and stores it when the
model has answered, rather than a turn at a time. A run that fails leaves the conversation as it found it,
so a redelivery re-runs it rather than continuing half of one — which suits at-least-once delivery, and
costs live visibility of a turn in flight.

## What was decided

**Everything goes to one `llm` module. Nothing enters `common`.** The reasoning is in
[module boundaries](../architecture/module-boundaries.md), which also records the rule it came from: `auth`
split on *what an application names*, not on port-versus-implementation — `TokenVerifier` and `JwksSource`
are ports and they stayed in the module.

The types are generic enough for common. What stops the promotion is that no adapter exists, and `common` is
published: a port there is a semver contract, and a shape is only known to have settled once a second
implementation has been written against it.

## What an adapter already found, without being written

Anthropic's `tool_result` block carries `is_error`. `Outcome` knows `result.failed`. `Message.fromOutcome`
keeps only `render`, so the flag is buried in the text we synthesised ourselves and an adapter cannot set
it. A missing field rather than a wrong shape — and the clearest argument for writing an adapter before
promoting rather than after.

## State

`llm-v4-sketch`, ten commits, unpushed. PR #4 merged, so `main` has everything up to `Attendant`.

Next: the missing specs. `schema/` has none at all, where v2 had eighteen across `SchemasSpec`,
`JsonSchemaDerivationSpec` and `JsonSchemaSpec`; `Tool` has none directly, where v3 had seventeen. That is
the most intricate code in v4 and the only part where a bug produces a schema a model silently misfills.
