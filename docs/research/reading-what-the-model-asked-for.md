---
title: "Reading what the model asked for — why a call carries its decoded arguments"
type: research
status: current
updated: 2026-09-26
tags: [llm, agent, tool, loop, types, extraction, structured-output, v4]
---

# Reading what the model asked for

`Tool.Call` has two states: `Raw`, three strings as the model wrote them, and `Decoded`, the same call with
its arguments read as the type the tool takes. This note is why that is worth a type.

## The observation that started it

**A tool's output schema is never advertised.** `Registered.advertised` puts the *input* schema in
`parameters`; `Schema[Out]` is used for exactly one thing, `Result.render`, turning the value into text for
the model to read. The model never sees an output schema and never fills one in.

So structured output serves nobody but a loop doing a type test on it. And that is precisely what v4 was
using it for — reading an ending off `Result.Succeeded(e: Ending)` — which made `terminate` this:

```scala
Tool.Definition("terminate", "…") { (_: Rig) => (input: Terminate) =>
  ZIO.succeed(Result.success(input.ending))
}
```

The identity function. An `Ending` laundered through a tool so the loop could read it back, because the
loop had no way to read what the model wrote.

## Some tools are signals, not capabilities

`terminate`, `wait`, "hand off to a human", "ask for clarification" — arguments and no behaviour. The call
*is* the instruction, and whatever the tool answers is an acknowledgement.

A toolkit that can only express a capability makes these fake a `handle`. Once the loop can read the input,
they need no implementation worth the name: `terminate` answers `"ending recorded"` and the loop reads the
`Terminate` off the call.

## Why it goes on the call and not in the result

`Result` is built at four places and two of them have no input to carry: arguments that did not parse, and
a name matching no tool. A `Result[In, A]` would need `In` optional in half its cases, which is the
illegal-state-representable problem this design keeps pushing out.

The call is always there — it is what the model wrote, present even when nothing could be done with it. So
the state goes on the call, and `Outcome` carries the call rather than its id.

## What the wire does about it

Nothing, and that is the point. `Message.Assistant` carries `Chunk[Call.Raw]`, and so does
`Progress.AwaitingTools`; `Session.dispatch` takes a `Raw`. A provider only ever sends raw arguments, and a
stored conversation has to be sendable as it stands — so nothing that travels can hold a decoded value.

Forcing the subtype at those three fields is what makes the second case safe to have at all. The
alternative considered was splitting into two types, which costs a vocabulary; bounding the field costs a
word in three declarations.

## A fact that came free

`Outcome` now says whether a call was understood: `Decoded` only exists downstream of a successful parse,
and `Raw` survives one that failed. That used to be inferred from `result.failed`, which conflated *could
not read the arguments* with *the tool refused*. Two facts that had been sharing one signal came apart on
their own.

## On the type test

`Outcome.call` is `Call[?]`, so reading the input is a runtime match. It is a **checked** one wherever the
tool's input is monomorphic:

```scala
outcomes.map(_.call).collect { case Call.Decoded(_, _, Wait(handle, timeout)) => … }
```

compiles to an exact `isInstanceOf` and genuinely discriminates — other decoded calls fall through rather
than matching and exploding. Erasure only bites when the input type is itself generic (`Boxed[String]`),
and the compiler says so:

> the type test for `Boxed[String]` cannot be checked at runtime because its type arguments can't be
> determined from `Any`

Static typing is available where it is wanted, by holding the tools rather than a registry:

```scala
final case class Tools(name: Tool[UserId, Int, String], city: Tool[UserId, Int, Location]):
  def registry = name + city
```

A loop with that has `In` concretely per branch, and `Tool.decoded` reads a raw call with no registry in
the way — which also works *before* dispatch, for a loop deciding whether to run a call at all.

## A tool as a shape

Taken to its end, a signal tool stops signalling anything and becomes a way of asking the model for a
shape. The tool does nothing; its arguments are the product.

> "Restate the weather you just described as structured data."
>
> The model has said `the weather is sunny, 18°C`. The call it makes carries `Weather(Sunny, 18, Celsius)`.

```scala
enum Sky derives Schema:
  case Sunny, Cloudy, Overcast

enum Unit derives Schema:
  case Celsius, Fahrenheit

final case class Weather(sky: Sky, temperature: Int, unit: Unit) derives Schema

val formatted: Tool[Any, Weather, String] =
  Tool.Definition("formatted", "Restate the weather you just described as structured data.") {
    (_: Any) => (_: Weather) => ZIO.succeed(Result.success("recorded"))
  }
```

and the loop takes the shape off the call:

```scala
outcomes.map(_.call).collect { case Tool.Call.Decoded(_, _, w: Weather) => w }
```

This is what tool calling was used for before providers had structured-output modes, and it is still the
mechanism underneath most of them.

**Make every field an enum that can be one.** A `unit: String` gets `"C"`, `"°C"`, `"celsius"` and
`"Celsius"` across four runs; `Unit` renders as `{"type":"string","enum":["Celsius","Fahrenheit"]}` and a
strict-mode provider enforces it. The difference between extraction that works and extraction that has to
be normalised afterwards is usually just this.

Three things that are not obvious the first time:

- **Nothing forces the model to call it.** It may answer in prose and stop. Providers take `tool_choice` to
  force a named function; the port does not model it, so it is set on the adapter's `Config` — an
  extraction model is one instance with `toolChoice` fixed — or on the request a caller hands the client.
- **Keep it out of the agent's registry.** An extraction tool sitting beside `search` and `terminate`
  invites the model to call it at odd moments. A registry holding only that tool, used for a call of its
  own, keeps the agent's list about what the agent does.
- **Decide where the loop stops.** After the call is dispatched, `Progress` reads `AwaitingModel` and a
  general agent asks again — a round trip to say nothing. Extraction wants to stop as soon as it has the
  `Decoded`. That is a loop policy, which is why `agent/Basic` is the general form and this is a workflow of
  its own.

## What providers guarantee

Two different things travel under "structured output", and only one is useful here. **JSON mode** promises
valid JSON and nothing about its shape. **Schema-constrained** output conforms to a schema you supply,
usually by constraining decoding.

| Provider | Shape |
|---|---|
| OpenAI | `response_format: {type: "json_schema", json_schema: {…, strict: true}}`, and `strict: true` on function definitions |
| Azure OpenAI | the same |
| Google Gemini | `responseMimeType: "application/json"` plus `responseSchema`, an OpenAPI-3 subset |
| Mistral | custom structured outputs with a schema |
| Cohere | `response_format: {type: "json_object", schema: …}` |
| Ollama, vLLM, llama.cpp | grammar-constrained decoding — arbitrary grammars, so stricter than a schema |
| OpenRouter | passes `response_format` through where the provider under it supports one |

Anthropic is the one to check rather than assume: its mechanism has been tool use with `input_schema` and a
forced `tool_choice`, with schema-constrained output moving through beta.

**The part that matters here: strict tool use is the same machinery pointed at the arguments.** OpenAI's
`strict: true` on a function guarantees the model's arguments conform to `parameters` by the same
constrained decoding. So asking for a shape through a tool call is not the fallback for providers without
structured output — where strict functions exist it carries the same guarantee, and it stays portable to
where they do not.

And the subset in `llm/v4/schema` already fits. Strict mode demands `additionalProperties: false`
everywhere, every property in `required`, and refuses validation keywords like `minLength` and `pattern` —
which is what [`the schema ADT`](../learning-material/json-schema-as-scala.md) renders and nothing else.
Optionality is the one difference: strict mode wants `anyOf[T, null]` rather than an omission, and
`Generator` already emits both. A `strict` flag on an advertised function would mostly just work, as a
field on the adapter's `ToolRequest.Function` — it is one protocol's spelling, so it belongs there rather
than on `Advertised`.

This table was written against knowledge current to May 2026 and the area moves; check the provider you are
about to write an adapter for.

## What output is still for

Facts the tool produced rather than ones the model asserted: whether a path was reachable, whether a search
found anything, what a measurement came to. The model's arguments cannot carry those, so this is additive —
the input did not replace the output, it stopped the output being used for something it was never shaped
for.

One caveat on reading the input: it is what the model *intended*, and between the arguments and the work
there are two gates — `permits` can refuse and `handle` can abort. For a signal tool nothing can fail, so
the intention is the outcome. For anything that does real work, read both.
