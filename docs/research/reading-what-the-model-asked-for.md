---
title: "Reading what the model asked for — why a call carries its decoded arguments"
type: research
status: current
updated: 2026-09-24
tags: [llm, agent, tool, loop, types, v4]
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

## What output is still for

Facts the tool produced rather than ones the model asserted: whether a path was reachable, whether a search
found anything, what a measurement came to. The model's arguments cannot carry those, so this is additive —
the input did not replace the output, it stopped the output being used for something it was never shaped
for.

One caveat on reading the input: it is what the model *intended*, and between the arguments and the work
there are two gates — `permits` can refuse and `handle` can abort. For a signal tool nothing can fail, so
the intention is the outcome. For anything that does real work, read both.
