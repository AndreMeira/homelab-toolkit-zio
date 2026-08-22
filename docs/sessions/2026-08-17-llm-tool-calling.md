---
title: "LLM tool calling — schema derivation, registry, and the decisions that changed on the way"
type: session
status: current
updated: 2026-08-17
tags: [llm, tool-calling, json-schema, zio-schema, openrouter, security, exploration]
---

# Tool calling, further than expected in one sitting

It went fast because most of it was already built. `Workflow` is the agent loop, `Mailbox` is the
human-in-the-loop story, `KeyValueStore` is memory, `intercept`/`tap` are the guardrail seam. What was
actually missing was narrow: describing a tool's arguments to a model, and dispatching what comes back.

Landed in `modules/incubator/src/main/scala/homelab/incubator/llm/v2/` — 23 specs green:

| File | |
|---|---|
| `JsonSchema.scala` | the describable subset as an ADT, plus `Encoder[A]` |
| `JsonSchemaDerivation.scala` | `zio.schema.Schema[A]` → `Either[Unsupported, JsonSchema]` |
| `Schemas.scala` | `mapAsEntries` — a `Map` described as a list of entries |
| `Tool.scala` | `Tool` (pure behaviour) + `Registry` + `Session` |

Plus `zio-schema` in `common`, [`../learning-material/tool-calling-wire-format.md`](../learning-material/tool-calling-wire-format.md),
and [`../research/llm-design-exploration.md`](../research/llm-design-exploration.md)
for the design rationale.

## Four decisions that turned over during the session

**Bounds and budget: combinators → the agent's own state.** I proposed `bounded(maxSteps)` / `metered(budget)`
beside `persisted`. Wrong, for a reason the code settles: `persisted` checkpoints `S`, so a counter living
there survives a crash and resumes, while a combinator's per-run `Ref` resets. Anything that must outlive a
checkpoint *is* conversation state. And the action at a limit is domain-specific — summarise, downgrade,
answer partially — which a combinator can't express and a behaviour can. Cross-cutting enforcement needs no
new API either: `intercept` may replace `Continue` with `Done`.

**Tool dispatch is not the parked `Worker` problem.** I claimed a heterogeneous registry needed the GADT from
the 2026-08-10 log. It doesn't: a tool's result is serialised straight into the conversation, so `B` never
escapes dispatch, and one polymorphic helper binding the existential is enough. Verified on 3.8.3 before
claiming it. `Worker.ask` stays harder, because *its* caller needs the type back.

**The types were never the hard part.** What is: at-least-once execution around the checkpoint (step
granularity decides whether a replayed step re-sends an email), failures-as-data rather than as effects, and
schema/decoder drift.

**Security: the namespace must be unaskable, not validated.** `Tool[Ctx, In, Out]` splits the arguments by
trust — `In` is the model's and is the only half described, `Ctx` is the caller's and appears in no schema. A
prompt injection cannot set what the model was never offered. Binding `Ctx` at `forSession` also puts the
allow-list in the same place: a tool a caller may not use is one the model is never *told* about, with the
dispatch-time check kept as the real control.

## The shape that fell out

Tools carry no schema. Registration does:

```scala
def add[In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): IO[Rejected, Unit]
```

Binding `In`/`Out` there **removes the existential entirely** — the registry stores a monomorphic
`Registered[Ctx]` holding a name, a schema and `(Ctx, String) => UIO[String]`, so dispatch is a lookup and a
call. It also puts derivation at boot (once per tool), collects every rejection where it is actionable, and
gives per-registry rendering choices somewhere to live.

Errors landed on the hierarchy with a deliberate asymmetry: `Unsupported` is an `EncodingError` (a fact about
a type; a caller may answer it by choosing another representation), while `Rejected` — which wraps it — is an
`ImplementationError` (a tool that cannot exist as written; fix it, don't handle it).

## Discoveries worth keeping

- **Scaladoc becomes the schema's `description`.** zio-schema lifts doc comments — `/**` markers and all —
  into `@description`, so a stray comment on an argument type ships to the model as prompt text. Found by a
  test failing for the right reason.
- **A `val` does not move work to compile time.** It moves it to class-initialisation, and makes the
  diagnostic worse: the first access throws `ExceptionInInitializerError`, every later one a bare
  `NoClassDefFoundError` with the cause gone. There is no `Schema[A]` at compile time to walk — `derives
  Schema` generates code that *builds* one at runtime.
- **Recursion must be found before rendering.** Forcing `Schema.Lazy` hands back the same instance, so a
  naive walk never terminates. A first pass tracks type ids on the current path; only types that re-enter
  their own path become `$defs`.
- **`parameters` must describe an object.** Writing the wire-format note produced this check: a tool taking a
  bare `String` is now refused at registration.

## The seam, found and closed the same session

**Two independent derivations were describing the same type.** The schema came from `zio.schema.Schema`; the
codecs came from zio-json, derived separately. For plain records they agree; for anything clever they do not.
`Schemas.mapAsEntries` advertises an array of `{key,value}` objects, while an independently derived
`JsonDecoder[Map[K,V]]` demands a JSON object — a model following our schema would send something the decoder
rejects. `@discriminatorName` had the same problem: it shapes the branches we advertise, and zio-json has
never heard of it.

Closed by deriving the codec from the same `Schema` (`zio-schema-json`), which collapses the signature too:

```scala
def add[In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): IO[Rejected, Unit]
```

One bound per side; the advertised JSON Schema, the decoder and the encoder all come from it. Verified before
adopting — with `mapAsEntries` in scope the schema-derived decoder accepts `[{"key":"a","value":1}]` and
*rejects* `{"a":1}`, which is exactly the shape we advertise. A tool with a `Map` argument is now a test
(`ToolSpec`), not a hazard.

One cost: `zio-schema-json` pulls zio-json 0.10 while the incubator's jwt-scala sketches pin 0.7, so the
module takes `libraryDependencySchemes += "dev.zio" %% "zio-json" % VersionScheme.Always`. Sketches are
unpublished and ungated, so the newer wins; a jwt sketch breaking on it is an argument for pruning it, which
was already on the list.

## Next

- The model port (stream-first, cost in the response value) and the loop as a `Workflow`.
- A recorded/replay model adapter — the highest-leverage test infrastructure here, and still absent.
- `Tool` is registered but nothing yet assembles a request or reads a response; `sketchOne` (v1) has the SSE
  reader that would feed it.
