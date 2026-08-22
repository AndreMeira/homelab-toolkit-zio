---
title: "LLM client for the ZIO toolkit — exploration"
type: research
status: draft
updated: 2026-08-16
tags: [llm, openrouter, agent, tool-calling, workflow, streaming, exploration]
---

# LLM client for the ZIO toolkit — exploration

Very early. This note covers four things: the shape of the model port, where loop bounds and cost accounting
belong (spoiler: not in the toolkit), tool dispatch, and the trust boundary that runs through a tool's
arguments. Everything else is already settled or surveyed elsewhere:

- [`research/agent/agentic-ai-library-survey.md`](../../../research/agent/agentic-ai-library-survey.md) — the landscape, the patterns worth taking
  and the anti-patterns worth avoiding. Its §6 synthesis is the scoreboard this note works against.
- [`research/agent/agent-architecture.md`](../../../research/agent/agent-architecture.md) — OpenRouter as the gateway, why tool
  calls are structured, and the MCP-vs-own-API boundary with its origin-agnostic registry. **Not revisited
  here.**
- `homelab-toolkit-zio/docs/architecture/processing.md` — `Workflow`, `Worker`, `Stateful`, `Graph` as built.
- `homelab-toolkit-zio/modules/incubator/src/main/scala/homelab/incubator/llm/sketchOne.scala` — a working
  OpenRouter SSE reader: scope crossing the producer→consumer boundary, partial events buffered across byte
  chunks, tool-call argument fragments folded, `peel` splitting a prefix from the streamable remainder.

## 1. The survey's four gaps, re-scored

Messaging, processing and workflow were built partly for this, so most of §6's list is now answered by
primitives rather than by anything LLM-specific:

| Gap (survey §6) | Status |
|---|---|
| explicit cyclic control flow | **closed** — `Workflow` is `Init → Continue* → Done`, with `next` defined over the *pending* union: no hidden executor, no phantom terminal case to match |
| guardrails separate from the loop | **closed** — `intercept` rewrites a step, `tap` observes one; sanitisation is an interceptor *before* the model call, not a wrapper around the agent |
| memory as first-class | **mostly** — `KeyValueStore` + `Stateful` per conversation, `persisted(store)` to resume. Tiering is a domain policy (below) |
| loop bounds + cost tracking | **not a toolkit gap** — §4: it belongs in the agent's own state |

`persisted` is also the pragmatic 80% of the durable-execution idea the survey called its highest-value
concept: checkpoint state per step and resume, rather than replaying effects from an oplog. Crash-resumption
without needing determinism.

## 2. The port

**Stream-first; the non-streaming call is derived.** The wire shape *is* a stream of deltas — that is what
the sketch already handles — and a stream folds into a response, while a response cannot be unfolded into a
stream. So one port, with `complete` as a convenience over it.

```scala
trait Model:
  def stream(request: Completion.Request): ZStream[Scope, LlmError, Completion.Delta]
  def complete(request: Completion.Request): IO[LlmError, Completion.Response] = // fold `stream`
```

Decisions worth making early:

- **Cost lives in the value, not the log.** OpenRouter returns `total_cost` per call. If it is logged and
  discarded, no combinator can enforce a budget and no caller can attribute spend to a conversation. It
  belongs on `Completion.Response` (and on the terminal delta), with `Monitor` for the metric.
- **Errors map onto `ApplicationError`,** with the refinements carrying operational meaning as they do in
  `auth`: 429 and 5xx as `TransientError` (so a retry `Schedule` can see them), a malformed SSE frame or JSON
  body as `DecodingError`, a bad key as `UnauthorisedError`, everything else `AdapterError`.
- **A gateway normalises transport, not capability.** OpenRouter fronts many models, but tool calling, JSON
  mode, reasoning tokens and prompt caching are not universal. The port must not silently assume the union:
  an unsupported capability should be a typed failure, not quiet degradation.

**What is deliberately *not* in the port:** the tool loop, prompt construction, memory, and tool execution.
One call in, one response (or stream) out.

## 3. The loop is a `Workflow`, not an executor

`S` is the conversation, `I` the request that seeds it, `O` the final answer. One step: call the model; if it
stopped, `Done`; if it asked for tools, dispatch them, append the results, `Continue`.

```scala
def next: Step.Pending[Request, Conversation] => ZIO[R, E, Step[Request, Conversation, Answer]] =
  case Step.Init(request)      => ZIO.succeed(Step.Continue(Conversation.from(request)))
  case Step.Continue(state)    =>
    model.complete(state.asRequest).flatMap: response =>
      response.finishReason match
        case Stop      => ZIO.succeed(Step.Done(state.answer(response)))
        case ToolCalls => tools.dispatch(response.toolCalls).map(results => Step.Continue(state ++ results))
```

What that buys against LangGraph, without a framework: no compile step, no dict-shaped state with reducers,
no callback registry — and the decorations are the ones already built (`persisted` = checkpointing,
`serialised` = one run per conversation, `intercept`/`tap` = guardrails and tracing).

**Long-running and human-in-the-loop tools are a `Mailbox`.** Hand the tool an address, let the run end, and
resolve the expectation when the answer arrives — LangGraph's interrupt-and-resume story with no new
machinery. Worth confirming this against `Workflow.persisted` before relying on it: the run must be able to
*stop* at a step and be resumed by an inbound message, which is a different control flow from awaiting a
receipt inside a step.

## 4. Bounds and budget — in the state, not the runner

Runaway loops and token spend are anti-pattern §5.2, and nothing enforces either today. The first instinct
was to add `bounded(maxSteps)` / `metered(budget)` combinators beside `persisted` and `serialised`. That is
wrong, for three reasons:

1. **`persisted` checkpoints `S`.** A hop count and a running cost held in the state survive a crash and
   resume counting for free. A counter held by a combinator — necessarily a `Ref` created per run, since
   `next` is shared across runs — resets on resume. Anything that must outlive a checkpoint is conversation
   state by definition.
2. **The action at a limit is domain-specific.** A combinator can only fail. A behaviour can summarise and
   continue, drop to a cheaper model, or return the partial answer it already has — usually what an agent
   actually wants. `if state.hops >= 12 then Done(partial)` also reads in the loop, which is the survey's
   §5.1 point about not hiding what is happening.
3. **Cross-cutting enforcement already exists.** `intercept` sees every step and may replace `Continue` with
   `Done`, so a cap that no behaviour can forget is available today, typed to the app's own state:

```scala
agent.intercept {
  case Step.Continue(state) if state.hops >= maxHops => ZIO.succeed(Step.Done(state.partial))
  case step                                          => ZIO.succeed(step)
}
```

What is genuinely lost is a platform-wide guarantee nobody can opt out of. The cheap mitigations for that sit
outside the workflow anyway: `run(input).timeout(...)` as a wall-clock fuse, and a spend cap on the
OpenRouter account — which also protects against a bug in our own accounting, as toolkit machinery would not.

The one ergonomic risk is a `Continue` constructed without incrementing. That is a domain fix: a single
`state.advance(response)` that bumps the hop count and adds the call's cost, so no call site does it by hand.

## 5. Tool dispatch — the types are the easy part

A tool is a name, a schema, and a handler `A => IO[E, B]`. The registry is heterogeneous and dispatch happens
on a runtime string, which looks like it needs a GADT. It does not: a helper method that binds the
existential's type variables is enough, because **`B` never escapes the dispatch site** — a tool's result is
serialised straight back into the conversation as a `tool` message.

```scala
private def invoke[A, B](tool: Tool[A, B], raw: String): IO[E, String] =
  ZIO.fromEither(tool.decoder.decode(raw)).flatMap(tool.handle).map(tool.encoder.encode)

def dispatch(tools: Map[String, Tool[?, ?]], name: String, raw: String): IO[E, String] =
  ZIO.fromEither(tools.get(name).toRight(UnknownTool(name))).flatMap(invoke(_, raw))
```

Verified on 3.8.3 with two tools of unrelated types; the sketch lives at
`homelab-toolkit-zio/modules/incubator/src/main/scala/homelab/incubator/llm/ToolSketch.scala`.

**This is *not* the `Worker` problem after all.** An earlier draft of this note claimed tool dispatch and the
parked "uniform reply `O`" flaw (2026-08-10 session log) were the same problem. Same shape, but `Worker.ask`
is strictly harder: its *caller* needs the reply type back, so the type must survive dispatch. Here it is
erased to text on purpose. Solving tools does not solve `Worker`.

So the effort is elsewhere, in rough order of cost:

1. **At-least-once execution around the checkpoint.** `persisted` writes state per step, so a crash after a
   tool ran but before the checkpoint replays it. Fine for `search`, wrong for `send_email`. **Step
   granularity is therefore a correctness decision**: one step per *model call plus its tools* makes every
   tool at-least-once, while giving tool execution its own step narrows the window to a single tool. Settle
   this before the loop's shape hardens; idempotency keys are the fallback for tools that cannot be replayed.
2. **Failures are data, not effects.** A tool that fails, or arguments that do not decode, should go back to
   the model as the tool result so it can retry or explain — not fail the run. `dispatch` returns
   `IO[E, ToolResult]` with the domain failure *inside* `ToolResult`, and `E` reserved for infrastructure.
   Backwards, and every hallucinated argument becomes a crashed conversation.
3. **Schema/decoder drift.** The JSON Schema handed to the model and the decoder that parses the reply are
   two hand-written artefacts that must agree, and nothing checks it. Derivation is one answer; a conformance
   property test is cheaper — generate an `A`, encode it, validate against the schema, decode it back.
4. **Fan-out within a turn.** A model may request several tools at once: run them with a `Permit` cap, keep
   *all* results (the protocol needs them before the next model call), and make sure one failure does not
   cancel its siblings.
5. **Permissions and injection.** Per-conversation tool allow-lists, and the two-stage sanitisation the
   survey calls the builder's job.

**Schemas: hand-write first.** A tool's JSON Schema is prompt engineering as much as typing — field
descriptions change model behaviour — so derivation buys less than it appears to, and it collides with the
`homelab-schemas` proto-as-source decision (proto and JSON Schema do not meet for free).

Dispatch stays origin-agnostic (in-process vs MCP) exactly as `agent-architecture.md` §4 argues; this note
only types what that registry holds.

## 6. Security: the trust boundary runs through the arguments

*Early thinking — the shape below is an intuition to design against, not a settled design.*

A tool's arguments come from two places, and conflating them is the leak. The model supplies some (untrusted:
it is a stochastic process reading attacker-influenced content); the caller supplies the rest (trusted: user
id, tenant, permissions — the *namespace* the call must be confined to).

**The rule: the trusted half must be unaskable, not validated.** If `userId` appears in the advertised
schema, the model can set it, and a prompt injection becomes a data breach with one validation check standing
in the way. Keep it out of the schema and the attack does not exist. Same move as `Pipe.KeySafe` and minted
mailbox addresses: make it impossible rather than checked.

So `A` — the type the schema is derived from — carries only what the model may choose, and the context
arrives beside it:

```scala
trait Tool[Ctx, A, B]:
  def handle(context: Ctx, args: A): IO[ApplicationError, B]
```

**Bind the context at the registry, per session.** `Registry[Ctx]`, with `forSession(ctx)` producing what
dispatches. Two properties fall out:

- there is no path to `handle` without a context — a tool cannot accidentally run unscoped;
- **the allow-list lives in the same place.** Which tools a session may use decides which schemas are
  advertised, so a forbidden tool is not refused at dispatch — the model is never told it exists. Stronger
  than refusing, and it costs nothing extra.

The existential dispatch of §5 is unaffected: `invoke[A, B](tool: Tool[Ctx, A, B], ctx: Ctx, raw: String)`.

**Then push the namespace into the data ports.** A repository that reads `find(namespace, criteria)` rather
than `find(criteria)` cannot be queried across tenants even by a buggy tool. The registry stops the model;
the port signature stops the developer.

Known-unsolved, and worth not pretending otherwise:

1. **Tool results are untrusted input.** A retrieved document can carry "ignore previous instructions". The
   survey's two-stage sanitisation applies to what comes *back* from a tool, not only to what the user typed.
2. **Filtering belongs in the data layer, not the prompt.** Anything a tool returns enters the conversation
   and may be paraphrased. "Return everything and instruct the model to be discreet" is not a control.
3. **Replay must not change principal.** A resumed run that re-executes a tool has to re-execute it as the
   same user — so either the context lives in the persisted state (making checkpoints user-scoped) or it is
   re-derived on resume and checked to match. Getting this wrong is a cross-user replay.
4. **Whose authority does the agent hold?** The service's credentials are broader than any user's; the tool
   layer must narrow to the caller's, or it is a confused deputy by construction.

## 7. Testing: the recorded adapter

The toolkit's convention is a parallel in-memory implementation of every port. For a model that means
**record/replay**: capture real SSE responses once, replay them deterministically. Without it every agent
test is a live call, and the loop — the part most worth testing — is the part hardest to reach. This is the
highest-leverage piece of test infrastructure here, ahead of any of the above.

## 8. Out of scope, deliberately

- **Memory tiering** (recent turns verbatim, older summarised, semantic recall) — a domain policy over a
  store port, not a toolkit concern.
- **Prompt templates / chains** — composition is function composition.
- **MCP** — settled in `agent-architecture.md`.

## 9. Open questions

1. **Does `ZStream` belong in a toolkit port at all?** The messaging ports avoid streams on purpose
   (`Consumer` takes the logic so the adapter keeps the commit boundary). A token stream is genuinely a
   stream, so either the model port is the exception, or deltas arrive as a `Pipe` and the port stays
   stream-free. This is the biggest unresolved shape question.
2. **What a budget is denominated in** — provider-reported cost is authoritative but arrives *after* the
   call, so a pre-flight check can only estimate. Refuse on estimate, or overshoot by at most one call?
3. **Capability negotiation** — per-model capability as data, or as a failure at call time?
4. **Conversation identity** — `Stateful` keyed by conversation gives per-key serialisation for free; worth
   checking whether the agent loop wants that or `serialised(lock)`.
