---
title: "Messaging for the ZIO toolkit — Pub/Sub and Mailbox"
type: research
status: draft
updated: 2026-07-14
tags: [messaging, pubsub, mailbox, nats, zio, hexagonal, partitioning]
---

# Messaging for the ZIO toolkit — Pub/Sub and Mailbox

Design rationale for adding a **messaging** capability to `homelab-toolkit-zio`. It is inspired by a
prior standalone experiment, `~/Dev/projects/zio-processor`, which explored a substrate-agnostic
distributed-computation framework over ZIO. This note records *what we take from that work*, *how it is
reshaped to fit the toolkit's philosophy*, and *the design decisions settled so far* — before any code
moves into the toolkit.

The prior art and its research corpus (~40 notes, substrate surveys for NATS/Kafka/Pulsar/Postgres/etcd)
live in `~/Dev/projects/zio-processor/docs/`; the load-bearing pieces are its `problem-statement-v2.md`
(the three-primitive vision) and `research/results/topology.md` (the committed Pub/Sub topology).

---

## The why, in one paragraph

Homelab services increasingly want an event-driven middle: publish an event, have another service
consume and react, partitioned so per-entity state can live in memory on one node. That is a *messaging*
concern shared across services — exactly what the toolkit exists to hold once, as a **port in `common`**
plus **one adapter module per broker**. The prior `zio-processor` already modelled this well and, by
accident of good taste, already follows several of the toolkit's rules (effect factories not `ZLayer`,
port-in-core/adapter-in-module, bare port names). So this is mostly a *harvest and reshape*, not a
green-field design — with one genuinely new decision (message keying) recorded below.

---

## 1. Scope — two independent capabilities, split

`zio-processor` defines three primitives (`problem-statement-v2.md`): **Pub/Sub** (partitioned event
flows), **Mailbox** (send to a specific node / request-reply), and **Directory** (resolve a logical name
→ a node's address). They are described there as *independent primitives, not layered*.

The toolkit mirrors that independence as **two separate capabilities**, promoted in order of need:

| Capability | Ports package | Status | Notes |
|---|---|---|---|
| **Pub/Sub** | `homelab.common.messaging` | **Phase 1 landed** (ports + in-memory adapter + tests); **Phase 2 (NATS) landed** (`modules/nats`) | see the Phase 2 note in §5 |
| **Mailbox** | `homelab.common.mailbox` | second — [design drafted](./mailbox-design.md) | request-reply / deliver-to-node; needed, but after Pub/Sub |
| Directory | — | deferred | design still unsettled in the source's own notes; not in scope |

**Split by capability in `common`; group adapters by broker.** The ports divide by capability
(`messaging` vs `mailbox`). The *adapters* divide by **broker** — a single `modules/nats` holds both the
NATS Pub/Sub adapter and the NATS Mailbox adapter, because they share one connection/client. This matches
the toolkit's existing "one folder per backend, same shape across backends" rule (`postgres` holds all
Postgres repos). `modules/inmemory` likewise holds the in-memory versions of both, as the test mirror.

A service pulls `messaging` alone, or `messaging + mailbox`, and whichever broker module it runs on.

---

## 2. What we take from `zio-processor` (the Pub/Sub topology)

The committed topology (`io.github.andremeira.zproc.common.topology`) is clean and ZIO-idiomatic. The
shapes we carry over, renamed into `homelab.common.messaging`:

```
Worker[+E]                              def run: ZIO[Any, E, Nothing]
├── Processor[+E, +A]                   def input: Consumer[E, A]         (a role: consumes, emits how it likes)
│   └── Processor.Batched[E, A]
├── Pipe[+E, +A, -B]                    extends Processor; def output: Producer[E, B]   (1-in / 1-out shape)
│   ├── Pipe.PerItem   └── Pipe.Batched
└── Source[+E, -A]                      def output: Producer[E, A]
    ├── Source.Repeat  └── Source.Tick

Consumer[+E, +A]                        def consume(A => ZIO[Any, E, Unit])   (ack/commit hidden inside)
└── Consumer.Batched[+E, +A]            extends Consumer[E, List[A]]
Producer[+E, -A]                        def emit(A): ZIO[Any, E, Unit]
Wire[+E, A]                             extends Producer with Consumer       (in-process transport)
```

Design principles worth preserving verbatim (from `topology.md`):

- **Processor is a role, Pipe is a shape.** Multi-output workers (success + DLQ) extend `Processor` and
  hold producers as fields; the common 1-in/1-out case uses `Pipe`, which supplies the run loop.
- **Batching is a type, not a method.** `Consumer.Batched[E, A] <: Consumer[E, List[A]]`; batch size is
  decided *at the adapter*, not the call site.
- **Ack/commit is hidden.** `consume(logic)` takes the handler so the adapter can wrap it with
  commit-boundary semantics. Consequence: **handlers must be idempotent** — redelivery is possible.
- **Traits earn their keep** — a trait exists only when it factors something non-trivial (`Source.Tick`
  earns it; a `Sink` sub-trait did not).

### Reshaping to fit the toolkit

- **Namespace:** `io.github.andremeira.zproc.*` → `homelab.common.messaging.*`; adapters →
  `homelab.<broker>`.
- **Error model:** the source leaves `E` fully generic. Toolkit adapters must map broker exceptions to
  `ApplicationError.FromException` at the edge, and ports should surface a small typed failure (e.g.
  `Publish.Error` / `Subscribe.Error`) rather than an unconstrained `E`. Aligns with
  [`error-modeling.md`](./error-modeling.md): type per *operation*, keep the global layer as markers.
- **Scaladoc:** rewrite the source's verbose docs to the toolkit convention — terse role line +
  `@param`/`@return`/`aborts with`.
- **Serialization:** introduce a **codec port split by direction** (mirroring `codecs/database` vs
  `codecs/http`): a `MessageCodec`/`Serde` port, provided by the adapter. The source reached for
  `zio-schema` in its Mailbox sketch; keep that a per-adapter choice, not a core dependency.
- **`Wire` location:** it has meaning only for in-process transport → belongs in `modules/inmemory`, not
  the core ports (the source itself flags this as unresolved).
- **Tests:** the source has **zero tests** across every module — the single biggest gap. The toolkit's
  in-memory-mirror + ZIO Test discipline applies from day one; the in-memory adapter is the test backend.

---

## 3. Keying convention (settled)

A real broker needs a **partition key** to co-locate related messages (Kafka partitions by key, NATS
JetStream subject-partitions, Pulsar `Key_Shared` routes by key). The committed `Producer.emit(value: A)`
has no place for one. The decision is **how the key reaches the broker**, and it is settled as follows.

> **The producer infers the partition key from the message's own domain fields.** `emit(value: A)` stays
> unchanged. The key is a **pure, total `A => K`** supplied at *adapter construction* — e.g. a producer
> of `UserUpdated` is built with `key = _.userId`. The key is *not* a dedicated field on the message and
> *not* a parameter on `emit`.

Why this shape:

- **Domain surface stays minimal.** Events already carry their aggregate identity (`UserUpdated` has
  `userId` because that is what it is); `A => K` just points at the field that is already there. Nothing
  is added to the message in the common case.
- **The core trait never changes.** Keying at `emit` (`emit(k, v)`) or via an envelope
  (`emit(Message[K, A])`) would break the `Pipe` run loop `process(v).flatMap(output.emit)` and ripple
  through `Pipe`/`Source`/`Router` + every adapter. Construction-time keying leaves all of it untouched.
- **Partitioning stays a substrate concern.** The message supplies the raw material; the *adapter* still
  decides to key and how `K` maps to partitions/subjects. The convention constrains only the
  *extraction*, not the routing.
- **Precedent in the source's own code.** The in-memory `Distributer` already takes a
  `partitioner: A => K` at construction and keys internally. This rule just generalises that to every
  real `Producer`.
- **Visible at wiring, invisible at `emit`.** Co-partitioning guarantees (e.g. "validation and payment
  producers both key by `order_id`") are legible where producers are *constructed* (`key = _.orderId`
  sits right there), not hidden — which is exactly where a correctness-relevant property should show.

**The rare exception.** When a message genuinely has no natural field to partition on, add an explicit
key field — as a last resort. But that case usually signals either that the message does not need
partition-ordering at all (any partition is fine → round-robin / no key) or a modelling gap (the
aggregate id that *should* be present is missing). It is the exception reached for maybe once, not a
default. No `KeyedProducer` variant is introduced unless such a case actually forces it — and even then,
additively, alongside the unchanged base `Producer`.

**Rule of thumb:** if `A => K` needs to close over anything outside the message, stop — either the key
belongs *in* the message (lift it in at the boundary, "parse don't validate"), or you do not actually
need keyed partitioning here.

---

## 4. Broker choice — NATS first (proposed)

The prior research (`notes/2026-05-06-nats-in-the-patterns.md`, routing-substrate survey) leans toward
**NATS** as the first real adapter:

- **Lightest to operate** — single binary, Raft-based clustering, no ZooKeeper/KRaft; sub-second startup.
- **Native subjects + wildcards** — hierarchical, dynamic subscriptions the Pub/Sub topology maps onto
  cleanly; JetStream adds Kafka-like durable streams + subject partitioning when needed.
- **Mailbox comes nearly free.** Core NATS request-reply *is* the Mailbox primitive: `expect` ≈ subscribe
  to a fresh inbox subject, `deliver` ≈ publish to it. Choosing NATS first gives a natural path to the
  second capability with **no new infrastructure**, and correct failure semantics (a reply to a departed
  node is simply dropped).

**Alternative:** Kafka — heavier but the most battle-tested partitioned log, preferable only if the
homelab is already Kafka-centric or needs long durable replay. Pulsar is a middle ground with the most
complex client and the least coverage in the source's notes. *Pending final confirmation* (depends on
what the homelab already runs — currently **none**).

### NATS exploration (incubator sketch)

A NATS adapter is being explored under `modules/incubator/.../messaging/nats/v{1,2}` (Testcontainers-
backed, real-broker tests). Two findings that shape the eventual broker adapter, regardless of which
broker wins:

- **Keying→subject fits cleanly.** The construction-time `A => String` (§3) maps a message onto a NATS
  subject with zero pressure on `emit` — confirmed end-to-end, including wildcard consumers across keyed
  subjects.
- **Consume must be push→queue, not blocking-receive.** v1 mapped `consume` onto a *synchronous*
  subscription (`nextMessage`), which parks one platform thread per consumer — so consumer count per
  instance is bounded by the thread budget, not the broker. v2 bridges the async delivery callback
  (NATS `Dispatcher`) into a ZIO `Queue`, so `consume` is a fiber-based `queue.take`; many consumers then
  share the connection's few threads (O(1), not O(consumers)) — and the consumer becomes structurally
  identical to the in-memory `QueueConsumer`. **Design principle for any broker adapter: bridge the
  client's async/callback (or batched-pull) delivery into a ZIO `Queue`; never one-blocking-thread-per-
  consumer.** (JDK virtual threads are a secondary escape hatch, but the bridge is the portable default.)

- **v3 = JetStream** brings the port's real commitment: durable streams + **explicit ack** (`ack` after
  the handler succeeds, `nak`→redeliver on failure, `term`→dead-letter on undecodable payload). Redelivery
  is real, so **handlers must be idempotent** — the port contract, now enforced. Findings folded in:
  - **Two consumer models behind smart constructors**, one port. `NatsConsumer.polling` (blocking `next`
    loop — few subscriptions, simple, demand-driven) vs `NatsConsumer.bridged` (async `consume`→`ZStream`
    →`Queue` — many subscriptions, O(1) threads). Same pull-vs-push axis as Core NATS; it persists into
    JetStream (pull consumer vs ordered/async consumer), so it's a durable design axis, not a v1/v2
    accident. Different classes, one `Consumer[E, A]` surface, caller picks by intent.
  - **Backpressure is server-side (`maxAckPending`)** — the broker stops delivering past N un-acked
    messages. This *supersedes* v2's local-queue-strategy debate: no thread stall, no head-of-line
    blocking, and the local bridge queue can stay unbounded yet be bounded overall. This is the clean
    answer to "the bridge loses the poll version's backpressure."
  - **Per-message failure policy:** logic failures `nak` (retry via redelivery) and are *swallowed* from
    `consume`, so a run loop survives a poison batch; `consume` aborts only on infrastructure failure.
    Persistent failures rely on `MaxDeliver`→dead-letter (a knob to expose when this graduates).
  - **Durability removes the subscribe-before-publish ordering constraint** that bit v1/v2 — a JetStream
    publish is persisted, so consume can happen before or after it.

  Validated end-to-end against a JetStream Testcontainer (`nats -js`): durable round-trip via both models,
  plus a redelivery test (a failing handler naks; the message is redelivered and eventually acked).

---

## 5. Open questions / next steps

Resolved: **message keying** (§3), **capability split** (§1), **batched delivery type** (below,
`List` not `Batch`), **`Source.Tick` correctness** (fixed on promotion), and **Phase 1** (below).

**Phase 1 landed (2026-07-12).** The Pub/Sub ports are promoted into `homelab.common.messaging`
(`Worker`/`Producer`/`Consumer`+`Batched`/`Processor`+`Batched`/`Pipe`+`PerItem`/`Batched`/`Source`+
`Repeat`/`Tick`/`Router`/`Hub`), rewritten to convention with `IO[E, _]` signatures; `Wire`/`Recipient`/
`Producer.asRecipient` were dropped (in-process / Mailbox concerns). The in-memory adapter is ported into
`modules/inmemory` (`QueueSource`/`QueueProducer`/`QueueConsumer`+`Batched`/`Wire`/`Distributer`, the
last two extending the two ports directly), with an 11-test `InMemoryMessagingSpec` — the coverage the
source never had, including the `Source.Tick` timing fix and the `Distributer` lost-wake-up path.

- **Batched delivery uses `List`, not `Batch`.** A batched *delivery* is a bag of inputs that arrived
  together (no per-slot outcome, no lineage, and processing is N→M — filter/fan-out — which `Batch`'s
  completeness forbids). Broker commit is monotonic-offset, so per-slot errors have nowhere to go;
  poison → DLQ via `emit` + whole-batch `E`. `Batch`/`Batcher` stay in the data layer; they meet
  messaging only *inside* a handler that opts into bulk processing (optionally a future `Batcher.Logic`-
  backed pipe combinator that auto-DLQs `BE`), never in a port signature.

**Phase 2 (NATS) landed (2026-07-12; redesigned to v5 2026-07-13).** Explored across
`modules/incubator/.../messaging/nats/v{1..5}` (kept as the exploration arc). v4 unified Core + JetStream but
grew a consumer zoo (polling/bridged × per-item/batched ≈ 11 classes); **v5 is a cleaner redesign** —
promoted to **`modules/nats`**, split by substrate:

- **The `Poll` seam.** A shared `Poll` (`one` / `many(max)`) separates *receiving* a message from *decoding +
  settling* it. Each substrate has one `Poll` implementation draining an adapter-internal bridge queue; the
  consumers on top add decode/settlement. This collapses v4's ~11 consumer classes into one `Consumer` + one
  `BatchConsumer` per substrate.
- `homelab.nats` — shared: `NatsConnection` (now just a scoped `make`), `Poll`, `Serde` (typeclass seam),
  `NatsError` (on `ApplicationError`, `AdapterError`-rooted, `Receive`→`TransientError`, `Decode`→
  `DecodingError`), and the `DecodeFailurePolicy` / `HandlerFailurePolicy` vocabularies.
- `homelab.nats.core` — Core NATS, ephemeral: `Producer` (fire-and-forget), `Consumer` / `BatchConsumer`
  (queue-drain, no ack, `Surface`|`Discard` decode policy), `CoreSubscriber` (shared dispatcher → O(1)
  delivery threads), `CorePoll`.
- `homelab.nats.stream` — JetStream, durable: `Producer` (`PublishAck`), `Consumer` / `BatchConsumer`
  (explicit ack, `Surface`/`Discard`/`Redeliver`, opt-in `inProgress()` `Heartbeat`), `JetStreamSubscriber`
  (attaches the durable consumer, bridges async `consume`), `StreamPoll`, and a private `ContextConfig`.

Design calls settled in the redesign:

- **One JetStream delivery model.** v4's polling-vs-bridged distinction is gone — everything is the async
  `consume`→queue bridge; the batched consumer drains *opportunistically* (whatever's buffered), not
  wait-to-fill.
- **Lazy subscription.** A `Poll` subscribes on the *first* `consume`, not at construction — justified by Core
  semantics (fire-and-forget + no backpressure would otherwise pile undrained messages into an unbounded
  queue; JetStream durability makes late subscription free). The once-only gate is double-checked-locked:
  lock-free hot path, cold subscribe serialized, `started` flips only on success (so a failed subscribe
  retries and concurrent first-callers don't race).
- **Stream provisioning is out of the runtime path.** `NatsConnection` no longer creates streams — that's an
  operator concern (CLI / NACK operator / Terraform), and the app only *attaches* its durable consumer (in
  `JetStreamSubscriber`); the test suite provisions streams via a test-side helper.
- **Settlement is exactly-once per message.** A message termed on a decode-`Discard` is not also acked (the
  batched path settles only the decodable messages); `Surface` (not silent `term`) stays the non-destructive
  decode default, and `Discard` names `term` honestly (a real DLQ decorator, `WithDeadLetter` in `common`, is
  still deferred).

Keying → subject at construction (§3), full toolkit-convention scaladoc, strict compiler flags. **20
Testcontainer tests** (`NatsSpec`) cover both substrates incl. redelivery, poison/blast-radius, durable
resumption (across restarts), `maxAckPending`, and the heartbeat. Follow-ups: real `Serde` codecs
(zio-json/zio-schema), the `WithDeadLetter` wrapper, `Receive`-error coverage (needs connection-failure
injection), and Mailbox (Phase 3).

Still open:

1. **`Recipient` vs `Producer` overlap.** Resurfaces with **Mailbox** (Phase 3): Pub/Sub only needed
   `Producer`, so `Recipient` was deferred; decide then whether to distinguish "addressable endpoint"
   from "emission behaviour".
2. **Codec port shape.** `Serde`/`MessageCodec` split by direction — concrete signature, and whether it
   carries headers (needed by Mailbox for correlation/reply-to). Introduced in **Phase 2** (serialization
   first appears at the broker).
3. **Typed op errors.** `Publish`/`Subscribe` failure enums + `ApplicationError.FromException` mapping —
   also Phase 2; in-memory can't fail (`E = Nothing`), so `E` only becomes real at the broker.
4. **Broker confirmation** (§4) — homelab runs **no broker yet**; genuinely open (NATS-only vs
   Kafka+NATS vs multi-vendor). The **only** decision blocking Phase 2.

---

## Related

- [`mailbox-design.md`](./mailbox-design.md) — the companion capability (point-to-point / request-reply);
  reuses this note's `Serde` seam, `NatsError`, and keying / effect-honesty stances.
- [`error-modeling.md`](./error-modeling.md) — the toolkit error layer; messaging adapters map broker
  failures into it.
- [`research/service-contracts/`](../../../research/service-contracts/) — inter-service *wire* contracts (the message
  *payload schemas* are a contracts concern, versioned independently of this transport design).
- `~/Dev/projects/zio-processor/docs/core/problem-statement-v2.md` — the three-primitive vision (source).
- `~/Dev/projects/zio-processor/docs/research/results/topology.md` — the committed Pub/Sub topology
  (source).
