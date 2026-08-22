---
title: "Mailbox for the ZIO toolkit — point-to-point & request-reply"
type: research
status: superseded
updated: 2026-08-16
tags: [messaging, mailbox, request-reply, nats, zio, hexagonal, directory, processing, worker]
---

# Mailbox for the ZIO toolkit — point-to-point & request-reply

> **Superseded (2026-08-16)** by
> [`docs/architecture/mailbox.md`](../architecture/mailbox.md),
> which describes the Mailbox that actually landed in `homelab.common.processing`.
>
> What changed, and why this note is kept only as rationale-of-record: the built design makes an **address a
> detachable value** — a serialisable promise handed to whoever should resolve it — rather than a correlation
> the requester tracks. So there is **no `ask`** (the party that answers need not be the party you sent to),
> **no reply-to field on the envelope** (a reply's destination *is* its correlation), and **no `Pipe` adapter
> presenting remote requests to `Worker`** (§8): the receiving half is itself a `Processor[E, Message]`, and
> unclaimed messages leave through a `forward` producer. `Serde` also went — codecs are
> `homelab.common.data.Codec`. The parts below that survived are §1's framing of point-to-point delivery, §4's
> observation that NATS request-reply needs no new infrastructure, and §5's originator-side timeout.

Design rationale for the **second** messaging capability in `homelab-toolkit-zio`, the companion to
[`messaging-design.md`](./messaging-design.md) (Pub/Sub, now landed as `homelab.common.messaging` +
`modules/nats`). Where Pub/Sub is *broadcast to anonymous consumers*, **Mailbox is point-to-point delivery
to a named address** — and its dominant use is **request-reply**: a synchronous edge stitched into the async
middle. This note records what the prior `zio-processor` experiment settled, how it reshapes to the toolkit's
conventions, and — the load-bearing new insight — how cleanly **NATS Core request-reply already *is* the
Mailbox primitive**, so the first adapter needs no new infrastructure.

Prior art: `~/Dev/projects/zio-processor/playground/.../mailbox/v{1,2,3}` (the `Signal`→`Mailbox` arc,
Postgres/Redis/in-memory sketches), plus `docs/research/notes/2026-04-24-mailbox-receipt-naming.md` and the
Pulsar/Directory substrate surveys.

A working prototype now exists — the port + an in-memory backend + **two** NATS adapters, 12 passing tests —
under `homelab-toolkit-zio`'s `modules/incubator/.../messaging/mailbox`. §§3–4 describe the as-built shape and
§7 records the decisions it settled.

---

## 1. What Mailbox is

A **Mailbox** delivers a message to a *specific, named recipient* rather than to whoever is subscribed. The
recipient is identified by a **serializable address** — a routing token that can travel on the wire, be
stored, and be handed to a remote node. Two modes:

- **Request-reply (the common case).** The originator mints a fresh, one-shot **reply address** (a
  "receipt"), embeds it in an outgoing request, and awaits the reply on it. The remote handler, holding only
  the deserialized address, delivers its response back to it. This is Enterprise-Integration-Patterns
  *Request-Reply + Return-Address + Correlation-Identifier*, and it's how you get a synchronous-feeling call
  (`ask`) over an async transport.
- **Deliver-to-named-entity.** Address a message to a logical entity whose current location you look up first
  ("send to user *bob*'s live WebSocket session on whatever node holds it"). This mode needs a **Directory**
  (name → address) and is deferred — see §5.

**How it differs from Pub/Sub** (and why it's a separate capability, not a special producer):

| | Pub/Sub | Mailbox |
|---|---|---|
| Recipient | anonymous subscribers | one named address |
| Routing | subject / partition key | the address itself |
| Cardinality | fan-out (0..N) | point-to-point (1) |
| Lifetime | long-lived subscriptions | often one-shot (a receipt awaits one reply) |
| Failure | undelivered → buffered/dropped per substrate | reply to a departed recipient → dropped, originator times out |

**They compose.** The whole point of having both: a Pub/Sub handler grabs a receipt, emits a request carrying
the reply address, and awaits — a synchronous request/response spliced into an event-driven flow (§4).

---

## 2. The shape the prior art converged on

`zio-processor` iterated `Signal` → `Mailbox` across three versions. The **v3** trait is the one to carry
over (verbatim from the source):

```scala
trait Mailbox[E, Addr]:
  def expect[B: {Schema, Tag}]: IO[E, Mailbox.Receipt[E, B, Addr]]
  def deliver[B: {Schema, Tag}](address: Addr, message: B): IO[E, Unit]

object Mailbox:
  trait Receipt[E, B, Addr]:
    def address: Addr          // serializable — travels on the wire
    def await:   IO[E, B]      // local, in-process — a Promise the delivery resolves
```

The **load-bearing idea is the asymmetry of `Receipt`**: `address` is a serializable token that goes out with
the request; `await` is a purely local promise that the matching `deliver` completes. `expect` creates *no
substrate-side artifact by default* — it's in-process book-keeping (register a promise under a fresh address);
`deliver` routes a value to a pending expectation, which may live on a different node.

What the v1→v3 arc taught (worth not re-learning):

- **`expect` must not bind a `Scope`.** v1's `expect: RIO[Scope, …]` over-committed — it forced a single
  lifetime story, but receipts are sometimes one-shot (a reply) and sometimes long-lived (a session inbox).
  v2/v3 dropped `Scope`; the caller decides lifetime.
- **`Addr` is a type parameter, substrate-specific.** In-memory used a `UUID`; Postgres used `(podId,
  msgId)`. Making it a parameter (v3) rather than a `type` member (v2) keeps generic code legible.
- **Errors are typed (`IO[E, …]`, not `UIO`).** v1/v2 swallowed encode/route/decode failures; v3 surfaces
  them.
- **Naming settled on the postal metaphor** — *request-reply* (industry term, not "signal"/"rendezvous"),
  *Receipt*, *address* — and reads consistently inside and out.

---

## 3. Reshaping to the toolkit

The prior art is close, but three things change to match `homelab-toolkit-zio` (same reshaping discipline as
Pub/Sub in [`messaging-design.md §2`](./messaging-design.md)):

- **Namespace & split.** Port in `homelab.common.mailbox`; adapters in `homelab.<broker>` — and per the
  "group adapters by broker" rule, the NATS Mailbox lives in `modules/nats` *beside* the Pub/Sub adapter,
  sharing one `Connection`. The in-memory mirror goes in `modules/inmemory`.
- **`Serde`, not `Schema`.** The prior art reached for `zio-schema` (`B: Schema`). The toolkit already has a
  codec seam — the `Serde[A]` typeclass the NATS Pub/Sub adapter uses — and keeps the concrete codec a
  per-adapter choice, not a core dependency. So `expect[B: Serde]` / `deliver[B: Serde]`. (The prior art's
  `Tag[B]` type-mismatch guard is **dropped**: round-tripping every reply through `Serde` makes the pending
  map homogeneous `Array[Byte]`, so there's no `Any` to mistype — serialization removes the need for a runtime
  tag, confirmed in the prototype.)
- **Errors on `ApplicationError`, `E` generic at the port.** Mirroring the Pub/Sub ports (`Producer[+E,-A]`),
  the Mailbox port stays generic in `E` and the adapter fixes it — the NATS adapter reuses `NatsError`
  (already `AdapterError`-rooted, with `Connect`/`Publish`/`Decode`). Per
  [`error-modeling.md`](./error-modeling.md): typed-per-operation, global markers.

The port, toolkit-shaped (as built in the prototype):

```scala
// homelab.common.mailbox
trait Mailbox[+E]:
  def expect[B: Serde](timeout: Duration): IO[E, Mailbox.Receipt[E, B]]
  def deliver[B: Serde](address: Address, message: B): IO[E, Unit]

object Mailbox:
  trait Receipt[+E, +B]:
    def address: Address              // a substrate-neutral opaque token (<: String)
    def await:   IO[E, Option[B]]     // Some(reply), or None if the timeout elapsed first
```

**`Address` is one neutral token, not a type parameter.** The prior art's `Addr` type parameter leaked the
*substrate's* address type (a NATS subject, a `UUID`, a Postgres `(pod, msg)`) into the domain — a request DTO
carrying `replyTo` would change type per broker, and `Addr` metastasised through every signature that touches a
request. The fix is a single opaque `Address` (`<: String`) in the port package that each adapter serialises
its internal address *into* (NATS: the inbox subject verbatim; in-memory: the UUID string) and parses back on
`deliver`. The domain carries a stable `Address`, never a broker type. Cost: you lose the compile-time "can't
hand a UUID address to a NATS mailbox" — worth nothing in practice (one live backend per process), and it was
buying the leak.

**Effect-honesty carries over cleanly.** Under the stance we settled for Pub/Sub — *construction allocates
local state; only explicit actions reach the network* — `expect` is honest: it's a named action (the "use"),
so opening its inbox subscription there is exactly right, not a hidden constructor side effect. Building the
`Mailbox` itself (`make(connection)`) touches nothing remote.

---

## 4. NATS maps onto this almost for free

This is the reason to do NATS first (already flagged in [`messaging-design.md §4`](./messaging-design.md)):
**Core NATS request-reply *is* the Mailbox primitive.** No streams, no new infra — the same connection the
Pub/Sub adapter already holds.

- **`expect`** → ask NATS for a fresh inbox subject (`connection.createInbox()` → `_INBOX.<random>`),
  subscribe to it, and return a receipt whose `address` is that subject and whose `await` takes the next
  message off the subscription and decodes it as `B`.
- **`deliver(address, message)`** → a plain `connection.publish(address, Serde[B].encode(message))`. The
  inbox subject is a normal NATS subject; publishing to it lands on the originator's subscription.

The prototype validated **two adapter strategies**, both against a real broker:

- **Subscribe-per-reply** (`NatsMailbox`) — `expect` mints a fresh `_INBOX`, subscribes, and `flush`es so the
  SUB is live before the address escapes; `deliver` publishes; the inbox self-unsubscribes on reply. Simple and
  stateless, but every `expect` pays a subscribe + a synchronous round-trip.
- **Shared inbox** (`SharedInboxNatsMailbox`, the recommended default) — one wildcard subscription
  (`_MAILBOX.<node>.*`) set up once at `make` and drained by a forked reply loop; `expect` is **network-free**
  (mint a reply subject, register a promise), and correlation rides in the reply **subject's trailing UUID**, so
  `deliver` stays a plain publish and the generic reply loop routes without a header or decoding the body. This
  is what jnats' native `request()` does internally.

`deliver` is byte-identical across both. The single-shot reply bridges the NATS callback into ZIO via a
`CompletableFuture` (per-reply) or a `Promise` (shared) — the *one-value-then-done* escape, where Pub/Sub's
multi-shot delivery needed a `ZStream`.

**Composition with Pub/Sub — the payoff.** The reply address rides in the request (a `replyTo: Address` field
on the DTO, "parse don't validate"; a transport header is the deferred alternative — §7):

```scala
// originator (an ask over the event bus)
for
  receipt <- mailbox.expect[OrderCreated](5.seconds)   // timeout owned by the mailbox
  _       <- producer.emit(CreateOrder(replyTo = receipt.address, cart))
  result  <- receipt.await                             // Option[OrderCreated] — None ⇒ timed out (and reaped)
yield result

// handler, anywhere on the fleet
consumer.consume(req => process(req).flatMap(resp => mailbox.deliver(req.replyTo, resp)))
```

**Failure semantics are honest and need no hidden machinery** — the reason Core NATS fits so well:

- **Departed recipient.** A reply published to an inbox whose subscriber is gone is simply dropped
  (fire-and-forget). The originator's `await` never completes → it times out. No orphaned state to reap, no
  lie about delivery. (Contrast the Postgres substrate, where a row lands in a table a dead pod never drains
  and a TTL sweep must reap it.)
- **No responders, fast.** NATS can answer a request to a subject with *no* live service immediately (a "no
  responders" signal) instead of forcing a full timeout — fast failure when nothing is listening.
- **At-most-once.** Core NATS Mailbox does not persist; if you need *guaranteed* delivery or replay across a
  recipient restart, that's a durable substrate (a JetStream per-node stream, or Postgres), a later axis
  (§5). For request-reply, at-most-once is usually right: the originator times out and re-issues the *whole*
  request, which is the correct unit of retry anyway.

The one thing NATS doesn't give you is **delivery confirmation** — `deliver` is a publish, so it can't tell
you the recipient actually received it. That's inherent to fire-and-forget and, again, the originator's
timeout is the backstop. If a use case genuinely needs confirmed hand-off, it wants a durable substrate, not
Core NATS.

---

## 5. Scope — request-reply first, Directory later

Phase this the way Pub/Sub was phased:

1. **Phase 3 — request-reply on NATS + in-memory.** The receipt pattern above: `expect`/`deliver`/`Receipt`,
   the NATS adapter, an in-memory mirror, and a Testcontainer spec (ask round-trip, timeout on a dead inbox,
   concurrent receipts, no-responders). This is self-contained: request-reply needs **no Directory**, because
   the receipt address is self-describing — it *is* the return path.
2. **Phase 4 — deliver-to-named-entity + Directory.** The second Mailbox mode (`send to user bob`) needs a
   **Directory** port (`register(name → address)` / `lookup(name)`), an independent primitive. Mailbox does
   not depend on it; they compose at the call site (`lookup(name).flatMap(deliver(_, msg))`). Directory's
   substrate is genuinely unsettled in the prior art (Postgres table for modest scale; etcd/Redis at churn),
   and for NATS a **JetStream KV bucket** is the natural fit — but it's a separate design once request-reply
   proves the Mailbox port shape.

Doing request-reply first also *retires* an open question from the Pub/Sub note: the **`Recipient` vs
`Producer`** overlap. In this design `deliver(address, message)` lives on the `Mailbox` port itself (prior-art
v3), so no separate `Recipient` port is introduced. Keep it that way unless a real ask-pattern use case needs
a *serializable, passable send-only endpoint* distinct from the mailbox — the collapse is the default; the
split must earn itself.

---

## 6. In-memory mirror & tests

Same discipline as Pub/Sub: the in-memory Mailbox is the test backend and the design driver.

- **Shape (as built).** A `Ref[Map[UUID, Pending]]` where `Pending` is `(promise, deadline)` — the promise a
  reply completes plus an absolute expiry. `expect` registers one under a fresh UUID (the `Address` is the UUID
  string); `deliver` parses the address and completes the matching promise (unknown → no-op). No `Any`, no
  `Tag` — bytes are the common currency. Pure in-process, no Docker.
- **Coverage (built):** ask round-trip; concurrent receipts don't cross-deliver; `deliver` to an unknown
  address is a no-op; `await` yields `None` on timeout; and an abandoned expectation is swept by a later
  `expect`. The two NATS adapters carry the same scenarios against a Testcontainer (plus request-reply with a
  foreign responder).

---

## 7. What the prototype settled, and what's still open

Settled by carry-over: the **trait shape** (`expect`/`deliver`/`Receipt`, §2), **`Serde` over `Schema`**, the
**error model** (generic `E` at the port, `NatsError` at the adapter), **NATS-first** (§4), and the
**request-reply-before-Directory** phasing (§5).

Settled by the incubator prototype (12 tests, port + in-memory + two NATS adapters):

- **Neutral `Address`, not an `Addr` type parameter** (§3) — kills the substrate leak into the domain.
- **Timeout owned by the mailbox, surfaced as `Option`.** `expect(timeout)`; `await: IO[E, Option[B]]` where
  `None` = timed out. A timeout is an *expected absence*, not a failure, so it stays out of `E` (which would
  otherwise need a `Timeout` case in every adapter), and the mailbox does the cleanup a caller's `await.timeout`
  can't.
- **Reaping without a fiber.** Each expectation stores an absolute **deadline**; `await` bounds on
  `deadline - now` (anchoring the budget to `expect`-time, not `await`-time), and every N-th `expect` **sweeps**
  entries past their deadline — the abandoned-entry backstop, amortised across traffic, no timer.
- **Shared-inbox is the default NATS adapter** (§4) — one wildcard SUB + subject-suffix correlation, `expect`
  network-free. Subscribe-per-reply is kept as the simpler, stateless alternative.
- **`Tag` dropped** — serialisation makes the pending map homogeneous bytes; no runtime type tag.
- **A random per-instance reply namespace is correct**, not a stopgap. The prefix is a *per-mailbox-instance*
  namespace and per-instance uniqueness is the requirement (a shared id would misroute replies between
  instances — a queue-group steals, plain subs waste). Stability buys nothing on restart for Core NATS: the
  awaiting promises are in-memory and die with the process, so a stable id can't recover an in-flight ask
  without *durable correlation state* (the durable-mailbox axis). It composes with the Directory (the dynamic
  name→address indirection re-registers the new address on restart; staleness self-corrects), and random keeps
  the reply address an unforgeable bearer capability. Switch to a *stable, meaningful* prefix (pod/service name)
  only for log-readability or per-service subject ACLs — an ops trade, not a correctness need.

Still open:

1. **Where the reply address rides.** A **`replyTo: Address` field on the request DTO** (built; explicit,
   portable) vs a **transport header** (keeps the payload clean, but needs the codec/header story the Pub/Sub
   note flagged, `messaging-design.md §5.2`). Leaning DTO-field by default, header as an adapter optimisation.
2. **Long-lived receipts.** A receipt that receives *many* messages (a session inbox) rather than one reply —
   the deliver-to-named-entity mode; Phase 4 (needs Directory to be addressable by entity). Phase 3 is one-shot.
3. **Durable Mailbox.** JetStream- or Postgres-backed for *confirmed* delivery / cross-restart recovery — it
   needs durable correlation state, so it's where a stable node id would finally matter. Deferred; Core NATS
   at-most-once + originator-retry is the Phase 3 contract.
4. **Directory** (Phase 4) — its own note; substrate open (Postgres vs etcd vs JetStream KV).
5. **The abandoned-and-never-swept tail.** If `expect` traffic stops, expired entries linger until the next
   `expect` — fine for cheap map entries and a non-issue in practice, but noted.

---

## 8. Where processing landed, and what it means for Mailbox (2026-08-15)

The processing stack settled on `Processor` → `Worker` → `Stateful`, started by a `Graph`
(`homelab-toolkit-zio/docs/sessions/2026-08-15-processing-worker-stateful-graph.md`). That changes what
Mailbox has to provide, and mostly by *removing* work.

**`Worker` already is request-reply — locally.** It is a `Processor[E, (A, Promise[E, B])]`: every message
carries the channel its reply goes to, `ask` waits on it, `send` doesn't, and `process` settles it from an
`onExit` so a failure, defect or teardown reaches the caller instead of stranding it. That is the
Return-Address pattern with a `Promise` as the address — the same shape §1 describes, with the address
happening to be local and unserialisable.

**So Mailbox is a pipe adapter, not a change to `Worker`.** The load-bearing observation: a mailbox-backed
`Pipe[E, (A, Promise[E, B])]` can present remote requests to an unchanged `Worker`. On `consume`, the adapter
takes a request off the transport, mints a local `Promise`, hands `(payload, promise)` to the processor, and
arranges for whatever settles that promise to be published to the request's receipt. The worker never learns
whether its caller was in-process or across the network — which is the same seam `Pipe` already provides for
Pub/Sub, applied to the reply direction.

Consequences worth stating before building it:

- **Nothing new is needed in `processing`.** `Graph` starts it; `Permit` bounds ingress; `Worker.Parallel`
  gives concurrency; `Pipe.KeySafe` marks a transport that keeps one key in flight at a time. A mailbox
  adapter is one more `Pipe` implementation.
- **The promise→receipt bridge needs a watcher per in-flight request**, since nothing else observes a
  `Promise` being settled. Either a fiber per request awaiting it (simple, one parked fiber per outstanding
  reply) or a reply channel that is a callback rather than a promise (cheaper, but then `Worker`'s payload
  type is no longer `Promise` and the local case pays an indirection). **Open** — measure before choosing.
- **`E` must be transportable.** Locally a failed step fails one caller's `Promise` with an `ApplicationError`
  value; remotely that value has to cross the wire, so the error half of the reply needs the same `Serde`
  story as the payload (`messaging-design.md §5.2`). Today's local `Worker` sidesteps this entirely.
- **Timeouts move.** `Worker.ask` waits indefinitely and says so — acceptable when the callee is in the same
  process and started by the same `Graph`. Over a transport, the originator-side timeout of §5 is what keeps
  `ask` from parking forever, and it belongs in the adapter, not in `Worker`.

**For `Stateful` over a remote mailbox**, per-key exclusion is the open question. A local
`Pipe.fromKeyedQueue` holds a key while a message is in flight; a transport-backed pipe must do the same
across *processes* to claim `Pipe.KeySafe` honestly — a per-key lease (claim row, or NATS consumer-per-key)
— or the state store must reject the losing write (a conditional/versioned `set`, or a `modify` implemented
with `SELECT … FOR UPDATE`). A process-local lock cannot substitute: it also fails to preserve ordering.
This is the one place where "Mailbox is just another pipe" stops being free.

---

## Related

- [`messaging-design.md`](./messaging-design.md) — the Pub/Sub half; this note is its companion and reuses its
  `Serde` seam, `NatsError`, keying and effect-honesty stances.
- [`error-modeling.md`](./error-modeling.md) — the `ApplicationError` layer the Mailbox errors map onto.
- `homelab-toolkit-zio/docs/sessions/2026-08-15-processing-worker-stateful-graph.md` — where `Worker`,
  `Stateful` and `Graph` landed, and why the handle-based actor designs were dropped. §8 above depends on it.
- [`research/service-contracts/`](../../../research/service-contracts/) — the request/reply *payload schemas* are a contracts
  concern, versioned independently of this transport design.
- `~/Dev/projects/zio-processor/playground/.../mailbox/v3/Mailbox.scala` — the source trait (v3).
- `~/Dev/projects/zio-processor/docs/research/notes/2026-04-24-mailbox-receipt-naming.md` — the naming /
  receipt semantics decisions.
- `~/Dev/projects/zio-processor/docs/core/problem-statement-v2.md` — the three-primitive vision (Pub/Sub,
  Mailbox, Directory).
