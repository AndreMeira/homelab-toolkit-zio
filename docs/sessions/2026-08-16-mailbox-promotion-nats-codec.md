---
title: Mailbox promoted to common, and the NATS module refactored onto Message-level codecs
type: session
status: current
updated: 2026-08-16
tags: [mailbox, codec, nats, jetstream, processing, testing, negative-controls]
---

# Mailbox lands; NATS drops `Serde`

Two halves that turned out to be the same work. The Mailbox went from an incubator sketch to
`homelab.common.processing.Mailbox` with a spec; the NATS module was reshaped so its codecs speak
`io.nats.client.Message` — which is precisely what a future Mailbox adapter will need, because a reply-to
lives in the message, not in the payload.

State: `common` green at 100, `nats` green at 22 (integration, real broker via Testcontainers).

Superseding doc: [`../architecture/mailbox.md`](../architecture/mailbox.md). The research note
`research/library-design/mailbox-design.md` is now marked *superseded* — its §8 assumed a `Pipe` adapter
feeding `Worker`, which is not what was built.

## Mailbox: what review changed before promotion

The design held; three defects and one omission did not.

| Found | Consequence | Fix |
|---|---|---|
| the table entry was removed *before* `promise.await` | `process` could never find the promise — every reply awaited mid-wait was lost | remove via `ensuring`, so it retires however the wait ends |
| `await` re-applied the *duration* at await time | the sweeper's deadline was earlier than the holder's, so a delivered, in-time reply could be forwarded while its holder waited on for nothing | the deadline fixed at `expect` is the authority; `await` computes `deadline - now` |
| the sweep fired on a 10% dice roll inside `expect` | untestable without stubbing `Random`, cost tied to call rate, and no reclamation once a process stops expecting | one `Ref[Instant]` gates it by interval; a `modify` elects a single scanner |
| unclaimed messages were dropped silently | no seam for inbound requests, no way to see stray replies | `forward: Producer[E, Message]`, no-op by default, plus `forwardTo` and `Incoming.make` |

The reclamation argument is worth keeping: `await` self-cleans only when it *runs to completion*, and three
ordinary paths skip it — the send fails after minting, the wait is interrupted (racing several expectations,
an outer timeout, shutdown), or the receipt is never awaited. `ensuring` closes the middle one; the sweeper
exists for the other two.

## The framing that makes the rest obvious

> **An address is a serialisable promise** — a value you attach to any message, resolved by whoever ends up
> holding it.

I got four review points wrong by not having this, and each wrong point is now a sentence in the code: there
is no `ask` (the resolver need not be who you asked), no `replyTo` on the envelope (a reply's destination *is*
its correlation), `Incoming` is a partial implementation rather than an adapter, and `droppoff[A]`'s phantom
parameter is deliberate. None of it was recoverable from the code, which is the real argument for the doc pass
that went with the promotion.

## A test that didn't discriminate

Negative controls caught a flaw in my own spec: the regression test for the removed-too-early entry *passed*
under the reintroduced bug on one run in two. `ZIO.yieldNow.repeatN(20)` offers a forked fiber the chance to
run; it does not guarantee it parked, so the reply sometimes landed before the wait began.

```scala
private def parked(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
  (ZIO.yieldNow *> fiber.status).repeatUntil {
    case _: Fiber.Status.Suspended => true
    case _                         => false
  }.unit
```

Now 3 runs out of 3 fail under the bug. The same fix went into the interrupt test, which had the mirror
flakiness — it would have failed *spuriously* on correct code. **`yieldNow` is not a synchronisation
primitive**; if a test turns on whether a fiber has parked, ask the fiber.

A second gap came from asking "is there a test for two fibers awaiting one receipt?" — there wasn't, and the
sequential memo test turned out not to prove the memo either: with the deadline absolute, a late await returns
promptly whether or not the outcome was remembered. The new test counts *decodes*, because two fibers both get
the reply regardless (`Promise.await` has many consumers) — one decode for two awaits is the only observable
difference. Confirmed by removing the memo: only that test fails.

## NATS: `Serde` → `Codec`, fixed on `Message`

`homelab.nats.Codec` replaces `Serde`, with both halves over `io.nats.client.Message` rather than
`Array[Byte]`. Consequences, in the order they fell out:

- **The subject moved into the encoder.** A NATS message cannot be built without one, so `subjectOf` left both
  producers. Keying is still a pure function of the value — relocated, not lost — and encoders can now set
  headers.
- **Decoders see the whole message**: headers, the subject a wildcard matched, reply-to. That is the piece a
  Mailbox `Location` will stand on.
- **Implementations became Message-level.** `Producer`/`Consumer`/`BatchConsumer` extend the contracts at
  `Message`; the codec is layered at the factory — `contramap(Encoder[A].encode)` for producers,
  `mapZIO(decode[A])` for consumers.
- **`apply` for the raw layer, `make[A]` for the codec layer.** Same-named overloads are ambiguous at any call
  site without an explicit type argument, including the typed factory calling the raw one; two names fix it.
- **`DecodeFailurePolicy` is gone.** `mapZIO` must yield an `A`, so it cannot skip: with decoding above the
  consumer, a malformed payload is indistinguishable from a failing handler. JetStream settles both through
  one `onFailure`; Core, having nothing to settle, aborts `consume` and lets the caller's `.either` decide.

**The cost, stated plainly:** the batched JetStream consumer used to `term` only the undecodable messages and
deliver the rest. It can't now — settling sits below the decoder and covers the batch, so one poison message
settles its batch-mates with it. Documented on `make[A]`, with the per-item `Consumer` as the way to confine
it. `mapZIO` also erases the `Batched` shape (it returns `Consumer[E, List[A]]`), so a two-line helper puts
the type back.

Six integration tests changed meaning and now pass against a real broker — including the whole-batch term,
which needed the poison drained as a batch of its own to be deterministic.

## The adapter, the same day

`homelab.nats.mailbox` — sketched in the incubator, then promoted once it was clear how little there was to
it. The prediction held exactly: a codec pair plus a `Location`, no new machinery, because a NATS subject
already *is* an address and resolving an expectation is an ordinary publish. Both `Location` obligations are
discharged by things NATS already has — one wildcard subscription on `prefix.>` for routes-back,
`NUID.nextGlobal()` for never-repeats.

Three things worth remembering from building it:

- **The prefix is `__MAILBOX__` for a reason.** The wildcard claims everything beneath the root, so an
  application publishing to a plausible-looking `mailbox.*` would have its traffic captured. Underscored
  tokens are legal (verified against a broker) and conventional for client-internal subjects — but *not*
  `_INBOX`, which the client uses for its own `request()` replies.
- **The request seam came free.** `get` returns `prefix.inbox`, matched by the same wildcard, so a peer's
  request lands unclaimed and leaves through `forward`. A test pins it.
- **A cyclic-reference error in IntelliJ** (not in sbt) traced to an anonymous `given` resolved from inside
  the object defining it, while a `val` in that object needed it to type. Fixed structurally: codecs in their
  own object, both givens named. Order-dependent compiler cycles are worth fixing by construction, not by
  perturbing the order until they go away.

The promotion also renamed to the module's convention — `core`/`stream` name by role and alias the contract
(`ProducerContract`), so the sketch's `NatsLocation`/`NatsOutgoing` became `Mailbox.Location`/`Mailbox.Outgoing`
against `MailboxContract`. The nats module's strict `scalacOptions` were the first real check of the code; the
incubator compiles with none.

## Next

- **The request side**: one `Incoming` per request type, each with its own inbox address, so dispatch stays
  routing; the responder is a `Worker` and the gap is a bridge from its reply to `Outgoing`.
- **Directory**: `Location.get` now answers, but nothing publishes or discovers it.
- **Sweep cost** is O(table) per scan; fine while the table holds only in-flight expectations.
- **Codecs vs `homelab-schemas`**: `Codec` in `common/data` is the library default and will have to meet the
  proto-as-source decision.
