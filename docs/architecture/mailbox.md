---
title: Mailbox — a serialisable promise
type: architecture
status: current
updated: 2026-08-16
tags: [mailbox, messaging, request-reply, processing, address, codec, zio]
---

# Mailbox

`homelab.common.processing.Mailbox` — point-to-point delivery to a minted address, and the request-reply
idiom built on it. One file, one object, seven nested types.

Supersedes [`research/library-design/mailbox-design.md`](../../../research/library-design/mailbox-design.md),
whose §1–8 describe a different shape (a `Pipe` adapter presenting remote requests to `Worker`, correlation
carried in a receipt, `Serde` at the seam). What landed is smaller and does not touch `Worker` at all.

Code: `modules/common/src/main/scala/homelab/common/processing/Mailbox.scala`
Codecs: `modules/common/src/main/scala/homelab/common/data/Codec.scala`
Spec: `modules/common/src/test/scala/homelab/common/processing/MailboxSpec.scala` (12)

## The idea

**An expectation is a serialisable promise.** `expect` mints one and returns a `Receipt` carrying an
`Address`. That address is a *value*: attach it to a message going anywhere, to as many parties as the
protocol calls for, or pass it along a chain. Whoever ends up holding it resolves the expectation by sending
a message to it, and the receipt's holder gets that message back, decoded. It is an actor system's `self`,
made small enough to put in a payload.

```scala
for
  receipt <- inbox.expect[Confirmation](30.seconds)
  _       <- outgoing.send(warehouse, DispatchOrder(id, replyTo = receipt.address))
  outcome <- receipt.await          // Some(confirmation), or None at the deadline
yield outcome
```

Note what is *not* happening: the reply does not come back from `warehouse` because we asked it — it comes
back because someone was handed an address. The party that resolves it need not be the party we sent to.

## The pieces

| Type | Role |
|---|---|
| `Address` | opaque over `String`; where a message can be sent. Minted by `Location`, unvalidated |
| `Message(address, payload)` | the envelope. On a reply leg the address *is* the droppoff |
| `Location[E]` | the naming half of a transport: `get` (this process's address) and `droppoff[A]` |
| `Outgoing[E]` | `producer` + `send[A: Encoder](address, value)` |
| `Inbox[E]` | what callers depend on: `expect[B: Decoder](timeout)` |
| `Incoming[E]` | `Inbox` + `Processor[E, Message]` — the partial implementation for consumer-backed transports |
| `Receipt[+E, +B]` | `address` (to give away) and `await` (to keep) |

`Incoming` is not an adapter, which is why it is not named after one. It holds the expectation table,
resolves what arrives, sweeps what expired, and leaves an adapter to supply only a `Location` and a
`Consumer`. `Incoming.make(location, consumer, forward, sweepInterval)` builds one; being a `Processor`, a
`Graph` drives its intake.

## Semantics that are load-bearing

- **Registration precedes existence.** `expect` registers the entry before the address exists anywhere else,
  so no reply can outrun its expectation. Enforced by the API, not by discipline.
- **An expectation resolves once.** `process` claims and removes the entry in a single `Ref.modify`, so a
  second delivery to the same address cannot complete a promise the first already claimed. It is forwarded.
- **The deadline is the authority, not the duration.** `expect(timeout)` fixes `deadline = now + timeout`;
  `await` computes its budget as `deadline - now` when it actually runs. The holder may await long after
  minting, so a re-applied duration would let a wait outlive the entry the sweeper is entitled to reclaim —
  and a reply landing in that window would be forwarded while its holder sat waiting for nothing.
- **`await` answers once.** Memoised through a `Ref.Synchronized`, so a second await returns the first
  outcome rather than waiting again. Exception: an *interrupted* wait leaves no outcome to remember.
- **Entries retire however the wait ends** — delivered, expired, failed, or interrupted out of a race
  (`ensuring`). The sweeper therefore only ever reclaims expectations nobody awaited: a send that failed
  after minting, or a receipt that was dropped.
- **The sweep is interval-gated, not random.** One `Ref[Instant]` claims the sweep via `modify`, so of many
  concurrent callers exactly one scans and the rest pay a compare-and-set. Cost is bounded per unit *time*,
  not per call, and it is deterministic under `TestClock`.
- **Unclaimed messages go to `forward`** — a reply whose holder has gone, a second reply to a resolved
  address, or something that was never a reply at all. Dropped by default (`Producer.noop`), so a process
  that only expects wires nothing. `forwardTo(producer)` derives an inbox that forwards instead; it shares
  the table and the intake, so it is a *replacement* — give a `Graph` the copy, not both.

## What a `Location` implementor owes

Two obligations, neither of which the mailbox can check:

1. **Every minted address must route back to the `Incoming` that asked for it.** Otherwise replies arrive
   nowhere and every expectation times out, with no diagnostic.
2. **An address must never repeat for the life of the process.** A reused address lets a late reply to a dead
   expectation resolve a live one — silently, with the wrong value.

`droppoff[A]`'s type parameter is phantom here *on purpose*: nothing in `Mailbox` reads it, but it is what an
implementor needs to derive a per-type channel (a typed subject, a schema-tagged queue) through a macro or a
type-level lookup. Do not prune it.

## Deliberate omissions

- **No `ask`.** It would presume the reply comes back from whoever we asked. Since an address is detachable,
  that presumption is false in general — `ask` is not missing, it is meaningless here.
- **No `replyTo` field on the envelope.** The reply leg's `address` already *is* the correlation, and a
  request may carry two addresses (or none). A conventional reply-to field for interop is a schema concern,
  not the mailbox's.
- **Not a bidirectional channel.** Inbound *requests* — calls this process should answer — arrive as
  unclaimed messages and leave through `forward`. The symmetric extension is one `Incoming` per request type,
  each with its own inbox address, so dispatch stays routing rather than matching; the responder is then a
  `Worker`, and the missing piece is a bridge from its reply to `Outgoing`.

**Transport requirement.** Both sides must be able to *produce*: the resolver sends to a droppoff address
rather than answering down a connection. That is free on a broker and impossible over plain HTTP, where the
server cannot call back — a gRPC-style bidirectional stream is the answer there, and a different design.

## What the spec pins

`MailboxSpec` (12, ~550ms, no sleeps except where time is the subject). Three of them were written against
bugs found in review and were verified to fail when those bugs are reintroduced:

| Test | Fails if |
|---|---|
| stays resolvable while the holder is waiting | the entry retires when the wait *starts* rather than ends |
| dies at the instant set when it was minted | the budget is measured from the await instead of the mint |
| resolves exactly once | the claim is a `get` + `update` instead of one `modify` |

Also covered: timeout leaves no entry, `forward` receives exactly the unmatched, `await` memoisation, the
interrupted wait retiring its entry, the interval-gated sweep reclaiming abandoned expectations, distinct
addresses under concurrency, decode failure, and the whole thing running as a `Processor` off its own intake.

## Open

- **No transport adapter yet.** NATS is the intended first one, and cheaper than it was: `homelab.nats`
  decoders now take the whole `io.nats.client.Message`, so a `Location` can mint an inbox subject and read a
  reply-to straight off the message — see
  [`../sessions/2026-08-16-mailbox-promotion-nats-codec.md`](../sessions/2026-08-16-mailbox-promotion-nats-codec.md).
- **`Location.get` is unused** by `Incoming`. It exists for the Directory pattern — publishing this process's
  address so peers can reach it.
- **The sweep is O(table) per scan**, paid on a caller's `expect`. Fine while the table holds only in-flight
  expectations; a process holding thousands at once wants a deadline-ordered index instead.
- **`Codec.Encoder`/`Decoder` in `common/data`** are the library default and will meet the `homelab-schemas`
  proto-as-source decision. If protobuf becomes the wire format, these become that library's codecs.
