---
title: "Redis streams, and what they decide for you"
type: learning-material
status: current
updated: 2026-09-01
tags: [redis, valkey, streams, consumers, lettuce, cluster]
---

# Redis streams, and what they decide for you

Two halves. The first is a tutorial you can type — every command below was run against
`valkey/valkey:8.1-alpine` and the output is what came back. The second is the part that matters when you
write an adapter: the handful of stream properties that decide, rather than influence, how a consumer must
be built.

---

# Part 1 — The tutorial

Start a throwaway server and talk to it:

```bash
docker run --rm -d -p 6379:6379 --name streams valkey/valkey:8.1-alpine
alias R='docker exec streams valkey-cli'
```

## 1. Append, and look

A stream is an append-only log. `XADD` appends; `*` asks the server to assign the id.

```
> XADD orders * item socks qty 2
1788298588492-0
> XADD orders * item hat qty 1
1788298588561-0
```

An id is `<unix-millis>-<sequence>`. The sequence disambiguates entries added within the same millisecond,
and ids only ever increase. An entry's payload is a flat list of field/value pairs — `item socks qty 2`
above.

```
> XLEN orders
2
> XRANGE orders - +
1788298588492-0
item
socks
qty
2
1788298588561-0
item
hat
qty
1
```

`-` and `+` are the smallest and largest possible ids. `XREVRANGE` walks backwards, which is how you ask
"where is this stream right now":

```
> XREVRANGE orders + - COUNT 1
1788298588561-0
item
hat
qty
1
```

Remember that one — it comes back in Part 2.

## 2. Reading as a tail

`XREAD` returns entries **strictly after** the id you give it.

```
> XREAD COUNT 10 STREAMS orders 1788298588492-0
orders
1788298588561-0
item
hat
qty
1
```

Pass `0` and you get everything from the beginning:

```
> XREAD COUNT 1 STREAMS orders 0
orders
1788298588492-0
item
socks
qty
2
```

Two things follow immediately, and they are the whole character of `XREAD`:

- **Reading does not consume.** The entries are still there; ten readers can each read all of them. Where
  a reader has got to is *the reader's* business, held in the id it passes next time.
- **`$` means "the stream's last id, as of this call"** — not a bookmark. `XREAD BLOCK 5000 STREAMS orders $`
  waits for what arrives *after the call starts*. Re-evaluating `$` on the next call is a bug waiting to
  happen; Part 2 has the story.

`BLOCK <ms>` parks the connection until something arrives or the timeout expires; on timeout you get a nil
reply rather than an error. `BLOCK 0` waits forever.

## 3. Consumer groups

`XREAD` broadcasts. A **consumer group** does the opposite: each entry goes to one consumer, and the server
remembers what has not been acknowledged.

```
> XGROUP CREATE orders packers 0
OK
> XREADGROUP GROUP packers alice COUNT 1 STREAMS orders >
orders
1788298588492-0
item
socks
qty
2
```

`0` is where the group starts (`$` would start it at the end, ignoring history). `>` means "entries never
delivered to this group" — the only id that reads new work; any other id re-reads *that consumer's* pending
entries.

The delivery was recorded in the **pending entries list**:

```
> XPENDING orders packers
1                        ← how many are owed
1788298588492-0          ← smallest pending id
1788298588492-0          ← largest
alice                    ← per-consumer counts follow
1
> XPENDING orders packers - + 10
1788298588492-0
alice
146                      ← milliseconds idle
1                        ← times delivered
```

`XACK` commits, and the entry leaves the list:

```
> XACK orders packers 1788298588492-0
1
> XPENDING orders packers
0
```

**That is the whole ack story**: a group turns "I read it" and "I finished it" into two separate facts, and
the gap between them is what makes recovery possible.

## 4. When a consumer dies

Deliver a second entry to `alice`, then imagine she never comes back. `XAUTOCLAIM` walks the pending list
and hands anything idle longer than a threshold to somebody else:

```
> XREADGROUP GROUP packers alice COUNT 1 STREAMS orders >     (delivers 1788298588561-0)
> XAUTOCLAIM orders packers bob 0 0-0
0-0                      ← cursor to resume from; 0-0 means the walk is complete
1788298588561-0          ← the entries now owned by bob
item
hat
qty
1
```

The `0` is the minimum idle time in milliseconds — everything, here. A real caller passes "longer than any
handler should take". The trailing `0-0` is where to start walking; the first element of the reply is where
to continue on the next pass.

Introspection shows the ownership move:

```
> XINFO GROUPS orders
name              packers
consumers         2
pending           1
last-delivered-id 1788298588561-0
entries-read      2
lag               0
> XINFO CONSUMERS orders packers
name    alice     pending 0    idle 205
name    bob       pending 1
```

`lag` is how many entries the group has never been delivered — the number to alert on.

## 5. Trimming, and the trap

Streams do not shrink because you read them. The writer bounds them:

```
> XADD orders MAXLEN 1 * item scarf
```

`MAXLEN 1000` is exact; `MAXLEN ~ 1000` trims only at internal node boundaries, is much cheaper, and is
what you want on a hot path. `MINID <id>` trims by id, which — because ids embed a timestamp — is really
"drop anything older than".

Now watch what that did:

```
> XLEN orders
1
> XPENDING orders packers
1
1788298588561-0
1788298588561-0
bob
1
> XRANGE orders 1788298588561-0 1788298588561-0
(empty)
```

**The entry bob still owes no longer exists.** Trimming does not consult the pending list. And when
somebody tries to reclaim it:

```
> XAUTOCLAIM orders packers carol 0 0-0
0-0
(no messages)
1788298588561-0          ← third reply element: ids evicted from the PEL because their data is gone
> XPENDING orders packers
0
```

Redis quietly drops the dangling reference. No error, no dead-letter — the work is simply gone. If you are
using a group as a work queue, **your trimming policy is part of your durability story**, and `MAXLEN ~` on
every append is not safe by default.

## 6. Cluster

Slots are computed from the key, or from the part between the first `{` and the next `}` if there is one:

```
{q:orders}:ready   → 7541
{q:orders}:wake    → 7541
{q:emails}:ready   → 5004
dkq:wake           → 9292
```

Multi-key commands need one slot, and `XREAD` is multi-key — the server extracts its keys from between
`STREAMS` and the ids, which you can see for yourself:

```
> COMMAND GETKEYS XREAD COUNT 1 STREAMS a b 0 0
a
b
```

So streams you want to read in one call must share a hash tag; otherwise it is `CROSSSLOT`, and you need a
reader per slot.

---

# Part 2 — What it constrains

Everything below follows from Part 1. These are not preferences.

## The two reading modes are two different contracts

| | `XREAD` | `XREADGROUP` |
|---|---|---|
| delivery | every reader gets every entry | one consumer per entry |
| position | yours, in memory | the server's, per group |
| unacknowledged work | no such concept | the pending list |
| recovery | nothing to recover | `XAUTOCLAIM` |
| fits | notification, fan-out, tailing | work queues |

Choosing wrongly is not a tuning mistake. A group used as a doorbell delivers a wake-up to one instance,
possibly one with nobody waiting, while another instance's consumer waits. A broadcast used as a work queue
has every instance doing the same job.

## A blocking read owns its connection

`XREAD BLOCK` and `XREADGROUP BLOCK` park the connection for the whole wait, exactly like `BLPOP`. Three
consequences:

1. A blocking reader needs a connection of its own; sharing one puts every other command behind the wait.
2. The client's command timeout must exceed `BLOCK`, or the client abandons a read that is doing what it
   was told to. In Lettuce that is `connection.setTimeout`, and getting it wrong looks like
   `RedisCommandTimeoutException` on an idle system.
3. Cancelling a blocked read is not free — a sync client is inside a socket read, so shutdown either waits
   out the block or interrupts the thread. Prefer a finite `BLOCK` you can afford to wait for.

## `$` is not a bookmark

`$` is resolved by each call, at that call. A reader that stores the literal `$` as "where I am" will:

- skip everything appended between two reads, because the second read's `$` is evaluated later;
- do it silently, since a stream never reports what you did not ask for.

This is not hypothetical — it is exactly what the first version of the toolkit's `StreamTailConsumer` did,
and it took a Testcontainers test to see it. **Resolve "from now" once**, with `XREVRANGE key + - COUNT 1`
(or `0-0` when the stream is empty), and store that concrete id.

## Where you advance the offset is your delivery semantics

With `XREAD` there is no ack, so the only decision is when you move your id:

- advance **before** running the logic → at-most-once; a crash loses the entry;
- advance **after** → at-least-once; a crash or a failure re-delivers it.

Neither is "the" answer; for notifications at-least-once is nearly free, because a duplicate wake costs one
wasted look.

## A group gives no ordering and no exclusivity

Two entries related to the same customer can be handed to two consumers at the same time, in either order.
Nothing in a group is per-key. If you need per-key serialisation, a stream is the wrong primitive — that is
a claim-and-lease design over a different structure (see `distributed-keyed-queue`), and no amount of group
configuration substitutes for it.

## Idle time decides what "dead" means

`XAUTOCLAIM` reclaims by *idle time*, which counts from the last delivery or claim. So a handler slower
than the threshold is indistinguishable from a dead consumer, and its entry will be given to a peer while
it still runs. Two ways out:

- set the minimum idle time above the slowest handler you will tolerate; or
- re-claim the entry for yourself periodically — `XCLAIM key group consumer 0 <id> JUSTID` resets the
  clock. That is a lease keepalive by another name.

## Consumer names are durable state

A group remembers every consumer name it has seen, with its pending count and idle time. Name consumers
after the process instance and you accumulate one dead consumer per restart, visible forever in
`XINFO CONSUMERS`. Either use stable names (`worker-0 … worker-n`) or call `XGROUP DELCONSUMER` on a clean
shutdown — after reclaiming what it held, since deleting a consumer discards its pending entries.

## Recovery is polled, not pushed

Nothing tells you an entry has gone stale. `XAUTOCLAIM` is something you schedule, so **recovery latency is
your tick interval**, and the tick has to be short enough to matter while long enough not to fight the
keepalive above.

## Trimming can destroy work in flight

Shown in Part 1 §5: `MAXLEN` deleted an entry that a consumer still owed, and `XAUTOCLAIM` then evicted the
dangling reference without a word. For a work queue this means:

- trim by `MINID` behind the oldest pending entry, or trim generously enough that it cannot catch up with
  in-flight work;
- treat `XADD MAXLEN ~ n` as safe only where the stream is a notification bus and losing an unread entry
  costs latency rather than data.

Note also that Lettuce's `ClaimedMessages` exposes only the cursor and the claimed messages — the third
reply element, the evicted ids, is not surfaced, so you cannot observe this through the client.

## Cluster decides your connection count

A multi-key `XREAD` needs one slot, and a blocking read needs its own connection, so **the number of
blocking readers is the number of slots you tail**, not the number of streams and not the number of nodes.
Two slots on one node still need two connections.

And when a slot moves — resharding, or a failover promoting a replica — a client blocked on the old owner
is unblocked with an error rather than following a redirect. A reader must therefore be supervised: catch
the failure, let the client refresh topology, reopen, and resume from the id it holds. Resuming is lossless
precisely because the offset is in memory.

## `COUNT` bounds the reply, not the wait

`XREAD COUNT 64 BLOCK 5000` returns as soon as *one* entry exists, with up to 64. It is not a batch size to
wait for. If you want batching by time, you build it above the read.

---

# Where the code is

Three sketches in `modules/incubator/src/main/scala/homelab/incubator/messaging/redis/`, each with a
Testcontainers spec:

- `StreamConsumer` — `XREADGROUP` + `XACK` + `XAUTOCLAIM`, i.e. a work-queue consumer. Its failure policy
  (`Redeliver` / `Discard` / `Surface`) is the ack decision made explicit.
- `StreamTailConsumer` — `XREAD` over many streams, offsets held in memory, resolved once at `watch`.
- `ClusterStreamTail` — one of the above per slot, fanned into a single queue.

They are sketches: connection ownership is documented rather than enforced, Lettuce types appear in their
signatures, and resharding is not handled.
