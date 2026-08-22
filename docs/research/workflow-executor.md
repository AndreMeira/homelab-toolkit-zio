---
title: "A Postgres-backed workflow executor — durable, keyed, multi-node"
type: research
status: draft
updated: 2026-07-14
tags: [workflow, durable-execution, postgres, leases, partitioning, zio, hexagonal]
---

# A Postgres-backed workflow executor — durable, keyed, multi-node

Design exploration for a runtime that drives `homelab.common.flow.Workflow` across nodes: park many
IO-bound multi-step workflows, checkpoint each step so a crash can resume, partition work by key, and
process serially per key. This note elaborates **the Postgres-only option** — Postgres as the durable
state store, the coordination layer, *and* the trigger — so it can be weighed before committing. It does
**not** settle the three open forks (what a "key" is, lease granularity, exactly-once tolerance); it shows
how the Postgres design looks and where each fork would bend it.

Related primitives already explored: [`messaging-design.md`](./messaging-design.md) (subject partitioning,
the key→partition→serial-consumer shape) and [`mailbox-design.md`](./mailbox-design.md) (per-key dispatch /
keyed inbox). This executor reuses their ideas but leans on Postgres rather than a broker for coordination.

---

## 0. Recap — what the executor drives

`Workflow.next: S => ZIO[R, E, Loop.Next[S, (S, O)]]` is a **checkpointable stepper**: the state `S` is the
entire checkpoint, and one `next(S)` is one durable step. So the executor's loop per instance is just:

```
load S → next(S) → { Continue(S') => persist(S'); loop
                     Done(S', O)  => persist terminal(S', O) }
```

Crash resume = load the last persisted `S` and keep calling `next`. Because `next` is **autonomous** (no
per-step input — state drives everything), a message only ever supplies the *initial* `S`; after that the
store, not the queue, sustains the workflow. That split — **the trigger starts it, the store runs it** — is
what lets Postgres own the whole lifecycle.

---

## 1. The three roles Postgres has to play

1. **Durable state store** — hold each instance's `S` and advance it transactionally per step.
2. **Coordination** — ensure *serial-per-key across the cluster* (one node steps a given key at a time) and
   **fence** a crashed/slow owner so it can't corrupt state after being replaced.
3. **Trigger / scheduling** — let a node discover runnable work (new triggers, retries, resumes) with
   acceptable latency.

The appeal of doing all three in Postgres: one datastore you already run, everything transactional, no
second system to operate. The cost shows up in §6.

---

## 2. The state store and the step loop

```sql
create table workflow_instance (
  id            uuid        primary key,
  workflow_type text        not null,               -- selects the right next() + serde
  partition_key text        not null,               -- the serialization key
  state         bytea       not null,               -- serialized S (the checkpoint)
  status        text        not null,               -- pending | running | done | failed
  available_at  timestamptz not null default now(), -- earliest the next step may run (backoff/schedule)
  seq           bigserial,                           -- FIFO order within a key
  output        bytea, error text,                   -- terminal results
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now()
);
create index on workflow_instance (partition_key, seq)
  where status in ('pending', 'running');
```

The **critical constraint**: a step spends most of its time in an external call (OpenRouter), so you must
**not hold a DB lock or connection across the step**. That immediately rules out the obvious
`SELECT … FOR UPDATE SKIP LOCKED` *held for the whole step* — that row lock lives for the transaction, and
you can't keep a transaction (and its connection) open for a 30-second LLM round-trip × thousands of
in-flight workflows.

So the loop grabs a connection only for the short bursts — **claim**, **heartbeat**, **persist** — and holds
nothing while parked on the API:

```
claim a key (short tx)      → hold connection ~1ms
loop:
  next(S)  [OpenRouter]      → NO connection held; fiber parked on IO
  persist(S') (short tx)     → hold connection ~1ms, fenced by the lease token
heartbeat (periodic, short) → renew the lease
```

3,000 parked workflows therefore hold ~0 connections; the pool is sized for *step-commit throughput*, not
for in-flight count. That's what makes "park a lot of pending IO on a few nodes + one database" actually
hold up.

**Keeping the write count down.** Two cheap levers, before scale is even a question. (1) *Checkpoint around
IO, not every step.* You needn't persist after every `next` — persist at durable boundaries (before/after
each external call, which is where you want durability anyway) and let cheap in-memory steps between them
advance un-checkpointed; a crash just re-runs from the last checkpoint (idempotent by contract). A 12-step /
3-call workflow becomes ~6 writes, not ~12. (2) *Fold the lease renewal into the persist.* The step's persist
UPDATE can bump `expires_at` in the same write, so an actively-stepping workflow needs no separate heartbeat —
you emit a standalone keepalive only when a single step outlasts the TTL (the `inProgress()` pattern from the
JetStream heartbeat, reused). If UPDATE churn (bloat/vacuum on the `state` blob) ever bites, the heavier lever
is going **append-only** — INSERT a step-row instead of updating the instance, current state = latest row (or
a periodic snapshot + fold) — trading a compaction job for a write pattern Postgres much prefers.

---

## 3. Serial-per-key + ownership: value leases, not advisory locks

The tempting Postgres trick is **advisory locks**: `pg_advisory_lock(hashtext(key))` gives cluster-wide
mutual exclusion per key, and a session lock **auto-releases when the connection drops** — so a crashed node
releases its keys for free (fencing solved). Elegant — and **wrong for this workload**, because an advisory
lock is held for the life of the *session*: to hold it across a 30-second step you'd hold a *connection* per
active key. Thousands of concurrent keys → thousands of pinned connections. The park-on-IO model and
session-scoped locks are fundamentally at odds.

So ownership is a **value-based lease**, not a held lock:

```sql
create table key_lease (
  partition_key text        primary key,
  owner         text        not null,   -- node id
  token         bigint      not null,   -- monotonic fencing token
  expires_at    timestamptz not null
);
```

- A node **claims a key** by upserting its lease (own it, bump the token, set `expires_at = now() + ttl`) —
  but only if the current lease is expired. At most one node owns a *given* key at a time (single-writer per
  key, so its steps run serially) — but a node holds leases on **many** keys at once, and each lease is
  ephemeral (acquired when it picks up that key's work, released when done). Keys→nodes is many-to-one, so node
  count tracks *throughput* (`nodes × maxConcurrency` simultaneous keys), not key cardinality — you never
  provision a node per key. A node steps *all* of an owned key's instances serially (FIFO by `seq`).
- It **renews** with a single periodic batch UPDATE (`where owner = me`), so N owned keys cost one heartbeat
  query, not N.
- Every state write is **fenced by the token**:

  ```sql
  update workflow_instance
     set state = $s2, status = $st, available_at = $next, updated_at = now()
   where id = $id
     and exists (select 1 from key_lease kl
                  where kl.partition_key = $key and kl.owner = $me
                    and kl.token = $token and kl.expires_at > now());
  -- 0 rows ⇒ the lease was stolen (higher token) ⇒ this node lost the key ⇒ abort the step
  ```

This is the classic **fencing-token** pattern: a resurrected or GC-paused old owner that wakes up after its
lease was reclaimed writes with a stale token, matches 0 rows, and knows to stop — no split-brain, no two
nodes advancing the same key. Serial-per-key holds across the cluster without ever pinning a connection.

The concurrency picture per node: one fiber per owned key, stepping that key's instances serially; many keys
→ many fibers → mostly parked on IO. **Concurrency unit = the key, not the partition** — exactly the
virtual-actor/grain shape, just with the "grain is placed here" fact recorded as a lease row.

**The pattern in one line: _lock to acquire, lease to hold._** Two mechanisms for two timescales — Postgres's
native row lock (`SKIP LOCKED`, held for the microsecond claim tx) picks a key contention-free, then a
value-lease (no lock held) owns it across the long IO. And it's the pairing of `SKIP LOCKED` with
**transactionality** that makes Postgres genuinely hard to replace here: the two hot operations of any work
queue are *acquire* and *advance*, and Postgres does both natively — contention-free concurrent claim, and an
atomic *advance state + update coordination in one commit* (no outbox, no dual-write, because the queue **is**
the state table). On a broker + KV you'd rebuild both, and the atomic-advance one you can't get without a
transaction at all.

---

## 4. Finding runnable work — claim, and how you learn there's work

Two moving parts: *claiming* (safe, done) and *noticing* (latency).

**Claim** uses the short-lived `SKIP LOCKED` pattern — but only to pick a *key to lease*, not to hold across
a step:

```sql
-- pick a claimable key that has runnable instances, in one short tx
select wi.partition_key
  from workflow_instance wi
 where wi.status = 'pending' and wi.available_at <= now()
   and not exists (select 1 from key_lease kl
                    where kl.partition_key = wi.partition_key and kl.expires_at > now())
 order by wi.seq
 for update of wi skip locked
 limit 1;
-- …then upsert key_lease for that key, commit. Total: a few ms.
```

**Noticing** is the latency knob:

- **Polling** — each node loops the claim query every T ms. Dead simple, robust, but T is your floor latency
  and idle nodes hammer the DB. Fine at low rates; wasteful at scale.
- **`LISTEN`/`NOTIFY`** — an enqueue fires `NOTIFY workflow_ready`; nodes `LISTEN` and claim on the nudge, so
  latency is ~instant and idle polling drops. Caveats: NOTIFY is **not durable** (a notify missed while a
  node was disconnected is gone), so you keep a slow poll as a backstop; and each listening node holds one
  dedicated connection.
- **Hybrid (recommended)** — `LISTEN/NOTIFY` for latency + a low-frequency poll (say every few seconds) as
  the safety net that also catches `available_at` (retries/backoff) waking up. This is the standard
  Postgres-queue shape.

The good shape in practice — **don't poll on a fixed tick; sleep on `LISTEN`, and when you sleep on a timer,
sleep until the next real deadline:**

```
loop:
  claim & step everything runnable
  nextDue = select min(available_at) from workflow_instance where status = 'pending'   -- one indexed query
  raceFirst:
    await NOTIFY 'workflow_ready'      -- an enqueue happened → wake instantly
    ZIO.sleep(nextDue - now)           -- a scheduled/backoff item is now due (∞ if nothing is pending)
```

Idle with nothing pending → block on `LISTEN`, **zero queries**, until an enqueue's `NOTIFY` wakes you. Idle
with future work → sleep *exactly* to the next `available_at`, not on a fixed interval. A low-frequency safety
poll (30–60s) backstops missed notifies and sweeps expired leases, so the idle floor is **~1 query/min/node**
instead of one every T ms — two orders of magnitude less idle load. (`NOTIFY` on a **per-partition channel**,
`workflow_ready.<part>`, wakes only the owning node rather than the whole fleet — another convenience of
ownership-by-partition, and it avoids a thundering herd of claim queries per notify.) In ZIO it's a
`ZIO.race` of `ZIO.sleep` against the next-notification effect (the JDBC `getNotifications` loop bridged with
`attemptBlockingInterrupt`).

**Do you even need NATS, then?** For a Postgres-only executor, no — the `workflow_instance` table *is* the
queue (insert = enqueue, claim = dequeue), and it's already transactional with the state, which kills the
dual-write problem (no "commit to DB and publish to broker" inconsistency). You'd add NATS only when you want
sub-millisecond fan-out or cross-service triggering that Postgres NOTIFY can't reach — an additive later
step, not a foundation.

---

## 5. Crash recovery — falls out of the lease

There's no separate recovery path. A node dies → it stops renewing → its key leases expire → other nodes'
claim queries see those keys as available (runnable `pending`/`running` instances whose lease is expired) →
they reclaim (token bumps) and resume from the stored `S`. The fencing token means the dead node, if it
comes back mid-step, can't clobber the new owner. **The store is the source of truth for "what's
incomplete"; the lease is the source of truth for "who runs it now."** No queue replay, no separate scan
job — recovery is just the normal claim loop noticing expired leases.

The one window this *doesn't* close is the external side effect — see §6.1.

---

## 6. Where it strains (the honest limits)

1. **The non-transactional side effect — the fundamental wall.** Persisting `S'` is transactional; the
   OpenRouter call inside the step is not. Crash *after* the call, *before* the persist → resume re-runs the
   step → a duplicate call. Postgres can't fix this; nothing can, fully. Mitigations live in the *step*: make
   it idempotent, or pass an **idempotency key** derived from `(instance id, step)` so the provider dedupes,
   or split a risky effect into a "record intent (in the state tx) → do → record result" shape that shrinks
   the window. `Workflow` already declares the at-least-once-per-step + idempotent-step contract; this is
   just where it bites hardest. (Fork #3.)
2. **Lease TTL tuning.** Too short → a legitimately slow step gets its lease stolen mid-flight (the fencing
   token saves correctness but you wasted work and may double-call). Too long → slow failover after a real
   crash. The heartbeat interval must be comfortably below the TTL, and the TTL comfortably above the slowest
   expected step — which for LLM calls can be tens of seconds. This is the tuning that'll bite in practice.
3. **Postgres as a queue at throughput.** `SKIP LOCKED` claim + per-step UPDATEs mean every workflow step is
   ≥2 short transactions. At thousands of steps/second this is real write/WAL load and index churn on
   `workflow_instance`; `NOTIFY` fan-out and `LISTEN` connections add up. Postgres-as-queue is well-trodden to
   ~low-thousands of ops/sec on modest hardware, but it *is* a ceiling.
4. **Key-lease cardinality.** Dozens–thousands of concurrent keys per node: fine (a few thousand lease rows,
   one batch heartbeat). Millions of active keys: the `key_lease` table churn and the claim query's
   `not exists` anti-join become the bottleneck, and you'd want partition-bucketing (lease a *partition* of
   keys, not each key) — which trades finer parallelism for cheaper coordination. (Fork #2.)
5. **Completed-instance growth.** `done`/`failed` rows accumulate; you need a retention/archival sweep, and
   the partial indexes (`where status in (...)`) keep the hot path lean only if you actually prune.

---

## 7. How the three open forks reshape this

- **What a "key" is (ephemeral vs long-lived).** *Ephemeral*: `partition_key` is just the serialization
  bucket, each trigger is its own row, and "serial per key" = one node steps that key's rows in `seq` order.
  The schema above is already this. *Long-lived (actor)*: the key *is* a durable entity with one evolving
  state, and triggers are events appended to it — you'd add an `event` inbox table per key and `next` would
  need per-step input (a different `Workflow` shape). This fork changes whether `workflow_instance` is
  many-rows-per-key or one-long-lived-row-per-key.
- **Lease granularity (per-key vs per-partition).** Per-key leases (above) give maximal parallelism and
  natural serial-per-key, at the cost of a lease row per active key. Per-partition leases (hash key → K
  buckets, lease the bucket) cap coordination cost at K rows and match the NATS subject-partitioning model,
  but serialize *all keys in a bucket* onto one node's single owner — you'd then re-introduce per-key
  ordering *within* the node's dispatcher. Fork #2 is really "how fine is the ownership unit."
- **Exactly-once tolerance (Fork #3).** If steps are naturally idempotent / idempotency-keyable (typical for
  LLM calls where a duplicate is cost, not corruption), the plain lease design is enough. If some step has a
  true exactly-once effect (charge, irreversible send), you need the intent-log shape in §6.1 and to accept
  its residual window — or to move that step behind its own idempotent boundary.

---

## 8. When you'd outgrow Postgres-only

- **Latency floor / fan-out** beyond what `LISTEN/NOTIFY` gives → add NATS as the *trigger* transport (the
  partitioned-subject work in `messaging-design.md`), keeping Postgres as state. The store still sustains;
  NATS just wakes nodes faster and reaches across services.
- **Throughput** past Postgres-as-queue's ceiling, or **millions of concurrent keys** → either partition
  more coarsely (per-partition leases + membership) or reach for a purpose-built engine (Temporal/Cadence),
  accepting its deterministic-replay model and operational weight. The `Workflow` step-checkpoint design is
  deliberately *not* that; this note is the "how far does the simple thing go" answer, and for homelab-scale
  IO-bound workflows it goes quite far.

---

## Related

- [`workflow-executor-impl.md`](./workflow-executor-impl.md) — a ZIO implementation sketch of the executor
  (fiber pool, lease finalizer, error policy, the step/claim/heartbeat loops).
- `homelab.common.flow.Workflow` — the stepper this executor drives (state `S` = the checkpoint).
- [`messaging-design.md`](./messaging-design.md) — subject partitioning; the alternative/complementary
  trigger transport if Postgres-only latency isn't enough.
- [`mailbox-design.md`](./mailbox-design.md) — per-key dispatch / keyed inbox; the same virtual-actor shape
  the executor uses locally.
- [`error-modeling.md`](./error-modeling.md) — where step failures (`E`) map onto `ApplicationError`.
