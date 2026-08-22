---
title: "Workflow executor — the store as the port, topology shapes as projections"
type: research
status: draft
updated: 2026-07-17
tags: [workflow, durable-execution, zio, messaging, topology, postgres, sketch]
---

# Workflow executor — the store as the port, topology shapes as projections

A side note to [`workflow-executor-impl.md`](./workflow-executor-impl.md), recording an alternative
decomposition that fell out of building the messaging primitives (`Consumer`, `Pipe`, `Processor`,
`Distributer`). Where the impl sketch designs a bespoke worker **pool** (and a partition-dispatcher
variant), this note takes a different stance:

> **`WorkflowStore` is the only real port. The executor is a *trivial projection* of the store onto
> whichever topology trait the caller wants — `Consumer`, `Pipe`, or `Processor`.** The workflow itself
> stays a pure `Workflow.Simple`; the store is the one authored adapter; the topology shape is a façade.

The claim in one line: **once you have the store, a `Consumer` / `Pipe` / `Processor` is a one-liner.**
That drops the executor from "a concept" to "a projection", and keeps the topology property (a workflow's
output is a wired port, not an imperative publish buried in a lambda).

---

## Three layers, one authored piece

```
Workflow.Simple[S, O]     pure domain state machine: next(s): IO[E, Loop.Next[S, (S, O)]]   (no ports, no store, no serde)
      │  (driven by)
WorkflowStore  +  drive   the port (durability + cross-node serial) and the fold that steps it            ← the only authored code
      │  (projected as)
Consumer | Pipe | Processor   one-line façades over (store + drive), differing only at the output edge
```

- **Layer 1 — `Workflow` (unchanged).** The state machine. Trivially unit-testable: feed a state, assert
  the transition. No knowledge of persistence, leasing, serde, or topology.
- **Layer 2 — `WorkflowStore` + `drive` (the substance).** The store is the durable, cross-node port;
  `drive` is the reused fold that steps one instance under fencing. This is the *only* code with real
  logic; everything above and below is pure domain or thin wiring.
- **Layer 3 — the topology projection.** `Consumer` / `Pipe` / `Processor`, each a wrapper over layer 2.

This is textbook hexagonal for the toolkit: `WorkflowStore` is the **port**, the Postgres implementation
is the **real adapter**, and the topology traits are **presentation adapters** over the port.

---

## The port: `WorkflowStore`

Everything that a distributed durable executor needs but in-memory primitives *cannot* give you lives
here — **durability** (the queue, checkpoints, terminal states) and the **cross-node serial gate** (the
one thing a `Distributer`'s in-memory per-key serial can't provide: stopping *another node* from running
the same key). Every method is a short transaction; none is held across a step. Failures are
`ApplicationError.AdapterError` (like the NATS adapter's `NatsError`).

```scala
type NodeId       = String
type WorkflowId   = String
type PartitionKey = String
type PartitionId  = Int
type LeaseToken   = Long

/** A leased, runnable instance handed to whoever drives it: identity, the key it serialises on, its
 *  type, the serialized state, the fencing token proving current ownership, and the durable attempt. */
final case class Claimed(
  id:           WorkflowId,
  key:          PartitionKey,
  workflowType: String,
  state:        Array[Byte],
  token:        LeaseToken,
  attempt:      Int,
)

trait WorkflowStore:

  // ── intake: two styles; a projection picks one ────────────────────────────────────────────────

  /** Pull one runnable instance whose key is free and lease that key to `node` for `ttl`
   *  (`SELECT … FOR UPDATE SKIP LOCKED` + lease upsert, one tx). `None` ⇒ no runnable, un-leased work.
   *  The *pull* intake — projects straight to a `Consumer[E, Claimed]`; the store is the serial gate. */
  def claim(node: NodeId, ttl: Duration): IO[ApplicationError, Option[Claimed]]

  /** Earliest runnable instance *per key* for a partition this node owns (`DISTINCT ON (key)`), up to
   *  `limit`. The *push* intake — feeds a `Distributer`, which then supplies in-memory per-key serial
   *  under a coarse partition lease. One-per-key is deliberate: it stops a hot key starving the buffer. */
  def pending(partition: PartitionId, limit: Int): IO[ApplicationError, List[Claimed]]

  // ── the durable step boundary (every write fenced by `work.token`) ────────────────────────────

  /** Persist a checkpoint and renew the lease in one fenced write. `false` ⇒ the token is stale (the
   *  key/partition lease was stolen) ⇒ the caller has lost the key and must stop; the new owner resumes
   *  from the last checkpoint. */
  def persist(work: Claimed, state: Array[Byte]): IO[ApplicationError, Boolean]

  /** Mark the instance done with its output (fenced). */
  def complete(work: Claimed, output: Array[Byte]): IO[ApplicationError, Unit]

  /** Reschedule to run no earlier than `at`, bumping the attempt count — a durable retry. */
  def reschedule(work: Claimed, at: Instant): IO[ApplicationError, Unit]

  /** Mark the instance permanently failed. */
  def fail(work: Claimed, reason: String): IO[ApplicationError, Unit]

  // ── liveness & scheduling ─────────────────────────────────────────────────────────────────────

  /** Renew every lease this node owns, one batch UPDATE (the heartbeat). */
  def renewLeases(node: NodeId, ttl: Duration): IO[ApplicationError, Unit]

  /** Hand a key lease back for fast failover (TTL expiry is the correctness backstop). */
  def releaseLease(work: Claimed): IO[ApplicationError, Unit]

  /** Soonest `available_at` among pending work — a precise idle sleep. */
  def nextDeadline: IO[ApplicationError, Option[Instant]]

  /** Block until a `workflow_ready` NOTIFY arrives (LISTEN). */
  def awaitReady: IO[ApplicationError, Unit]
```

The **submit** side is its dual — a `Producer`-shaped write port (`WorkflowClient.submit`, in the impl
note) that inserts a pending row and fires the `workflow_ready` NOTIFY. It's out of scope here; this note
is about the read/drive side that the topology projections wrap.

### What the store owns — and deliberately doesn't

| Owns (the port) | Does *not* own (lives elsewhere) |
| --- | --- |
| Durability: the queue, checkpoints, terminal states | The state machine → `Workflow.Simple` (pure) |
| The **cross-node** serial gate: leases + fencing token | In-memory concurrency / per-key serial → `Distributer` or a worker pool |
| Scheduling metadata: `available_at`, LISTEN/NOTIFY | (De)serialization → the `Serde` the driver carries |
| Fenced writes (`persist`/`complete`/`reschedule`/`fail`) | The topology shape → the projection |

The split that matters: **the store provides serial *across nodes* (leases); the `Distributer` provides
serial *within a node* (per-key rounds).** They compose — the partition/key lease says "this node may run
this key", the `Distributer` says "one at a time inside this node". Neither alone is enough; conflating
them is the mistake.

### The two intake methods are the round-vs-fork fork in the road

`claim` and `pending` are not redundant — they are the two intake styles from the impl note, surfaced as
two store methods so the *projection* chooses, not the store:

- **`claim` (pull).** One instance at a time, key leased per-claim. Projects directly to a
  `Consumer[E, Claimed]`. The DB is the serial gate; heavier per-instance DB traffic.
- **`pending` (push).** A batch of earliest-per-key rows for an owned partition; fed into a `Distributer`
  for in-memory serial. Lighter DB (coarse partition lease + per-step persists only); carries the
  partition-membership churn.

Both present downstream as *the same thing* — a `Consumer[E, Claimed]` — so the executor projection
never sees which one it is. That decoupling is the whole point: **intake mechanism is an adapter choice
below the `Consumer` seam.**

### The adapter — `PostgresWorkflowStore` (sketch)

Syntax-plausible, not compiled — the point is the SQL shape behind each port method, not a working
`Fragment` API. Two tables: the durable instance queue, and the side lease table that *is* the cross-node
serial gate.

```sql
CREATE TABLE workflow_instance (
  id            uuid        PRIMARY KEY,
  workflow_type text        NOT NULL,
  key           text        NOT NULL,                 -- the partition/serial key
  state         bytea       NOT NULL,                 -- the checkpoint (opaque to the store)
  status        text        NOT NULL DEFAULT 'pending', -- pending | done | failed
  available_at  timestamptz NOT NULL DEFAULT now(),   -- earliest run time (retry backoff writes this)
  attempt       int         NOT NULL DEFAULT 0,
  output        bytea,                                 -- set on completion
  seq           bigserial   NOT NULL                   -- per-key FIFO tiebreak
);
CREATE INDEX ON workflow_instance (status, available_at, seq);

CREATE TABLE key_lease (
  key        text        PRIMARY KEY,                  -- one live lease per key ⇒ one owner per key
  node       text        NOT NULL,
  token      bigint      NOT NULL,                     -- fencing token, bumped on every (re)acquire
  expires_at timestamptz NOT NULL
);
```

The **fencing token** is `key_lease.token`, bumped on each acquire; every durable write checks
`token = work.token AND expires_at > now()`, so a slow former owner whose lease was stolen fails its
writes and stops. Each method is one short `transact`:

```scala
final class PostgresWorkflowStore(xa: Transactor) extends WorkflowStore:

  /** Pull one runnable instance whose key has no live lease, lock it (SKIP LOCKED), and upsert the key
   *  lease (bumping the token) — all in one statement so the claim and the fence are atomic. */
  def claim(node: NodeId, ttl: Duration): IO[ApplicationError, Option[Claimed]] =
    sql"""
      WITH candidate AS (
        SELECT wi.id, wi.key, wi.workflow_type, wi.state, wi.attempt
        FROM workflow_instance wi
        WHERE wi.status = 'pending' AND wi.available_at <= now()
          AND NOT EXISTS (SELECT 1 FROM key_lease kl WHERE kl.key = wi.key AND kl.expires_at > now())
        ORDER BY wi.available_at, wi.seq
        FOR UPDATE OF wi SKIP LOCKED
        LIMIT 1
      ),
      lease AS (
        INSERT INTO key_lease (key, node, token, expires_at)
        SELECT key, $node, 1, now() + $ttl FROM candidate
        ON CONFLICT (key) DO UPDATE
          SET node = excluded.node, token = key_lease.token + 1, expires_at = excluded.expires_at
        RETURNING key, token
      )
      SELECT c.id, c.key, c.workflow_type, c.state, l.token, c.attempt
      FROM candidate c JOIN lease l ON l.key = c.key
    """.query[Claimed].option.transact(xa).mapError(fromSql)

  /** Persist a checkpoint AND renew the lease in one fenced write. Returns whether the fence held
   *  (`false` ⇒ token stale ⇒ the caller lost the key). */
  def persist(work: Claimed, state: Array[Byte]): IO[ApplicationError, Boolean] =
    sql"""
      WITH renewed AS (
        UPDATE key_lease SET expires_at = now() + ${config.leaseTtl}
        WHERE key = ${work.key} AND token = ${work.token} AND expires_at > now()
        RETURNING key
      )
      UPDATE workflow_instance wi SET state = $state
      FROM renewed WHERE wi.id = ${work.id}
      RETURNING wi.id
    """.query[WorkflowId].option.transact(xa).map(_.isDefined).mapError(fromSql)

  /** Terminal writes are fenced the same way — the `EXISTS` guard is the token check. */
  def complete(work: Claimed, output: Array[Byte]): IO[ApplicationError, Unit] =
    sql"""
      UPDATE workflow_instance SET status = 'done', output = $output
      WHERE id = ${work.id} AND EXISTS (
        SELECT 1 FROM key_lease WHERE key = ${work.key} AND token = ${work.token} AND expires_at > now())
    """.update.transact(xa).unit.mapError(fromSql)

  def reschedule(work: Claimed, at: Instant): IO[ApplicationError, Unit] =
    sql"""UPDATE workflow_instance SET available_at = $at, attempt = attempt + 1
          WHERE id = ${work.id}""".update.transact(xa).unit.mapError(fromSql)

  def fail(work: Claimed, reason: String): IO[ApplicationError, Unit] =
    sql"""UPDATE workflow_instance SET status = 'failed' WHERE id = ${work.id}"""
      .update.transact(xa).unit.mapError(fromSql)

  /** Heartbeat: one batch UPDATE across every lease this node holds. */
  def renewLeases(node: NodeId, ttl: Duration): IO[ApplicationError, Unit] =
    sql"""UPDATE key_lease SET expires_at = now() + $ttl WHERE node = $node"""
      .update.transact(xa).unit.mapError(fromSql)

  /** Fast hand-off: drop the lease (token-checked) so a peer can claim the key without waiting for TTL. */
  def releaseLease(work: Claimed): IO[ApplicationError, Unit] =
    sql"""DELETE FROM key_lease WHERE key = ${work.key} AND token = ${work.token}"""
      .update.transact(xa).unit.mapError(fromSql)

  def nextDeadline: IO[ApplicationError, Option[Instant]] =
    sql"""SELECT min(available_at) FROM workflow_instance WHERE status = 'pending'"""
      .query[Option[Instant]].unique.transact(xa).mapError(fromSql)

  /** Scoped LISTEN on a dedicated connection; `submit` fires `NOTIFY workflow_ready` on insert. */
  def awaitReady: IO[ApplicationError, Unit] = ??? // LISTEN workflow_ready — connection-level, elided

  /** Partition (push) intake: earliest row per key for an owned partition. Fed to a `Distributer`. */
  def pending(partition: PartitionId, limit: Int): IO[ApplicationError, List[Claimed]] =
    sql"""
      SELECT DISTINCT ON (key) id, key, workflow_type, state, 0 AS token, attempt
      FROM workflow_instance
      WHERE status = 'pending' AND available_at <= now() AND (hashtext(key) % ${config.partitions}) = $partition
      ORDER BY key, seq
      LIMIT $limit
    """.query[Claimed].to[List].transact(xa).mapError(fromSql)
```

Notes on the sketch:

- **`claim` folds the SELECT, the lock, and the lease upsert into one CTE** so there's no window between
  "found a free key" and "leased it". `SKIP LOCKED` lets many nodes claim concurrently without blocking;
  `NOT EXISTS (live lease)` is the cross-node serial gate; the `ON CONFLICT … token + 1` bump is what
  invalidates a stolen predecessor.
- **`persist` returning `false` is the whole recovery story** — a slow node whose lease expired and was
  re-leased (higher token) writes zero rows, learns it lost the key, and stops; the new owner resumes
  from the last committed checkpoint. Idempotent external calls make the re-run safe.
- **`pending`'s `token` is a placeholder** — the *partition* lease fences that path (a separate
  `partition_lease` table, not shown), so the per-key token is unused there. Same `Claimed`, different
  gate.
- **`fromSql`** wraps the driver exception into an `ApplicationError.AdapterError` at the edge (the NATS
  adapter's `NatsError` pattern), so `Throwable` never reaches the driver or the projections.

---

## The two reused cores

Given the store, exactly two pieces carry logic. Write them once.

**1 — the driver.** Fold one instance to completion (or a retry/failure), fenced at every checkpoint.
Returns `Some(output)` on `Done`, `None` otherwise (lease lost, rescheduled, or failed).

```scala
// captured over: workflow: Workflow.Simple[S, O], serde: Serde[S] & Serde[O], store, config
private def drive(work: Claimed): UIO[Option[O]] =
  def loop(state: S): IO[ApplicationError, Option[O]] =
    workflow.next(state).flatMap {
      case Loop.Next.Continue(next) =>
        store.persist(work, serde.encode(next)).flatMap {
          case true  => loop(next)          // fenced checkpoint held → advance
          case false => ZIO.none            // token stale → stop; new owner resumes
        }
      case Loop.Next.Done((_, out)) =>
        store.complete(work, serde.encode(out)).as(Some(out))
    }

  decode(work.state)
    .flatMap(loop)
    .catchAll(onError(work, _).as(None))    // TransientError+attempts → reschedule; else → fail
    .catchAllDefect(onDefect(work, _).as(None))
    .ensuring(store.releaseLease(work).ignore)   // the one finalizer — fast hand-off
```

**2 — the intake loop.** Claim (or drain a partition), hand each `Claimed` to a handler, idle precisely
when there's nothing to do. Plus a heartbeat fiber renewing leases. Both come straight off the store and
are identical regardless of the topology shape.

```scala
private def pump(handle: Claimed => UIO[Unit]): UIO[Unit] =
  store.claim(node, config.leaseTtl).orDie.flatMap {
    case Some(work) => handle(work)
    case None       => idle                 // awaitReady race (nextDeadline sleep)
  }.forever
```

Nothing above is shape-specific. The only thing a shape decides is **where `drive`'s `Some(output)`
goes.**

---

## The projections — one line each, differing only at the output edge

```scala
final class WorkflowExecutor[S, O](/* store, workflow, serde, node, config */):

  private def drive(work: Claimed): UIO[Option[O]] = ???   // as above
  private def pump(handle: Claimed => UIO[Unit]): UIO[Unit] = ???

  /** Raw intake — the store's claim loop as a Consumer of leased work. The base every other shape wraps. */
  def intake: Consumer[ApplicationError, Claimed] = new Consumer:
    def consume[E2 >: ApplicationError](logic: Claimed => IO[E2, Unit]): IO[E2, Unit] =
      store.claim(node, config.leaseTtl).flatMap {
        case Some(work) => logic(work)
        case None       => idle
      }

  /** Pipe — drive to completion, emit the terminal output to a *declared* port. Topology preserved. */
  def pipe(output: Producer[ApplicationError, O]): Pipe[ApplicationError, Claimed, O] = new Pipe:
    def input  = intake
    def output = output
    def run    = pump(work => drive(work).flatMap {
                   case Some(o) => output.emit(o).orDie
                   case None    => ZIO.unit
                 })

  /** Processor — same driver, fanned to owned producers (progress events + terminal), no single output. */
  def processor(events: Producer[ApplicationError, Ev],
                output: Producer[ApplicationError, O]): Processor[ApplicationError, Claimed] = new Processor:
    def input = intake
    def run   = pump(work => drive(work) /* … emit events during, output on Done … */ )
```

- **`Consumer[E, Claimed]`** — the intake itself: `consume` claims one instance and hands it to a
  caller-supplied logic. The workflow-as-a-consumer your original pipeline sketched.
- **`Pipe[E, Claimed, O]`** — intake + `drive` + a **declared `output` port**: the result leaves as a
  wired edge, so "publish the output elsewhere" is topology, not an imperative publish in a lambda.
- **`Processor[E, Claimed]`** — intake + `drive` + owned producer fields: multi-output (progress events
  plus a terminal), the shape `Processor`'s own doc points to for fan-out.

A fourth, if you want to *pull* completed outputs rather than push them: a `Consumer[E, O]` whose
`consume(logic)` drives one instance and delivers its output — `intake` post-composed with `drive`. Rare;
noted for completeness.

---

## Trade-offs and open questions

- **Round vs fork is now `intake`'s problem, not the executor's.** If `intake` is `claim`-backed (pull),
  each instance is independent. If it's a `Distributer` fed by `pending`, the `Distributer`'s round model
  couples a round's keys — a long step head-of-line-blocks the next poll (see the impl note's variant 2).
  A fork-per-key `Distributer` mode would remove that; either way it's decided *below* the `Consumer`
  seam, invisible to the projection. **This is the main thing to resolve before committing.**
- **Durability doesn't cross an in-memory `Producer`.** `store.complete` then `output.emit` are two
  effects; if `output` is an in-memory `Wire` and the node dies between them, the result is durably "done"
  but never published. For at-least-once downstream, make `output` a durable producer, or treat the
  `done` row itself as the published record a downstream consumer reads.
- **Idempotency is still assumed.** Fencing + last-checkpoint resume re-runs a partially-done step; the
  workflow's external calls must be idempotency-keyed, exactly as the impl note requires.
- **`drive` swallows `S`/`O` types; the projection is monomorphic per workflow type.** A registry
  (`workflowType → WorkflowExecutor`) erases them, as in the impl note's `WorkflowRunner.of`.

## Why this is the minimal option

The only genuinely authored code is **the store adapter** (SQL, leasing, fencing) and **`drive`** (the
fold). `Workflow` stays pure; the intake loop, heartbeat, and all three topology shapes are wiring over
the port. No new concept is introduced — `Consumer`/`Pipe`/`Processor` already exist; the executor merely
*projects* onto them. That is the smallest surface that (a) keeps the workflow simple, (b) preserves the
topology property, and (c) leaves the round-vs-fork intake decision swappable.

---

## Related

- [`workflow-executor-impl.md`](./workflow-executor-impl.md) — the pool and partition-dispatcher
  variants this note reframes; `Claimed` / `WorkflowStore` / `WorkflowRunner` originate there.
- [`workflow-executor.md`](./workflow-executor.md) — the Postgres design behind the store (state table,
  leases, scheduling, the strains).
- `homelab.common.messaging` — the topology family the projections land in: `Worker` → `Source` /
  `Processor` → `Pipe`, plus `Producer` / `Consumer` / `Distributer` / `Router`.
- `homelab.common.flow.Workflow` — the pure `Workflow.Simple` state machine the driver steps.
