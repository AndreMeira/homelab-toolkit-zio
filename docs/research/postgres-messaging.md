---
title: "A Postgres messaging adapter — a work queue, a transactional outbox, and later the WAL (pre-research)"
type: research
status: draft
updated: 2026-11-11
tags: [postgres, messaging, consumer, producer, outbox, skip-locked, listen-notify, wal, logical-decoding, cdc, pre-research, not-a-decision]
---

# A Postgres messaging adapter

> **Pre-research.** It states what a Postgres adapter for the messaging ports would be, the questions it
> raises, and a later stage that streams the outbox from the WAL. Nothing is built, and the first round
> does not include the WAL. Facts about Postgres internals marked *(to verify)* are from memory.

## Why Postgres

The toolkit's broker adapter today is NATS. Postgres would add three things:

1. **A producer that commits with the domain write.** An event emitted in the same transaction as the row
   it describes (`UserRegistered` with the user insert) is committed or rolled back with it. There is no
   window, as there is with "write the row, then publish to NATS", where a crash leaves one without the
   other. This is the transactional outbox, and it's the reason to build this at all.
2. **Nothing new to run.** Postgres is already shared infrastructure, and `homelab-postgres` already has
   the transaction (`PostgresDatabase.transaction`, `PostgresTransaction`) and the migrations.
3. **A consumer that's mostly written already.** `PollConsumer` was designed with exactly this as its
   motivating case: "rows claimed with `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`, a `claimed_until` column
   carrying the lease, a `visible_at` column carrying the retry delay". The adapter supplies its `Source`
   (`claim`, `ack`, `nack`) and a wake.

It is not a NATS replacement: claims cost milliseconds, and the table needs vacuum tuning under churn.
At homelab volumes neither matters.

## Two roles, which this note keeps apart

- **Postgres as the broker** (round one). Messages live in a table, and consumers claim them from it. The
  producer inserts, in the caller's transaction or in a transaction of its own.
- **Postgres as the source, another broker as the destination** (a later round). Messages are written in
  the caller's transaction, then **relayed** to NATS (or anywhere a `Producer` goes) in commit order. The
  WAL is the natural way to read them out. This is the "Kafka connector" role, what Debezium's outbox
  pattern does.

The first is a work queue; the second is a stream relay. They want different things from Postgres, below.

## Round one: the table

A first shape, to be challenged:

```sql
create table messaging.message (
  id            bigint generated always as identity primary key,
  queue         text        not null,
  payload       bytea       not null,         -- the codec's bytes
  headers       jsonb       not null default '{}',
  enqueued_at   timestamptz not null default now(),
  visible_at    timestamptz not null default now(),  -- a nack's delay
  claimed_until timestamptz,                         -- the lease, null when nobody holds it
  attempts      int         not null default 0
);
create index message_claimable on messaging.message (queue, visible_at) where claimed_until is null;
```

**The claim** is one statement, and `PollConsumer` batches it across callers:

```sql
update messaging.message set claimed_until = now() + $lease, attempts = attempts + 1
 where id in (select id from messaging.message
               where queue = $1 and visible_at <= now()
                 and (claimed_until is null or claimed_until < now())  -- a lapsed lease is claimable again
               order by visible_at, id
               limit $2 for update skip locked)
returning id, payload, headers, attempts;
```

- **An ack** deletes the rows.
- **A nack** sets `visible_at = now() + wait`, `claimed_until = null`. An interrupted `logic` is nacked
  with no wait, following the `Consumer` rule that interruption is not a failure.
- **A lapsed lease** needs no sweep: the claim's own `where` picks it up. That's the same simplification as
  the lock API's inline reclaim.

### The wake

`NOTIFY messaging, '<queue>'` in the inserting transaction, and one `LISTEN messaging` connection per
instance, which raises `PollConsumer`'s `wakeUp`. DKQ's
`postgres-substrate.md` (in `distributed-keyed-queue/docs/research/`) worked
through the properties, and they carry over:
- **It fires on commit,** so a woken consumer can't arrive before the row it was told about is visible.
- **It's fire-and-forget.** A listener that was away missed it. The poll stays the source of truth, which
  `PollConsumer` already assumes.
- **Identical notifications in one transaction collapse into one,** so a burst of inserts to one queue wakes
  once.
- **The trap: a transaction-pooling PgBouncer breaks `LISTEN`.** The listener's connection must go straight
  to Postgres, or through a session-mode pool. It's the most likely way to lose wakes in production, and
  it's a deployment mistake rather than a code one.

### The producer, and the question it raises

The toolkit's `Producer[E, A]` has `emit(value): IO[E, Unit]`, with no environment. A producer that
inserts in its own transaction fits that. **The outbox producer doesn't**: it must run in the caller's
`PostgresTransaction`, so its `emit` is a `ZIO[PostgresTransaction, E, Unit]`. Options:
- **A sibling type** (`Outbox[A]`) with
  `emit(value): ZIO[PostgresTransaction, E, Unit]`. It's honest about the requirement and can't be passed
  where a plain producer is expected.
- **A producer built per transaction** (`outbox.within(tx): Producer[E, A]`), which fits every combinator,
  but each call site has to remember to build one inside the transaction.
- **Both:** the sibling type, and a self-committing `Producer` for callers with no transaction of their
  own.

**Decided (2026-11-11): a sibling type, `Outbox`.** `emit` requires the caller's `PostgresTransaction`, so
the type says what it needs and can't be mistaken for a producer that commits on its own.

### Questions for round one

- **One table, or one per queue?** One table needs one migration and one index, with the queue name as the
  partition. A table per queue isolates vacuum and contention but needs a migration per queue.
- **Where the migration lives.** It should ship with the adapter if possible, which raises how a service
  adds it and how it meets the service's own migrations. The toolkit configures Flyway with
  `MigrationConfig.locations`, every location sharing one history table. Three ways:
  1. **An extra location in the service's Flyway.** Flyway merges every location into one version
     sequence: the adapter's `V1` collides with the service's, and an adapter release adding `V3` after the
     service reached `V12` fails validation unless `outOfOrder` is on. Fragile.
  2. **The adapter's own Flyway, with its own history table.**
     - **How it works:** the adapter migrates its own schema (`messaging`) with its own history table
       (`messaging.flyway_schema_history`), independently of the service's sequence.
     - **What the service adds:** no migration path, only one call at startup (a `migrate` or a layer).
     - **What it avoids:** the two sequences never meet, and an upgrade brings its own migrations.
     - **What it needs:** `MigrationConfig` gains `table` and `schemas`, both plain Flyway settings, so the
       adapter reuses `PostgresMigration`.
     - **Ordering:** the adapter's migration runs before the service's, so a service migration that refers
       to the messaging table finds it.
  3. **SQL for the service to copy** into its own migrations. Full control, but it drifts as soon as the
     adapter changes its table.

  **Decided (2026-11-11): option 2.** The adapter ships its migration and runs it with its own Flyway and
  history table. The module's README explains how a service wires it in: the one call at startup, running
  it before the service's own migrations, and the schema and history table it creates.
- **Payload size:** a large payload makes every claim, ack and update rewrite a wide row. DKQ's note
  suggests a side table for large payloads.
- **Vacuum settings** for a table that churns: a per-table `autovacuum_vacuum_scale_factor` near zero.
- **Ordering:** none promised beyond "roughly oldest first", as with NATS JetStream's pull consumers.
  Per-key order (DKQ's territory) is out of scope.

## A later round: streaming the outbox from the WAL

> **To discuss** (2026-11-11), after round one is settled.

### Why the WAL, and not polling the table

A relay that polls the outbox table by `id` has an ordering trap. **`id`s are assigned at insert, not at
commit.** A transaction that took `id` 41 can commit after the one that took 42, so a relay that has
already moved past 42 never sees 41. Polling relays work around it with lag windows, or by claiming and
deleting with `SKIP LOCKED`, which turns the relay back into a work queue.

The WAL has no such gap. **Logical decoding replays committed transactions in commit order** (by LSN), and
each transaction's changes arrive together. That is exactly the order a relay wants, and why
change-data-capture tools read the WAL rather than the table.

### What it takes *(to verify)*

- **`wal_level = logical`** on the server. Changing it needs a restart, and it's cluster-wide.
- **A logical replication slot** with an output plugin. `pgoutput` is the built-in one, the same plugin
  logical replication uses. `wal2json` is an alternative that has to be installed.
- **A publication** for the outbox table, inserts only: `create publication outbox for table
  messaging.outbox with (publish = 'insert')`.
- **A client speaking the replication protocol.** pgjdbc has one: `PGConnection.getReplicationAPI()`
  opens a `PGReplicationStream` on the slot. The relay reads it, publishes each message, and confirms the
  LSN it has published up to (`setFlushedLSN`). There's no need for Kafka Connect or Debezium: this would
  be a toolkit relay from the WAL to a `Producer`.
- **The `REPLICATION` attribute** on the relay's role.

### An outbox with no table

`pg_logical_emit_message(transactional => true, prefix => 'outbox', content => …)` writes a message
straight into the WAL, as part of the current transaction. It's decoded at commit with the rest of that
transaction, or never if it rolls back. `pgoutput` delivers these when the slot is opened with
`messages 'true'` (Postgres 14 and later).

That gives an outbox with **no table at all**: no rows to delete, no vacuum churn, and no publication.
The domain write and the event commit together, and the relay reads the event in commit order. It's the
most interesting thing here, and the first thing to verify.

### What it costs *(to verify)*

- **A slot holds WAL until it's confirmed.** If the relay is down, the WAL grows on the server's disk.
  `max_slot_wal_keep_size` (Postgres 13+) caps it, by invalidating the slot, which then loses the events
  it held. So a relay that's down too long either fills the disk or loses events, and it needs a
  dashboard and an alert either way.
- **One reader per slot.** A slot is a stream with a single consumer, not a queue with competing ones. The
  relay is a singleton per slot (a lock from DKQ, or a lease, would elect it), and it should be fast:
  publishing is all it does.
- **Failover:** before Postgres 17, a logical slot isn't carried over to a promoted standby. 17 adds
  failover slots.
- **At-least-once:** after a restart, the slot replays from the last confirmed LSN, so what was published
  but not confirmed is published again. The destination needs idempotence, for example a NATS JetStream
  message id.
- **Operational weight:** server configuration, a role with `REPLICATION`, and a slot to watch. That's
  more than round one asks for, which is why it comes later.

## Order of work

1. **Round one, the work queue:**
   - the table, shipped as the adapter's own migration, with `table` and `schemas` added to
     `MigrationConfig`;
   - the `Source` for `PollConsumer`, and the `LISTEN` wake;
   - the self-committing producer;
   - the module's README: wiring the migration in, and the listener's connection, which must bypass a
     transaction-pooling PgBouncer;
   - tests against a Testcontainers Postgres, the way the module's other specs run.
2. **The outbox producer:** `Outbox`, the sibling type decided above.
3. **The WAL relay, in a later round:**
   - verify the facts marked above, starting with `pg_logical_emit_message` through `pgoutput`;
   - prototype a relay from the WAL to a NATS `Producer`;
   - measure what a down relay costs in WAL.

## Related

- `distributed-keyed-queue/docs/research/postgres-substrate.md`: the
  same building blocks (`SKIP LOCKED` claims, `LISTEN`/`NOTIFY`, vacuum) for a keyed queue.
- [`workflow-executor-impl.md`](workflow-executor-impl.md): `SKIP LOCKED` leases for workflow steps.
- `PollConsumer` (`homelab.common.messaging`): the consumer this adapter feeds.
