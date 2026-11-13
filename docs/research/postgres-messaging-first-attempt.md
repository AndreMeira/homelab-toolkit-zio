---
title: "The Postgres messaging adapter — a first attempt, its pieces, and why it was dropped"
type: research
status: superseded
updated: 2026-11-12
tags: [postgres, messaging, consumer, producer, outbox, skip-locked, listen-notify, attempt, lessons]
---

# The Postgres messaging adapter: a first attempt

> **Dropped, 2026-11-12, and its code removed.** This is the record of round one of
> [`postgres-messaging.md`](postgres-messaging.md), as built on 2026-11-11 and reviewed live. The design
> held up; the code broke too many of the codebase's rules to be worth adjusting, so a second attempt
> should start from this record and the rules at the end, not from the code. The review itself, step by
> step, is in [`../sessions/2026-11-11-postgres-messaging-review.md`](../sessions/2026-11-11-postgres-messaging-review.md).

## What it set out to do

A Postgres adapter for the toolkit's messaging ports, inside `homelab-postgres`
(`homelab.postgres.messaging`):
- **a table as the broker:** one table for every queue, claimed with `for update skip locked` through
  `PollConsumer`;
- **a wake:** an insert trigger's `NOTIFY`, plus a periodic tick;
- **two producers:** one that commits on its own, and a transactional outbox;
- **its own migration,** with its own Flyway history table, so its versions never meet a service's.

## The pieces

| Piece | Role |
|---|---|
| `PostgresMessaging` | `migrate(database)`: applies the adapter's migration with its own history table. `listenerConnection(source)`: an unpooled JDBC connection for the listener |
| `Outbox[A](queue, encoder)` | `emit(value): ZIO[PostgresTransaction, …]`: sends in the caller's transaction, so a rollback sends nothing. Deliberately not a `Producer`, since its `emit` needs the transaction |
| `PostgresProducer[A](database, queue, encoder)` | a `Producer` that sends each call in a transaction of its own, by running an `Outbox` inside `database.transaction` |
| `PostgresConsumer[A]` | a `Consumer`: a `PollConsumer` over the table, its payloads decoded with `common`'s byte `Codec` |
| `QueueTable` | the `PollConsumer.Source`: `claim`, `ack`, `nack` against the table |
| `QueueListener` | holds a dedicated connection in `LISTEN`, and wakes its consumer on its queue's notification or on the tick |

`MigrationConfig` gained `table` (the history table's name) and `schemas` (the schemas Flyway manages), so
the adapter could reuse `PostgresMigration` for its own migration.

## The table

```sql
create table messaging.message (
  id            bigint      generated always as identity primary key,
  queue         text        not null,
  payload       bytea       not null,               -- the codec's bytes
  enqueued_at   timestamptz not null default now(),
  visible_at    timestamptz not null default now(), -- a nack's delay: not handed out before this
  claimed_until timestamptz,                        -- the lease: null when nobody holds the message
  attempts      int         not null default 0      -- how many times it has been handed out
);

-- What a claim reads: one queue's messages, oldest visible first.
create index message_claim on messaging.message (queue, visible_at, id);

-- A queue table churns: vacuum it as it goes rather than waiting for a fifth of it to be dead.
alter table messaging.message set (autovacuum_vacuum_scale_factor = 0.0, autovacuum_vacuum_threshold = 1000);

-- Every insert wakes the queue's consumers, at commit; identical notifications in a transaction collapse.
create function messaging.notify_message() returns trigger language plpgsql as $$
begin
  perform pg_notify('messaging', new.queue);
  return null;
end;
$$;

create trigger message_notify
  after insert on messaging.message
  for each row execute function messaging.notify_message();
```

The schema (`messaging`) was created by Flyway (`schemas`), and the history table was
`messaging.flyway_schema_history`. The `headers` column the research note proposed was left out: nothing
read one.

## The queries

**The claim.** A lapsed lease is claimable again from the same `where`, so no sweep is needed:

```sql
update messaging.message
   set claimed_until = now() + make_interval(secs => $lease), attempts = attempts + 1
 where id in (select id from messaging.message
               where queue = $queue and visible_at <= now()
                 and (claimed_until is null or claimed_until < now())
               order by visible_at, id
               limit $upTo
                 for update skip locked)
returning id, payload, attempts;
```

**The fence.** The attempt count returned by the claim fences the outcome. An ack or nack lands only while
the row still carries that count, so a holder whose lease lapsed, and whose message was claimed again,
cannot delete or delay it:

```sql
delete from messaging.message where id = $id and attempts = $attempts;                       -- ack
update messaging.message set visible_at = now() + make_interval(secs => $wait), claimed_until = null
 where id = $id and attempts = $attempts;                                                       -- nack
```

## What the tests proved

`PostgresMessagingSpec`, against a Testcontainers Postgres:
- **construction opens no connection,** and the first `consume` does (a counting `DataSource`, and a
  counting connection factory). With the listener forked in `make` instead, it failed: two connections
  before any `consume`;
- **a round trip,** and the row gone once the handler succeeds;
- **the outbox:** a rolled-back transaction sends nothing, a committed one sends;
- **a nack delay** of 300 ms, with the attempt counted;
- **a lapsed lease** hands the message to a second consumer, and **the fence** keeps the first one's late
  nack from releasing it. Without the `attempts` condition, it failed;
- **the `NOTIFY` wake** reaches a consumer parked with a one-minute tick, within seconds;
- **the migration** keeps its own history table, and runs again harmlessly.

## What was learned on the way

- **`PollConsumer`'s fetcher waits for a wake after an empty claim.** So the tick is not optional: nothing
  notifies a nack whose delay has passed, or a lease that lapsed.
- **`LISTEN` needs autocommit on.** The pool turns it off, and with it off, a `LISTEN` takes effect only at
  commit.
- **pgjdbc's `getNotifications(timeout)` blocks in a socket read that `Thread.interrupt` doesn't break.**
  Waiting in slices of at most 500 ms kept shutdown short, whatever the tick.
- **The listener's connection must be dedicated and unpooled.** A pooled one waits behind transactions
  under load, and so does every reconnect. It must also bypass a transaction-pooling PgBouncer, which
  doesn't keep a `LISTEN` session.
- **A `NOTIFY` from an insert trigger is sent at commit,** and duplicates in one transaction collapse, so
  every producer wakes consumers without doing anything.
- **ZIO's `.once`** (on an effect: `effect.once`, which gives an effect that runs it at most once) starts a
  listener on the first `consume` without a flag of one's own.
- **magnumpg's array givens include a `SqlArrayCodec[Byte]`,** which makes a `bytea` payload's
  `Array[Byte]` ambiguous once imported. Binding arrays is the wrong road for this table.
- **Magnum's top-level `batchUpdate(values)(f: T => Update)`** prepares one statement and runs it as one
  JDBC batch: the way to insert, ack or nack a batch in one round trip.

## Why it was dropped: the review

The live review found too much against the codebase's rules to adjust:
- **An effect as a parameter or a field:**
  - `QueueListener` took `connect: ZIO[Scope, Throwable, Connection]` and `wake: UIO[Unit]`;
  - `PostgresConsumer` held `start: UIO[Unit]`.

  The rule is "never pass or store a lambda or an effect; only an argument that is a scope at the
  syntactic level is exempt" (DKQ `db6ae4f`, toolkit `535b080`). A `foreach` lambda was judged fine.
- **The network reached at construction,** in the first version: `make` forked the `LISTEN` loop. The rule:
  "construction allocates local state; only explicit actions reach the network"
  (`docs/research/mailbox-design.md`).
- **A pooled connection for `LISTEN`,** in the first version.
- **A nested `def`, and an anonymous `new Consumer`,** where a private method and a named class belong.
- **One statement per message** in `Outbox.emitMany`, `ack` and `nack`, instead of one batch.
- **A `throw` in a test fixture,** a fake `DataSource` that failed by throwing.
- **`QueueTable` nested** in `PostgresConsumer`'s companion, where it belonged in its own file.

## For a second attempt

- Keep the design: the table, the claim, the attempt fence, the trigger, the tick, a dedicated listener
  connection, the adapter's own migration, and `Outbox` as a sibling of `Producer`.
- Pass **data or typed components,** never effects. The listener takes the source config it opens its own
  connection from, and the thing it wakes.
- **Reach the network only in an explicit action.** Construction builds; the first `consume` (or a method
  the caller runs) starts the listener.
- **Write batches with `batchUpdate`.**
- **Check the rules before showing code.** These ones live in `CLAUDE.md`, in the design notes, and in
  commit messages.
