---
title: "2026-11-11 — the Postgres messaging adapter, live review paused"
type: session
status: current
updated: 2026-11-11
tags: [postgres, messaging, review, consumer, listener, effects, construction]
---

# 2026-11-11 — the Postgres messaging adapter, live review paused

Round one of `docs/research/postgres-messaging.md` was implemented on branch `postgres-messaging`
(uncommitted). The user started a live review and paused it, to resume the next day: too many adjustments.

## What is on the branch

- **`homelab.postgres.messaging`,** in `homelab-postgres`:
  - `PostgresMessaging` (`migrate`, `listenerConnection`);
  - `PostgresConsumer` (a class with its companion: `Config`, `Claimed`);
  - `QueueTable`, the `PollConsumer.Source` over the table, and `QueueListener`, both package-private and
    each in its own file;
  - `PostgresProducer`;
  - `Outbox`.
- **The migration** `homelab/postgres/messaging/migrations/V1__message.sql`: the `messaging.message` table,
  the claim index, autovacuum settings, and the insert trigger that sends `NOTIFY messaging, '<queue>'`.
- **`MigrationConfig`** gains `table` and `schemas`.
- **`PostgresSpecLayers.databaseWithSource`** (defined before `database`, which is built from it).
- **`PostgresMessagingSpec`:** seven tests. Construction opens no connection; a round trip; the outbox
  commit and rollback; a nack delay; a lapsed lease with its fence; the `NOTIFY` wake; the migration's
  own history.
- **`modules/postgres/README.md`,** and a pointer from `docs/architecture/messaging.md`.

All 14 `postgres` tests passed at the pause, and scalafmt was clean.

## Decided during the review

- **No network at construction.** The listener starts with the first `consume` (`listener.run.forkIn(scope).once`).
  The rule is the messaging notes' "construction allocates local state; only explicit actions reach the
  network". Local background fibers, like `PollConsumer`'s, are allowed but best avoided.
- **No nested `def`s.** `await` became a private method.
- **The listener is its own class, `QueueListener`,** on a dedicated, unpooled connection, so it never
  waits behind the pool's transactions.
- **`make` returns a class, `PostgresConsumer[A]`,** with a private constructor, instead of an anonymous
  `new Consumer`.

## To fix: no lambdas, no effects as arguments

The rule, which the user raised at the pause: **never pass or store a lambda or an effect; only an argument
that is a scope at the syntactic level is exempt** (a block such as `ZIO.scoped: …` or
`database.transaction: …`). Name a method or write the branches. A stored or passed effect is deferred logic
with no name: the receiver can't know what it carries. It's the same reason `ZStream` is kept off the ports
(`docs/architecture/messaging.md`, "Deliberately absent").

It's written only in commit messages, which is why a search of the docs missed it at first:
- DKQ `db6ae4f` (2026-09-11), "style: no stored effects, no anonymous logic — Connection holds values",
  which states it "once more because it keeps being broken";
- toolkit `535b080` (2026-09-23): "name a method or write the branches, never pass an anonymous function".

It belongs in `CLAUDE.md`.

**Effects passed or stored,** in three places:

1. **`connect: ZIO[Scope, Throwable, Connection]`** on `QueueListener` and `PostgresConsumer.make`. A
   candidate fix is to pass the data instead: a `DatabaseSourceConfig` for the listener, from which
   `QueueListener` opens its own unpooled connection. `PostgresMessaging.listenerConnection` would then go.
2. **`wake: UIO[Unit]`** on `QueueListener`. A candidate fix is to pass the component that is woken (the
   `PollConsumer`, or a narrow type with a `wakeUp` method).
3. **`start: UIO[Unit]`** held by `PostgresConsumer`, the once-guarded listener. To redesign: the class
   could hold the `QueueListener` and start it itself on the first `consume`.

The spec's construction test builds a counting `connect` effect. It has to follow whatever replaces the
`connect` parameter.

**Lambdas passed,** in the main code, each to become a named method or written branches:
- `QueueTable.ack` and `nack`, and `Outbox.emitMany`: their `foreach: x =>` lambdas are fine, the user said:
  it is really hard to come up with a rule for those.

**One batch, not one statement per message.** `Outbox.emitMany`, `QueueTable.ack` and `QueueTable.nack`
each run one statement per message, so a batch of a hundred costs a hundred round trips. Magnum already
does this: the top-level `batchUpdate[T](values)(f: T => Update)(using DbCon)` (Magnum 1.3.1, `util.scala`)
prepares one statement from the first update's fragment, adds each value as a JDBC batch entry, and runs
`executeBatch` once:

```scala
batchUpdate(values)(value => sql"insert into messaging.message (queue, payload) values ($queue, ${encoder.encode(value)})".update)
```

Each payload binds as a plain `bytea`, so no array codec is involved, and `ack` and `nack` take the same
shape with the attempt fence unchanged. `Repo.insertAll` uses the same mechanism. With pgjdbc's
`reWriteBatchedInserts=true` the inserts are even rewritten into one multi-row insert. Building a
multi-row `VALUES` by hand, or binding arrays through magnumpg, is unnecessary: the latter would also make
the payload's `Array[Byte]` ambiguous with magnumpg's `SqlArrayCodec[Byte]`.
- `PostgresConsumer.make`: `polling.mapZIO: claimed => …` (a named `decode`);
- `QueueListener.run`: `catchAllCause(cause => …)`;
- `QueueListener.await`: `.flatMap: received =>` and `exists(_.getParameter == queue)`.

The spec has its own: `seen.succeed`, `dedicated.update(_ + 1)`, `layer.tap(environment => …)`, and the
`InvocationHandler` lambdas of its fake. These need sweeping with the same rule, or a decision on what a
test fixture may keep.

## Other open points

- **The reconnect path no longer logs** (`QueueListener.run`'s `catchAllCause(cause => wake *> …)`), after
  the user's edit. Whether to restore the warning was not answered.
- **One run of the spec failed after 249 s** and could not be reproduced: six reruns passed in 7 s each.
  The output was filtered, so the cause is unknown. The suspicion, unproven, is a slow container start.
- **`PostgresSpecLayers`** breaks the naming rule (`*SpecSupport`, not `*SpecLayers`). It predates this
  work, and was left as it is.
