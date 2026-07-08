---
title: "Checkpoint — 2026-07-06 — database library evaluation + Magnum `Database[Tx]` sketch"
type: session
status: current
updated: 2026-07-06
tags: [database, magnum, doobie, persistence, checkpoint]
---

# Checkpoint — 2026-07-06 — database library evaluation + Magnum `Database[Tx]` sketch

> Second session today (after the auth promotion). Evaluated **Magnum vs Doobie** for persistence and
> prototyped a Magnum-backed `Database[Tx]` adapter in the playground. Also added contributor/consumer docs
> and a scalafmt spacing rule. No decision committed yet; the sketch is not promoted.

## Database library decision (in progress — leaning Magnum)

- **Both are low-effort** to port (the user has a proven doobie→ZIO adapter in `zio-conduit-doobie` and a
  1:1 Magnum port in `registration-service`). So effort isn't the tiebreaker.
- **Doobie's cost is cats-effect + `zio-interop-cats`** in the dependency tree of every consuming service —
  a real smell for a *pure-ZIO toolkit*. Magnum is effect-agnostic: blocking JDBC lifted with
  `ZIO.attemptBlocking`, no foreign effect system.
- **The `Database[Tx]` port already matches the doobie/Kyo model** (Tx threaded through the environment),
  but Magnum bridges cleanly via a `DbTx`-in-`Tx` wrapper.
- **The "Magnum has no SQL fragments" concern is resolved** — the user already wrote a small `Frag`
  concat lib (`kyo-conduit-postgres/.../MagnumInterop.scala`: `combine` + `and`/`or`/`comma`/`++`/`when`/
  `emptyFragment`). It correctly folds the `FragWriter` to thread parameter positions. **Bug there:**
  `concat(separator)(right)` ignores `separator` and hardcodes `" "`.
- **Evidence it's enough:** every query in `registration-service` is a static parameterised `sql"..."` —
  no dynamic assembly needed. Magnum + the concat lib covers the common dynamic-WHERE case; doobie stays a
  per-service escape hatch (the port abstracts the backend) if heavy programmatic SQL ever shows up.

**Recommendation:** Magnum for the toolkit. Not yet committed.

## Magnum sketch (playground — `incubator/db/`, not promoted)

Compiles; translates the Kyo `PostgresDatabase` to ZIO nearly line-for-line:

- **`MagnumInterop`** (in `com.augustnagro.magnum`) — `makeDbTx(connection)` shim reaching `DbTx`'s
  package-private constructor. *(This is the "own impl" the user was half-remembering — a DbTx shim, not
  the fragment lib, which is separate and in `kyo-conduit-postgres`.)*
- **`PostgresTransaction`** — wraps a `Connection`; `dbTx`, `commit`/`rollback`, and `Transactional` /
  `Transactional.withTime` helpers (`DbTx ?=> A` blocks lifted with `attemptBlocking`).
- **`PostgresDatabase`** (was `MagnumDatabase`) — `Database[PostgresTransaction]` via
  `ZIO.acquireReleaseExitWith`. **Finalization logic settled after two rounds:** commit runs in the *body*
  (`effect <* tx.commit`) so a failed commit surfaces as a typed `AdapterError`; the exit-aware release
  rolls back on failure and **always** closes. Lesson: `tx.commit *> close` leaks the connection on commit
  failure (`*>` short-circuits `close`) — the release must guarantee `close` (`rollback.ignore *> close`).
- **`PostgresUserRepository`** — sample repo, raw `sql` inside `Transactional`, same style as
  registration-service.

Added `com.augustnagro %% magnum % 1.3.1` to the `incubator` deps (sketch only).

## Docs + tooling added this session

- **`docs/learning-material/sbt-multi-module-builds.md`** — contributor guide: aggregate vs dependsOn,
  adding modules, gotchas, + a **Publishing via JitPack** section.
- **`docs/learning-material/using-modules-as-a-dependency.md`** — consumer guide: publishLocal / git source
  dep / JitPack / GitHub Packages.
- **`modules/auth/README.md`** — library-consumer README for the auth module.
- **`.scalafmt.conf`** — `newlines.topLevelStatementBlankLines = [{ blanks = 2, maxNest = 0 }]`; ran
  `scalafmtAll` repo-wide (2 blank lines around top-level statements).

## What's next

1. **Commit to Magnum (or not).** If yes: promote the sketch to the real `magnum` module — proper package,
   the `MagnumInterop` shim + the **fixed** concat lib, a `Transactor`/`DataSource` (Hikari) layer, and
   migrations. Then a Testcontainers spec.
2. Trim the `PostgresDatabase.transaction` `@return` doc — rollback failures are swallowed, so only
   *connection* and *commit* failures surface as `AdapterError` (drop "rollback").
3. Still pending from the auth session: prune `incubator` v1/v2 (drop `zio-http`), and the `auth` coverage
   gaps (`CachedTokenProvider` w/ TestClock, `CachedJwksSource`, the HTTP sources).
