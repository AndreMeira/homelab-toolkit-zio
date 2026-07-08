---
title: "Depending on a toolkit module from your service"
type: learning-material
status: current
updated: 2026-07-06
tags: [sbt, dependencies, jitpack, publishing, modules]
---

# Depending on a toolkit module from your service

This repo is a set of small libraries (`homelab-common`, `homelab-auth`, …). This note is for the **other
side**: a ZIO service that wants to pull one of them in as an sbt dependency.

## What you can depend on

| Module | Artifact | Provides |
|--------|----------|----------|
| common | `homelab-common` | ports, `Requester`, value objects, the error hierarchy |
| auth   | `homelab-auth`   | JWT/JWKS service & user authentication (depends on `common`) |

Coordinates: organization **`com.andremeira.homelab`**, version **`0.1.0-SNAPSHOT`**, cross-built for
**Scala 3** (use `%%`). `magnum`/`inmemory` aren't implemented yet; `incubator` is never published.

Depending on `homelab-auth` **transitively brings `homelab-common`** (and its libraries), so you rarely
need both lines.

## It isn't on Maven Central — so pick a method

### 1. Local publish — simplest when you control both repos

In the **toolkit** repo:

```
sbt publishLocal          # publishes every module to ~/.ivy2/local
# …or just one:
sbt auth/publishLocal
```

In your **service**'s `build.sbt`:

```scala
libraryDependencies += "com.andremeira.homelab" %% "homelab-auth" % "0.1.0-SNAPSHOT"
```

No resolver needed — `~/.ivy2/local` is already on sbt's default chain. (The root and `incubator` are
`publish / skip`, so they're deliberately not published.) Re-run `publishLocal` after each toolkit change;
if your service caches the SNAPSHOT, `sbt update` or a fresh reload picks up the new build.

### 2. Git source dependency — no publishing at all

Reference the toolkit's subproject straight from GitHub in your service's `build.sbt`:

```scala
lazy val toolkitAuth =
  ProjectRef(uri("https://github.com/<owner>/homelab-toolkit-zio.git#<ref>"), "auth")

lazy val myService = (project in file("."))
  .dependsOn(toolkitAuth)     // homelab-common comes with it
```

`<ref>` is a tag, branch, or commit SHA; the project ID (`"auth"`) is the `lazy val` name from the toolkit's
`build.sbt`. sbt clones and **builds the toolkit from source** alongside your service — nothing to publish,
and you can pin an exact commit while iterating.

### 3. JitPack — a real binary coordinate from a GitHub tag

```scala
resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.<owner>.homelab-toolkit-zio" %% "homelab-auth" % "<tag>"
```

JitPack builds the tag on first request and serves the artifacts. For a multi-module sbt build it publishes
each subproject under the `com.github.<owner>.<repo>` group; reference the one you want by its artifact
name. You'll likely need a `jitpack.yml` pinning the JDK, and a **pushed git tag** to build from.

### 4. GitHub Packages — if you want authenticated hosted artifacts

Set `publishTo` to your repo's GitHub Packages Maven registry and publish with a token; consumers add the
same registry as a resolver with credentials. More setup than the above — reach for it only if you want
hosted, access-controlled artifacts.

## Things to know

- **`%%` vs `%`.** `%%` appends the Scala 3 binary suffix (`_3`); your service must be on a compatible
  Scala 3 (the toolkit builds with **3.3.4**, the 3.3.x LTS line). Use plain `%` only for Java libraries.
- **SNAPSHOTs move.** `0.1.0-SNAPSHOT` can change under you. For a reproducible build, pin a **tag** via
  method 2 or 3 rather than tracking the SNAPSHOT.
- **You depend on the port, not the class.** `homelab-auth` implements `homelab-common`'s
  `ServiceAuthenticator` / `UserAuthenticator`. Wire your code against the port and inject the
  implementation — see [`modules/auth/README.md`](../../modules/auth/README.md) for usage.

## Recommendation

Controlling both repos in a homelab: **`publishLocal`** for day-to-day work, and a **git source dependency
or a JitPack tag** when you want a pinned, reproducible reference (CI, another machine, a teammate).
