---
title: "Adding modules to the sbt build"
type: learning-material
status: current
updated: 2026-07-06
tags: [sbt, multi-module, build, jitpack, modules]
---

# Adding modules to the sbt build

This toolkit is a **multi-module sbt build**: one repository, several independently-publishable libraries
(`common`, `auth`, …). This note explains how the build is wired and how to add one or more modules of your
own. Everything here is grounded in the real `build.sbt` at the repo root.

## The mental model

A multi-module build has:

- a **root project** — the umbrella. It builds nothing of its own; it *aggregates* the modules so that
  `sbt compile` / `sbt test` at the root fan out to all of them.
- one **subproject per module** — each a normal library with its own `src/`, dependencies, and artifact.

Two kinds of wiring connect them, and they are **not** the same thing:

| Wiring | Declared with | Means |
|--------|---------------|-------|
| **Aggregation** | `root.aggregate(a, b)` | "run my tasks on these too" — a *task fan-out*. No code sharing. |
| **Dependency** | `b.dependsOn(a)` | "`b`'s code can use `a`'s code" — a *classpath dependency*. |

A module you add almost always needs **both**: `aggregate` so it's part of the build, and `dependsOn(common)`
so it can use the shared ports and types.

## Anatomy of a module

Here is the `common` module, annotated:

```scala
lazy val common = project                 // `common` is the project ID → used as `sbt common/compile`
  .in(file("modules/common"))             // where its sources live → modules/common/src/main/scala/...
  .settings(
    name := "homelab-common",             // the ARTIFACT name (for publishing) — distinct from the ID
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-prelude"  % zioPreludeVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,   // `% Test` → only on the test classpath
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"), // register zio-test's runner
  )
```

Notes:

- **`lazy val`** is required. sbt project vals must be lazy so forward references between modules resolve
  regardless of declaration order.
- **Project ID vs artifact name.** The `val` name (`common`) is the ID you type in the sbt shell
  (`common/test`). `name := "homelab-common"` is the published artifact name. Keep them related but they
  needn't match.
- **Directory layout** follows sbt's convention *under* `.in(file(...))`:
  `modules/common/src/main/scala`, `.../src/test/scala`, `.../src/main/resources`.

## Adding a single module

Say you're adding a Postgres persistence adapter.

**1. Create the source directory** matching the convention:

```
modules/postgres/src/main/scala/homelab/postgres/
modules/postgres/src/test/scala/homelab/postgres/
```

**2. Declare the project** in `build.sbt`:

```scala
lazy val postgres = project
  .in(file("modules/postgres"))
  .dependsOn(common)                       // uses the shared ports/types
  .settings(
    name := "homelab-postgres",
    libraryDependencies ++= Seq(
      "com.augustnagro" %% "magnum"       % magnumVersion,
      "dev.zio"         %% "zio-test"     % zioVersion % Test,
      "dev.zio"         %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )
```

**3. Add it to the root's `aggregate` list** so it participates in the build:

```scala
lazy val root = project
  .in(file("."))
  .aggregate(common, magnum, inmemory, auth, postgres)   // ← add here
  .settings(name := "homelab-toolkit-zio", publish / skip := true)
```

**4. Reload and build:** `sbt reload` then `sbt postgres/compile`.

That's it — the `auth` module was added exactly this way when the JWKS stack was promoted out of `incubator`.

## Adding multiple modules at once

Declare each as above, then wire the dependencies **between** them with `dependsOn`. `dependsOn` takes any
number of projects:

```scala
lazy val core     = project.in(file("modules/core")).dependsOn(common)
lazy val postgres = project.in(file("modules/postgres")).dependsOn(common, core)   // depends on two
lazy val http     = project.in(file("modules/http")).dependsOn(common, core)

lazy val root = project.in(file("."))
  .aggregate(common, core, postgres, http)   // every module in the fan-out list
```

Keep the dependency graph **acyclic** — sbt rejects cycles. The arrows point *inward* toward `common`:
adapters depend on the domain, never the reverse (the same hexagonal rule the code follows).

## Shared vs per-module settings

Settings scoped to `ThisBuild` apply to **every** module unless a module overrides them. This repo sets the
Scala version and the strict compiler flags once:

```scala
ThisBuild / scalaVersion := scala3Version
ThisBuild / scalacOptions ++= Seq("-Wvalue-discard", "-Wnonunit-statement", /* ... */)
```

A module inherits these. To *add* to an inherited seq, use `+=` / `++=` inside the module's `.settings`; to
*replace* it, use `:=`. The `incubator` module opts out of the strict flags entirely:

```scala
scalacOptions := Nil   // replaces the inherited flags (experiment area — Java-interop heavy)
```

Prefer inheriting the strict flags for real modules; only relax deliberately.

## Gotchas (all hit while building this repo)

- **`dependsOn` propagates dependencies.** Depending on `common` puts `common`'s own libraries (e.g. `zio`)
  on your classpath transitively — which is why `auth` compiles without re-declaring `zio`. Still, **declare
  the libraries you use directly**; don't rely on a transitive path you could lose later.
- **Register the test framework per module.** `testFrameworks += … ZTestFramework` is a *per-project*
  setting. A module with tests but no registration will "find no tests." (`ThisBuild / testFrameworks` would
  set it globally if you prefer.)
- **Keep zio-test pinned to the zio-core version.** If `zio-test` drifts from the `zio`/`zio-prelude`/
  `zio-http` core, the layer macros fail at compile with `NoSuchMethodError: LayerMacros.constructLayer`.
  This build keeps a single `zioVersion` knob for exactly this reason.
- **After moving files or cross-module edits, a stale incremental compile can throw**
  `error while loading …: <Class>.class` or a spurious `izumi.reflect` hint. It's not your code —
  `sbt clean` (or `<module>/clean`) clears it. Verify a green build with a clean when in doubt.
- **`aggregate` ≠ `dependsOn`.** Forgetting `aggregate` means `sbt test` at the root silently skips your
  module; forgetting `dependsOn` means your code can't see `common`. You usually need both.

## Publishing via JitPack

The modules are consumable straight from GitHub without a publishing pipeline: **[JitPack](https://jitpack.io)
builds them on demand from a git tag** and serves the artifacts. As a maintainer you do two things:

1. **Add a `jitpack.yml`** at the repo root pinning a JDK new enough for the code (Ed25519 needs 15+; use 17):

   ```yaml
   jdk:
     - openjdk17
   ```

2. **Push a git tag** — that tag *is* the version:

   ```
   git tag v0.1.0 && git push origin v0.1.0
   ```

That's it. On the first request, JitPack clones the tag, runs `sbt publishM2`, and caches the result.
Consumers then reference the module under the `com.github.<owner>.<repo>` group by its artifact name — see
[`using-modules-as-a-dependency.md`](using-modules-as-a-dependency.md).

Notes:

- **`publish / skip` modules are excluded** — the `root` and `incubator` won't be published, which is what
  we want; only real modules (`common`, `auth`, …) are served.
- **sbt multi-module is JitPack's rough edge.** If the first build fails, read the log at
  `jitpack.io/com/github/<owner>/<repo>/<tag>/build.log` and expect to iterate once on the tag / `jitpack.yml`.
- **Tags are immutable coordinates.** To ship a fix, push a new tag (`v0.1.1`); don't move an existing one.

For local iteration (same machine) `sbt publishLocal` is simpler — publishing all modules to `~/.ivy2/local`;
JitPack is for handing a **pinned, cross-machine** coordinate to a consumer.

## Cheat sheet

```scala
// 1. declare
lazy val mymod = project
  .in(file("modules/mymod"))
  .dependsOn(common /*, other modules */)
  .settings(
    name := "homelab-mymod",
    libraryDependencies ++= Seq(/* deps, `% Test` for test-only */),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

// 2. aggregate under root
lazy val root = project.in(file(".")).aggregate(/* ..., */ mymod)
```

```
modules/mymod/src/main/scala/homelab/mymod/   // sources
modules/mymod/src/test/scala/homelab/mymod/   // tests
```

```
sbt reload            # pick up build.sbt changes
sbt mymod/compile     # build just this module
sbt mymod/test        # test just this module
sbt test              # test everything (via root aggregate)
sbt clean test        # when incremental compile acts up
```
