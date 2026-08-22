---
title: "A gRPC workload module for the ZIO toolkit — build integration and proto ownership"
type: research
status: draft
updated: 2026-08-22
tags: [grpc, zio-grpc, scalapb, sbt-protoc, proto, packaging, homelab-toolkit-zio, build]
---

# A gRPC workload module for the ZIO toolkit — build integration and proto ownership

> **Everything shown here is illustration, not reference.** The envelope fields, the `serve` signatures and
> the handler registry below exist only to make the *build and packaging* questions concrete enough to
> answer. None of them is a design decision, and a proper design pass on the API — the port shape, the
> routing model, streaming, the error contract — is still owed and should not be skipped by treating these
> sketches as a starting point. What this note aims to settle is narrower: **where protos live, how they are
> built, and what a consumer of the library actually inherits.**

The idea being explored: a `WorkloadServer.serve(dispatch)` in the toolkit, shaped like
`HttpServer.serve(routes)` but for workload handling, where a service author writes a handler and is
unaware that gRPC is underneath.

Companions: [`research/service-contracts/schema-contract-repo-design.md`](../../../research/service-contracts/schema-contract-repo-design.md)
(the proto-as-source decision and the `wire`/`domain`/`zio-grpc` module split), and the toolkit's own
`docs/architecture/messaging.md` (the ports this would sit beside).

## 1. The distinction that decides everything: contract protos vs protocol protos

Two kinds of `.proto` exist in this homelab, and conflating them is the failure mode this note is written
to prevent.

| | **Contract proto** | **Protocol proto** |
|---|---|---|
| Example | `ingredient_service.proto` — a service's API | an `Envelope` carrying an opaque payload |
| Describes | *what a service offers* | *how the library moves bytes* |
| Changes when | a service's API changes | the library changes |
| Consumers | other services, the SPA (via buf → TS) | only the library's own adapter |
| Owner | `homelab-schemas` | the toolkit module that implements it |

The toolkit must **never** hold a contract proto: it would invert the dependency (a shared library carrying
service APIs), force a toolkit release for every API change, and drop those protos out of buf's TS pipeline,
which is where the SPA gets its types.

A protocol proto is the opposite case. It is to `homelab-grpc` what the NATS wire format is to the NATS
client: part of the library's implementation, versioned with the library. That one can live in the module.

The corollary is a constraint on the design, not just on the build: **if the envelope is library-specific,
its payload must be opaque** (`bytes` plus enough metadata to decode it). A toolkit proto that named domain
types would be a contract proto wearing a disguise.

## 2. Where the protos live

```
homelab-toolkit-zio/
  modules/grpc/
    src/main/protobuf/homelab/toolkit/v1/workload.proto   ← the module's own protocol
    src/main/scala/homelab/grpc/…                          ← adapter: lifecycle, interceptors, mapping
  project/plugins.sbt                                      ← sbt-protoc + codegen (build-level)
```

`PB.protoSources` stays at its default (`src/main/protobuf`), which is the main difference from
`homelab-schemas`, where the protos sit at the repo root because two modules generate from different
directories and buf reads them too.

**Namespace discipline:** `homelab.toolkit.v1`, disjoint from anything in `homelab-schemas`. If the same
proto were ever generated into two jars, the duplicate FQNs would surface as a classpath conflict at
runtime, not at compile time.

## 3. Build wiring

Proven already in `homelab-schemas` — this is that block, minus the two-directory arrangement:

```scala
// project/plugins.sbt — build-level: affects the whole build, not just this module
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17"
libraryDependencies += "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-codegen" % "0.6.3"

// build.sbt
lazy val grpc = project
  .in(file("modules/grpc"))
  .dependsOn(common)
  .settings(
    name := "homelab-grpc",
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true)          -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb",
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb"          %% "scalapb-runtime-grpc" % scalapbVersion,
      "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core"        % zioGrpcVersion,
      "io.grpc"                        % "grpc-netty"           % grpcVersion,
    ),
    scalacOptions += "-Wconf:src=.*src_managed.*:silent",
  )
```

Four things this repo will hit that `homelab-schemas` did not:

- **The strict flags.** `-Wvalue-discard`, `-Wnonunit-statement` and the `-Wconf:…:error` line are set at
  `ThisBuild` level here. Generated ScalaPB code will not survive them, hence silencing `src_managed`.
  Note this is a *scoped* exemption, unlike `incubator`'s blanket `scalacOptions := Nil`.
- **`grpc-netty` lands in the toolkit**, which is exactly what the schemas design asked for: *"Server
  transport (`grpc-netty`) belongs in the service repos, not this shared jar."* The toolkit is the better
  home than each service, and it satisfies the module rule in `CLAUDE.md` — a module exists to quarantine a
  third-party dependency.
- **`protoc` is fetched on a clean build.** CI already has network; worth knowing it is now on the critical
  path of `sbt compile`, not just `sbt test`.
- **The package-remap option** (`option (scalapb.options)`) needs
  `"com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf"` to unpack `scalapb.proto` onto
  protoc's include path, as `homelab-schemas` does. Only needed if the remap is used.

## 4. What this means for a user of the library

The point of the exercise. A service author adds one dependency:

```scala
libraryDependencies += "com.andremeira.homelab" %% "homelab-grpc" % "<version>"
```

**What they write** is a handler and a codec-able type. No `.proto` for the workload path, no `ServerBuilder`,
no `StatusException`, no channel lifecycle.

**What they do not write, but do inherit:**

- `grpc-netty`, `zio-grpc-core`, `scalapb-runtime(-grpc)` on the classpath, transitively. This is a real
  dependency footprint — netty in particular — and it arrives whether or not the service also talks HTTP.
- The generated `Envelope` classes are *in the jar* and importable. They are not part of the intended API
  surface, but Scala has no way to hide them; the boundary is convention, and the module's Scaladoc has to
  say so.
- A port to listen on, TLS material, and whatever the config case class ends up requiring — invisible in the
  handler, unavoidable in deployment.

**What leaks regardless of API design**, and should be stated in the module's docs rather than discovered:

- **Deadlines and cancellation.** A client deadline expiring surfaces as fiber interruption in the handler.
  That is the correct mapping, but it means handler code has to be interruption-safe.
- **Message size limits.** gRPC's default 4 MB cap applies to the envelope; a large payload fails at the
  transport, not in the handler.
- **The envelope is a wire contract between library versions.** Two services on different `homelab-grpc`
  versions must interoperate, so field numbers are permanent — the same discipline as a schemas proto, but
  **without buf's breaking-change check**, which only covers `homelab-schemas`. Either add a `buf.yaml` to
  the toolkit for lint/breaking only, or accept that the discipline is manual.

## 5. The alternative worth recording: put the envelope in `homelab-schemas`

Not obviously wrong, and it should be weighed before the in-module choice is treated as settled.

**For:** every proto in the homelab goes through one pipeline — buf lint, buf breaking-change checks, TS
generation. The envelope becomes reachable by a non-Scala peer, which matters the moment anything other than
a ZIO service wants to call a workload server (a Python job, the SPA, another language the homelab picks up
for learning).

**Against:** it makes the toolkit depend on `homelab-schemas-wire`, coupling a library release to a contract
release and pointing the dependency arrow the opposite way from every other toolkit module. It also blurs
the very distinction §1 draws: `homelab-schemas` would then hold both contracts and one library's internal
protocol.

**Middle option:** keep it in the module, and add a toolkit-local `buf.yaml` for lint/breaking only (no
generation). Recovers the safety net without the dependency. This is probably where the design lands, but it
is not decided here.

## 6. Open questions for the design pass

Build-adjacent, and each one changes what gets generated or shipped:

1. **Does the module serve typed contracts too?** A second entry point taking a generated `ZBindableService`
   from `homelab-schemas` would own no proto at all and give services the netty/interceptor/mapping
   lifecycle for their real APIs. That is arguably the more valuable half, and it can ship first.
2. **Is the client symmetric with the server** — i.e. does the module hand back the same port type the
   server consumes, so that an in-memory implementation makes tests transport-free?
3. **Unary only?** `messaging.md` states "no streams in the ports". Streaming would need either a separate
   port or a deliberate exception, and it changes the proto.
4. **Does the payload discriminator drive routing**, or is it a check on a single sealed ADT decoded by one
   codec? The first requires a handler registry in the library; the second does not.
5. **Which codec** encodes the payload, and is the choice fixed or carried on the wire? Fixing it now is a
   one-way door.
6. **Health, reflection, and metrics endpoints** — standard gRPC services the module could expose. They are
   protocol, not contract, so they would live here too, and they affect the generated surface.

## 7. Provisional sketches — illustration only

Repeating the banner: these exist to make §4 concrete. They are not a proposed API.

```proto
// modules/grpc/src/main/protobuf/homelab/toolkit/v1/workload.proto — ILLUSTRATIVE
package homelab.toolkit.v1;

message Envelope {
  string                    key          = 1;  // per-key ordering server-side; empty = unordered
  string                    message_id   = 2;  // idempotency + correlation
  string                    payload_type = 3;  // stable schema name + version, never a Scala class name
  Encoding                  encoding     = 4;  // how `payload` is serialised
  google.protobuf.Timestamp sent_at      = 5;  // sender clock: metrics, not decisions
  bytes                     payload      = 6;
}

service Workload {
  rpc Dispatch(Envelope) returns (Envelope);
}
```

```scala
// ILLUSTRATIVE — what a service author might write
WorkloadServer.serve(config)(
  Handlers
    .add[ImportRequested, ImportAccepted](importRecipe)
    .add[RetryRequested,  Acknowledged  ](retry)
)
```

Three things the sketches are meant to expose, all of which survive whatever the real API turns out to be:
trace context and auth belong in gRPC **metadata** rather than the envelope (an interceptor turns them into
`Monitor` spans and a `Requester`); failures are a gRPC `Status` mapped from `ApplicationError`, never a
status field inside a successful response; and a field like `key` is inert unless the server side genuinely
uses it — `flow.KeyLock` or the keyed `Distributer` would be what makes it real.
