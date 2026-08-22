# homelab-toolkit-zio

Shared **Scala 3 + ZIO** building blocks for homelab services — hexagonal at the module level: `common`
is the boundary (shared data + ports + the in-process implementations that need no dependency of their
own), and every module that drags in a third-party library is quarantined behind its own artifact. The ZIO
counterpart of [`../homelab-toolkit`](../homelab-toolkit) (Kyo).

Why ZIO, not Kyo (and when that flips): see
[`../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md`](../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md).

## Adding a dependency

Artifacts are published to this repo's **GitHub Packages** Maven registry, under organization
`com.andremeira.homelab`, cross-built for Scala 3 (use `%%` — it appends the `_3` suffix).

**1. Get a token.** GitHub Packages serves Maven artifacts only to authenticated callers — *"You need an
access token to publish, install, and delete private, internal, and public packages"* — and it must be a
**classic** PAT. Repo visibility does not change this. Create one with the **`read:packages` scope and
nothing else**; a single token covers every package on the account, so one serves the whole homelab.

**2. Put it in `~/.sbt/1.0/credentials`** (never in a repo). The realm string is fixed by GitHub; get it
wrong and sbt skips the credentials, which surfaces as a 401 that reads like a bad token:

```
realm=GitHub Package Registry
host=maven.pkg.github.com
user=<your-github-username>
password=<your-classic-pat>
```

**3. Add the resolver and the module** to your service's `build.sbt`:

```scala
resolvers += "homelab-toolkit-zio" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

libraryDependencies += "com.andremeira.homelab" %% "homelab-auth" % "0.1.0"
// every adapter transitively brings homelab-common — you rarely need both lines
```

**In CI**, add the same PAT as an Actions secret (or an org-level one, which every repo inherits) and read
the credentials from the environment instead of the file:

```scala
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", ""),
  sys.env.getOrElse("GITHUB_TOKEN", ""),
)
```

Don't use Actions' built-in `GITHUB_TOKEN` to *read* these — it needs one access grant per package per
consumer repo. The shared PAT avoids that matrix.

> **No release is cut yet.** Until the first `v*` tag exists, use `sbt publishLocal` in this repo and depend
> on `0.1.0-SNAPSHOT` — `~/.ivy2/local` is already on sbt's resolver chain, so no resolver and no token are
> needed. Full recipe, plus the git-source-dependency and JitPack alternatives:
> [`docs/learning-material/using-modules-as-a-dependency.md`](docs/learning-material/using-modules-as-a-dependency.md).

## The modules

| Module | Artifact | One line |
|---|---|---|
| `common` | `homelab-common` | data, ports, and everything that needs no third-party library |
| `postgres` | `homelab-postgres` | Postgres persistence — Magnum + Hikari + Flyway |
| `nats` | `homelab-nats` | NATS Core + JetStream behind the messaging ports |
| `auth` | `homelab-auth` | JWT/JWKS authentication, including the in-cluster Kubernetes issuer |
| `telemetry` | `homelab-telemetry` | OpenTelemetry behind the `Monitor` port |
| `incubator` | — | sketches and experiments; never published |

### `homelab-common`

The boundary every other module implements, plus the parts that are pure ZIO and so need no quarantine:

- **`data`** — `Batch` and its map variants (partial results carried in the type), `Codec`.
- **`error`** — the `ApplicationError` hierarchy and `ValidationError`, the one error vocabulary a service
  speaks.
- **`messaging`** — the `Producer` / `Consumer` ports, `Pipe`, `Hub`, `Router`, `Partitioner`; an in-memory
  family (`Wire`, `QueueProducer`/`QueueConsumer`, the keyed `Distributer`) that is a real implementation
  rather than a test double; and `PollConsumer` for stores that never call you. See
  [`docs/architecture/messaging.md`](docs/architecture/messaging.md).
- **`processing`** — what runs continuously: `Processor`, `Worker`, `Stateful`, `Workflow`, and the `Graph` /
  `Node` that start and supervise them, plus `Mailbox` for request-reply. See
  [`docs/architecture/processing.md`](docs/architecture/processing.md) and
  [`docs/architecture/mailbox.md`](docs/architecture/mailbox.md).
- **`flow`** — backpressure and rate primitives: `Batcher` (serial, deduplicated, distributed and adaptive
  strategies), `KeyedQueue`, `KeyLock`, `Permit`, `Loop`.
- **`store`** — `KeyValueStore`, `Bucket` and `Memo` ports, with in-memory implementations.
- **`auth`** / **`database`** / **`monitor`** — the ports the `auth`, `postgres` and `telemetry` modules
  implement: `Requester`, `ServiceAuthenticator`, `UserAuthenticator`, `Database`, `Monitor`.

### `homelab-postgres`

The `Database` port over Postgres: a Hikari-pooled `PostgresDatabase`, `PostgresTransaction`, Flyway
migrations (`PostgresMigration`), and HOCON configuration. Queries are written with **Magnum**, which is
effect-agnostic — blocking JDBC is lifted with `ZIO.attemptBlocking`, so no cats-effect comes along.

### `homelab-nats`

The messaging ports over NATS. **Core** (`nats.core`) for ephemeral pub/sub and **JetStream**
(`nats.stream`) for durable delivery, each with a single and a batched consumer, plus a `Mailbox` transport,
wire codecs and an explicit `HandlerFailurePolicy` (what an ack, a nak and a term mean for your handler).
`ZStream` is an internal bridging detail and never surfaces in a signature.

### `homelab-auth`

`ServiceAuthenticator` / `UserAuthenticator` implemented by verifying JWTs against a JWKS — signature and
expiry checked locally, claims mapped to a `Requester`. Two issuers: any public JWKS endpoint, or the
in-cluster **Kubernetes** service-account issuer (cluster-CA TLS, the pod's own projected token, and
`TokenReview` where it is needed). Keys, tokens and verifications are cached. EdDSA (Ed25519) and RS256, over
the JDK `HttpClient` — no `zio-http`. See [`docs/architecture/auth.md`](docs/architecture/auth.md) and
[`modules/auth/README.md`](modules/auth/README.md).

### `homelab-telemetry`

`OtelMonitor`, the OpenTelemetry implementation of `common`'s `Monitor` port (spans + metrics) via
zio-telemetry. The application wires the `Tracing`/`Meter` layers; the toolkit supplies the adapter.

### `incubator`

Throwaway sketches — successive versions of an idea kept side by side (`actor/v1..v7`, `nats/v1..v5`) until
one is promoted into a real module. `publish / skip`, and its tests compile but do not run in CI.

## Build

```bash
sbt compile
sbt test               # the full suite, incl. Postgres/NATS Testcontainers integration tests
sbt common/test        # one module
sbt publishLocal       # every module to ~/.ivy2/local
```

## Releasing

**Publish a GitHub Release** — *Releases → Draft a new release*, create the tag `v0.1.0` on the spot, and
hit *Publish release*. That fires [`.github/workflows/release.yml`](.github/workflows/release.yml), which
runs the suite and publishes the five library modules. From the CLI it's the same event:

```bash
gh release create v0.1.0 --generate-notes
```

Note the workflow triggers on the **release**, not on a tag push, because a release created in the UI emits
only the release event — `git push origin v0.1.0` alone would publish nothing.

The tag is the version (`v0.1.0` → `0.1.0`); a local build always says `0.1.0-SNAPSHOT`. Published versions
are **immutable** — fix a botched release by releasing the next patch, never by overwriting. If a publish
fails after the release exists, re-run the workflow manually (*Actions → release → Run workflow*) with the
same tag.

## Docs

[`docs/`](docs/) follows the homelab-wide taxonomy ([`../DOCS.md`](../DOCS.md)): `architecture/` for
current-state, `learning-material/` for how-it-works and gotchas, `sessions/` for dated checkpoints. Design
rationale that precedes the code lives in [`../research/library-design/`](../research/library-design/).
