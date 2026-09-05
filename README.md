# homelab-toolkit-zio

Shared **Scala 3 + ZIO 2** building blocks: ports for the things a service always needs — persistence,
messaging, authentication, observability — and adapters that implement them. `homelab-common` holds the
ports and everything that needs no third-party library; each adapter is a separate artifact, so depending
on one never drags in the others' dependencies.

Apache-2.0. Built with Scala 3.8.3 and ZIO 2.1.23.

## Install

**1. Create a classic PAT** with the `read:packages` scope and nothing else. GitHub Packages serves Maven
artifacts only to authenticated callers, public ones included, and fine-grained tokens are not supported.
One token covers every package on the account.

**2. Put it in `~/.sbt/1.0/credentials`** — never in a repo. The realm must match exactly; get it wrong and
sbt skips the credentials and you get a 401 that looks like a bad token:

```
realm=GitHub Package Registry
host=maven.pkg.github.com
user=<your-github-username>
password=<your-classic-pat>
```

**3. Add the resolver and what you need** to `build.sbt`:

```scala
resolvers += "homelab-toolkit-zio" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

libraryDependencies += "com.andremeira.homelab" %% "homelab-auth" % "<version>"
// adapters bring homelab-common transitively — you rarely need both lines
```

**Which version?** The published versions are this repo's releases without the leading `v`:

```bash
gh release list --limit 1
```

A release tagged `v1.2.3` makes that line `… %% "homelab-auth" % "1.2.3"`.

**In CI you need no token.** These packages are public, so the built-in `GITHUB_TOKEN` every Actions run is
given is enough:

```scala
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", ""),
  sys.env.getOrElse("GITHUB_TOKEN", ""),
)
```

```yaml
env:
  GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}   # built-in; no secret to create
```

**To use an unreleased change**, run `sbt publishLocal` here and depend on the snapshot version from
`build.sbt` — `~/.ivy2/local` is already on sbt's resolver chain, so no resolver and no token are needed.
Git-source and JitPack alternatives are in
[`docs/learning-material/using-modules-as-a-dependency.md`](docs/learning-material/using-modules-as-a-dependency.md).

## What to depend on

| Artifact | Use it for | Brings |
|---|---|---|
| `homelab-common` | ports, error vocabulary, in-memory implementations | ZIO only |
| `homelab-postgres` | Postgres persistence | Magnum, Hikari, Flyway |
| `homelab-nats` | NATS Core and JetStream messaging | jnats |
| `homelab-auth` | JWT/JWKS authentication, incl. the Kubernetes issuer | JDK `HttpClient` only |
| `homelab-telemetry` | OpenTelemetry spans and metrics | zio-telemetry, otel-api |

`incubator` is sketches and experiments; it is never published.

### `homelab-common`

- **`data`** — `Batch` and its map variants (partial results in the type), `Codec`.
- **`error`** — the `ApplicationError` hierarchy and `ValidationError`: one error vocabulary per service.
- **`messaging`** — `Producer` / `Consumer` ports, `Pipe`, `Hub`, `Router`, `Partitioner`; an in-memory
  family (`Wire`, `QueueProducer`/`QueueConsumer`, `Distributer`) usable in production, not just in tests;
  and `PollConsumer` for stores that never call you.
  → [`docs/architecture/messaging.md`](docs/architecture/messaging.md)
- **`processing`** — `Processor`, `Worker`, `Stateful`, `Workflow`, and the `Graph` / `Node` that start and
  supervise them; `Mailbox` for request-reply.
  → [`docs/architecture/processing.md`](docs/architecture/processing.md),
  [`docs/architecture/mailbox.md`](docs/architecture/mailbox.md)
- **`flow`** — `Batcher` (serial, deduplicated, distributed, adaptive), `KeyedQueue`, `KeyLock`, `Permit`,
  `Loop`.
- **`store`** — `KeyValueStore`, `Bucket`, `Memo`, with in-memory implementations.
- **`auth`**, **`database`**, **`monitor`** — the ports the adapter modules implement: `Requester`,
  `ServiceAuthenticator`, `UserAuthenticator`, `Database`, `Monitor`.

### `homelab-postgres`

`PostgresDatabase` (Hikari-pooled), `PostgresTransaction`, Flyway migrations, HOCON config. Queries use
**Magnum**; blocking JDBC is lifted with `ZIO.attemptBlocking`, so no cats-effect is pulled in.

### `homelab-nats`

Core (`nats.core`) for ephemeral pub/sub, JetStream (`nats.stream`) for durable delivery — each with a
single and a batched consumer — plus a `Mailbox` transport, wire codecs, and an explicit
`HandlerFailurePolicy` for ack / nak / term.

### `homelab-auth`

`ServiceAuthenticator` and `UserAuthenticator` verifying JWTs against a JWKS, with claims mapped to a
`Requester`. Two issuers: any public JWKS endpoint, or the in-cluster Kubernetes service-account issuer.
EdDSA (Ed25519) and RS256. Keys, tokens and verifications are cached.
→ [`docs/architecture/auth.md`](docs/architecture/auth.md), [`modules/auth/README.md`](modules/auth/README.md)

### `homelab-telemetry`

`OtelMonitor`, the OpenTelemetry implementation of the `Monitor` port — spans plus hit/latency/error
metrics. Your application wires the zio-telemetry `Tracing` and `Meter` layers; `OtelMonitor.make` takes
them and its scaladoc shows the wiring.

## Contributing

```bash
sbt compile
sbt test               # full suite, incl. Postgres/NATS Testcontainers integration tests
sbt common/test        # one module
sbt publishLocal       # every module to ~/.ivy2/local
```

### Releasing

Draft a GitHub Release, create the tag `v<version>` on the spot, and publish it — that fires
[`.github/workflows/release.yml`](.github/workflows/release.yml), which runs the suite and publishes the
five library modules. From the CLI:

```bash
gh release create v<version> --generate-notes
```

The workflow triggers on the **release**, not on a tag push: `git push origin v<version>` alone publishes
nothing. The tag is the version (`v1.2.3` → `1.2.3`). Published versions are immutable — fix a bad release
by releasing the next patch. If a publish fails after the release exists, re-run it from *Actions → release
→ Run workflow* with the same tag.

## Docs

[`docs/architecture/`](docs/architecture/) for how a module works today,
[`docs/learning-material/`](docs/learning-material/) for the underlying technology and its gotchas,
[`docs/research/`](docs/research/) for design rationale.
