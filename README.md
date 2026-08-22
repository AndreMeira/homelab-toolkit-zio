# homelab-toolkit-zio

Shared **Scala 3 + ZIO** building blocks for homelab services — hexagonal at the module level:
`common` is the boundary (shared data + ports), each adapter is its own module. The ZIO counterpart of
[`../homelab-toolkit`](../homelab-toolkit) (Kyo).

Why ZIO, not Kyo (and when that flips): see
[`../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md`](../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md).

| Module | Artifact | Holds |
|---|---|---|
| `common` | `homelab-common` | data + ports, `processing`, `messaging` (incl. the in-memory family) |
| `postgres` | `homelab-postgres` | persistence adapter — Magnum + Hikari + Flyway |
| `telemetry` | `homelab-telemetry` | the OTel implementation of the `Monitor` port |
| `auth` | `homelab-auth` | JWKS authenticator, hasher, EdDSA issuer |
| `nats` | `homelab-nats` | Core NATS + JetStream implementations of the messaging ports |

**Status:** scaffold — only `common` **data** is populated (errors, value objects, `Requester`). Ports
and adapters come next; the DDD layout mirrors `registration-service` and `../homelab-toolkit`, only
re-typed to `ZIO[R, E, A]` + `ZLayer`. Tests will use `zio-test` (no homemade framework).

## Build

```bash
sbt compile
sbt test
```

## Releasing and consuming

Artifacts are published to this repo's **GitHub Packages** Maven registry by pushing a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Consuming a module needs a resolver *and* a classic PAT with `read:packages` — GitHub Packages never serves
Maven anonymously, public repo or not. The recipe (plus `publishLocal` for the day-to-day loop) is in
[`docs/learning-material/using-modules-as-a-dependency.md`](docs/learning-material/using-modules-as-a-dependency.md).
