# homelab-toolkit-zio

Shared **Scala 3 + ZIO** building blocks for homelab services — hexagonal at the module level:
`common` is the boundary (shared data + ports), each adapter is its own module. The ZIO counterpart of
[`../homelab-toolkit`](../homelab-toolkit) (Kyo).

Why ZIO, not Kyo (and when that flips): see
[`../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md`](../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md).

| Module | Artifact | Holds | Status |
|---|---|---|---|
| `common` | `homelab-common` | data (`ApplicationError`, `ValidationError`, value objects, `Requester`) + ports (later) | 🟡 data only |
| `magnum` | `homelab-magnum` | persistence adapter — lib TBD (Magnum is effect-agnostic, or doobie/zio-jdbc) | ⚪ empty |
| `inmemory` | `homelab-inmemory` | in-memory adapter (shared test backend) | ⚪ empty |
| `auth` | `homelab-auth` | JWKS authenticator, argon2 hasher, EdDSA issuer | ⚪ empty |

**Status:** scaffold — only `common` **data** is populated (errors, value objects, `Requester`). Ports
and adapters come next; the DDD layout mirrors `registration-service` and `../homelab-toolkit`, only
re-typed to `ZIO[R, E, A]` + `ZLayer`. Tests will use `zio-test` (no homemade framework).

## Build

```bash
sbt compile
sbt test
```
