# homelab-auth

Service- and user-token authentication for ZIO services, by verifying JWTs against a JWKS. It implements
the [`ServiceAuthenticator`](../common/src/main/scala/homelab/common/auth/ServiceAuthenticator.scala) and
[`UserAuthenticator`](../common/src/main/scala/homelab/common/auth/UserAuthenticator.scala) ports from
`homelab-common`, so your code depends on the port and injects the implementation.

- **Fetches + caches a JWKS**, verifies signature and expiry locally, and maps the claims to a
  `Requester.Service` / `Requester.User`.
- **Two issuers out of the box:** any public JWKS endpoint, or the **in-cluster Kubernetes** service-account
  issuer (with cluster-CA TLS and the pod's own token as the bearer).
- **JDK `HttpClient`** for transport — no `zio-http`, no resource layer.
- **Algorithms:** EdDSA (Ed25519) and RS256.

## Add it

```scala
libraryDependencies += "com.andremeira.homelab" %% "homelab-auth" % "0.1.0-SNAPSHOT"
// transitively brings homelab-common (the ports, Requester, value objects, error hierarchy)
```

## Quick start

Everything hangs off a `TokenVerifier` (which owns the fetch → cache → verify stack). Build one, then wrap
it in an authenticator.

**A user, from a public JWKS:**

```scala
import homelab.auth.*
import homelab.common.types.SignedToken
import java.net.URI

for
  verifier <- TokenVerifier.from(URI.create("https://issuer.example/.well-known/jwks.json"))
  auth      = JwtUserAuthenticator(verifier)
  user     <- auth.authenticate(token)          // IO[AdapterError | UnauthorisedError, User.Authenticated]
yield user
```

`JwtUserAuthenticator` also offers `any(token: Option[SignedToken]): IO[AdapterError, User]` for
optional-auth routes: a missing or invalid token becomes `User.Anonymous`, but an infrastructure failure
still propagates.

**A calling service, from the in-cluster Kubernetes issuer:**

```scala
import homelab.auth.*

for
  verifier <- TokenVerifier.k8s(K8sJwksSource.Config())     // in-pod defaults for URI, CA, token
  auth      = JwtServiceAuthenticator(
                verifier,
                JwtServiceAuthenticator.Expectations(audience = "my-service", issuer = "https://kubernetes.default.svc"),
              )
  service  <- auth.authenticate(token)                      // IO[... , Requester.Service]
yield service
```

## `aud` / `iss` — the important part for service tokens

`JwtServiceAuthenticator` requires **`Expectations(audience, issuer)`** and rejects any token whose `aud`
doesn't include your audience or whose `iss` doesn't match. This is the check that stops a validly-signed
Kubernetes SA token minted **for another audience** from authenticating to your service — don't skip it.

`JwtUserAuthenticator` intentionally omits `aud`/`iss`: user tokens come from your own issuer, so trust is
already scoped by which JWKS the verifier draws from.

## Running in Kubernetes

`TokenVerifier.k8s` reads, from the pod's projected service-account volume:

- the **CA** (`/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`) to trust the API server's TLS, and
- the **token** (`.../token`) as the bearer for the JWKS request — read fresh each fetch, so kubelet
  rotation is transparent.

The pod's ServiceAccount needs the **`system:service-account-issuer-discovery`** ClusterRole to read the
OIDC JWKS endpoint. Defaults target `https://kubernetes.default.svc/openid/v1/jwks`; override via
`K8sJwksSource.Config` for an external issuer or tests.

## Error model

- `verify` / `authenticate` fail with **`UnauthorisedError`** for a bad token (unknown key, invalid
  signature, expired, wrong `aud`/`iss`, unmappable claims) and **`AdapterError`** when the check itself
  can't run (JWKS unreachable, unusable published key).
- `any` never rejects a bad token — it only fails with `AdapterError`.
- Startup builders (`TokenVerifier.k8s`, `K8sJwksSource.make`) fail with `CaUnreadable` if the cluster CA
  can't be read.

## Design

The stack, inside out: `JwtProvider` (token) → `JwksSource` (`HttpJwksSource` / `K8sJwksSource`) →
`CachedJwksSource` → `JwksTokenVerifier` (with a `kid → PublicKey` cache) → `JwtServiceAuthenticator` /
`JwtUserAuthenticator`. See [`../../docs`](../../docs) for the architecture notes and session checkpoints.
