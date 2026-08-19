---
title: Auth — verifying tokens, and what a verified token means
type: architecture
status: current
updated: 2026-08-16
tags: [auth, jwt, jwks, kubernetes, tokenreview, service-account, caching]
---

# Auth

`homelab.auth` — turning a bearer token into a principal. 15 files, ~1,265 lines, and every one of them is
either a *verifier*, a *source of keys*, a *provider of our own credential*, or a cache over one of those.

The ports it implements live in `common`: `homelab.common.auth.{ServiceAuthenticator, UserAuthenticator,
Requester}`. This module is the adapter side.

Code: `modules/auth/src/main/scala/homelab/auth/`
Specs: `CachedTokenVerifierSpec`, `JwksTokenVerifierSpec`, `JwtServiceAuthenticatorSpec`,
`JwtUserAuthenticatorSpec`, `ProjectedTokenProviderSpec`

## The one distinction to hold on to

There are **two directions** of token here, and half the module makes sense only once they're separated:

| | inbound | outbound |
|---|---|---|
| whose token | someone else's, presented to us | ours, presented to someone else |
| question | is it valid, and who is it? | give me a current one |
| port | `TokenVerifier` | `JwtProvider` |
| implementations | `JwksTokenVerifier`, `K8sTokenReviewer` | `ProjectedTokenProvider` |
| cache | `CachedTokenVerifier`, `CachedJwksSource` | `CachedTokenProvider` |

`CachedTokenProvider` decodes `exp` **without verifying the signature**, which looks alarming until you see
it's on the outbound side: that's *our own* credential, not something we're authenticating.

## Inbound: two ways to verify, and the trade between them

```scala
TokenVerifier.from(uri)                 // any public JWKS
TokenVerifier.k8s(audience, ttl)        // Kubernetes TokenReview — authoritative
TokenVerifier.k8sOffline(config)        // Kubernetes JWKS — offline
```

Both Kubernetes paths verify a service-account token; they differ on one axis, and the doc on each says so:

- **`k8s` asks the API server** (TokenReview) on every uncached call. A round-trip, and it needs the
  `system:auth-delegator` ClusterRole — but it is **authoritative**, because it honours *revocation*: a bound
  token whose pod or Secret was deleted is rejected. Offline signature checking cannot see that.
- **`k8sOffline` fetches the cluster's JWKS** and verifies signatures itself. No round-trip, no RBAC — and no
  revocation: a token is good until its own `exp`.

`CachedTokenVerifier` exists chiefly to front the first one, and its `ttl` is exactly the freshness dial: a
revoked token stays accepted until its entry goes stale. Keep it short (the apiserver caches webhook reviews
~2 minutes itself); a long `ttl` means you may as well have used `k8sOffline`. **Only successes are cached** —
an `AdapterError` is transient and always retried, and no entry outlives the token's own `exp`.

## The JWKS chain

```
JwksTokenVerifier ──▶ CachedJwksSource ──▶ HttpJwksSource ──▶ (public issuer)
                                            └─ K8sJwksSource ──▶ apiserver OIDC endpoint
                                                                  bearer: ProjectedTokenProvider
```

- **`JwksTokenVerifier`** reads the token's `kid`, resolves the JWK, rebuilds the public key
  (`PublicKeyDecoder`), and checks signature + expiry. It does **no** `aud`/`iss` check and no principal
  mapping — deliberately, that is the authenticator's job. Supported algorithms are **EdDSA** (our own
  registration issuer) and **RS256** (Kubernetes SA tokens); the resolved key type decides which applies.
  Reconstructed keys are cached by `kid`, so `KeyFactory` runs once per signing key rather than per request.
- **`CachedJwksSource`** holds the fetched key set and refetches on two triggers: empty cache, or **a `kid` it
  doesn't know**. The second one is rotation handling — a freshly rotated signing key first appears as an
  unknown `kid`, which forces a refetch.
- **`HttpJwksSource`** is an abstract base where a variant supplies only `client` and `request`. `request` is
  *effectful* precisely so an implementation can pull a rotating credential per fetch instead of freezing it
  into config. Errors narrow to `Unreachable` (retryable), `BadStatus`, `JwksDecodingFailed`.
- **`K8sJwksSource`** is that variant for in-cluster use: TLS trusting the cluster CA, presenting the pod's
  own SA token as the bearer, because the discovery endpoints require
  `system:service-account-issuer-discovery`. Its constructor is private so the CA-trusting client and the
  token provider can't drift apart — build it via `make(config)`.

## Outbound: the pod's own credential

`ProjectedTokenProvider` reads the projected SA token **from the filesystem on every call**. That is
deliberate: the kubelet rotates it in place roughly hourly (atomic symlink swap, so reads never tear), so a
read-once-at-startup provider would hold a token that eventually expires. Reading per call also surfaces a
missing or unreadable file — automount disabled, a restrictive `defaultMode` under a non-root user — as an
`AdapterError` rather than assuming the credential is there.

`CachedTokenProvider` sits over it and holds the token until it is within `refreshSkew` of `exp`, so we roll
onto the rotated token *before* the held one dies.

## What a verified token becomes

Verification yields raw `JwtClaim`s. Two authenticators map claims to principals, and they differ on purpose:

- **`JwtServiceAuthenticator`** checks `aud` (must *contain* the expected audience) and `iss` (must equal the
  expected issuer) via `Expectations(audience, issuer)`, then maps `sub` to a `Service`. **This is the check
  that stops a validly-signed Kubernetes SA token minted for a different audience from authenticating here** —
  a shared issuer mints for many audiences, so signature validity alone proves nothing about intent.
- **`JwtUserAuthenticator`** does **no** `aud`/`iss` check, and says why: user tokens are our own registration
  issuer's, so trust is already scoped by which JWKS the verifier draws from. There is no shared issuer to
  guard against. It maps `sub` → `UserId` and the `name` claim → `UserName`, and its `any` accepts an
  `Option[SignedToken]`, yielding the anonymous `Requester.User` when absent.

Both take a `Monitor` (defaulting to `Monitor.Noop`) and wrap each authentication in a span plus metrics.

## Errors

Every failure lands in the `ApplicationError` hierarchy, and the refinements carry operational meaning:
`UnauthorisedError` for "this token is not acceptable" (`MalformedToken`, `UnknownKey`, `UntrustedToken`,
`TokenRejected`, `InvalidServiceToken`), `AdapterError` for "we could not carry out the check"
(`KeyUnusable`, `BadStatus`, `CaUnreadable`, `ClientUnavailable`, `TokenUnavailable`), and `TransientError`
on the retryable ones (`Unreachable`, `CanNotReviewToken`). A caller can therefore tell *rejected* from
*could not check* without matching on concrete cases.

## Open

- **No specs for `CachedJwksSource`, `HttpJwksSource` or `K8sJwksSource`** — the HTTP paths are the untested
  ones (a local server would cover them). The verifiers, the service/user authenticators and the projected
  provider do have specs.
- **`build.sbt`'s header comment is stale** — it still says only `common` is populated and that adapters
  "come next"; `auth`, `postgres`, `nats` and `telemetry` all exist now.
