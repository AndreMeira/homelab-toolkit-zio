---
title: Auth — verifying tokens, and what a verified token means
type: architecture
status: current
updated: 2026-09-22
tags: [auth, jwt, jwks, kubernetes, tokenreview, service-account, caching, usage]
---

# Auth

`homelab.auth` — turning a bearer token into a principal. 15 files, ~1,265 lines, and every one of them is
either a *verifier*, a *source of keys*, a *provider of our own credential*, or a cache over one of those.

The ports it implements live in `common`: `homelab.common.auth.{ServiceAuthenticator, UserAuthenticator,
Requester}`. This module is the adapter side.

Code: `modules/auth/src/main/scala/homelab/auth/`
Specs: `CachedTokenVerifierSpec`, `JwksTokenVerifierSpec`, `JwtServiceAuthenticatorSpec`,
`JwtUserAuthenticatorSpec`, `ProjectedTokenProviderSpec`

## What it proposes

One job, stated as narrowly as it can be: **turn a bearer token into a principal, or say why not.**

It does not decide what a principal may do. It has no opinion on routes, roles or ownership. What it
produces is a `Requester` — a `Service`, an authenticated `User`, or `User.Anonymous` — and the rest is the
application's.

Three things follow from drawing the line there, and they are what the module is actually for:

- **Authorisation stays visible in the domain.** A handler takes a `Requester` as a parameter, so who is
  asking is in the signature rather than in a thread local or an interceptor nobody reads.
- **A token's validity and a token's meaning are separate questions.** A verifier answers *is this signed by
  someone we trust, and unexpired*. An authenticator answers *and is it addressed to us, and who does it name*.
  Keeping them apart is what lets the same verifier serve callers with different expectations.
- **Both directions of credential are covered.** Verifying what arrives and producing what we present are
  different problems with different failure modes, and the module names them separately rather than calling
  both "auth".

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

## Choosing

Two questions decide everything. **Whose issuer**, and **does revocation matter.**

| you are verifying | reach for | round-trip | needs RBAC | sees revocation |
|---|---|---|---|---|
| a Kubernetes service-account token, and revocation matters | `TokenVerifier.k8s(audience, ttl)` | per uncached call | `system:auth-delegator` | **yes**, within `ttl` |
| a Kubernetes service-account token, offline is worth more | `TokenVerifier.k8sOffline(config)` | none after the first fetch | none | no, until `exp` |
| a token from any issuer publishing a JWKS | `TokenVerifier.from(uri)` | on key rotation only | none | no, until `exp` |

Then pick the authenticator by *what the token is*, not by how it was verified:

| the caller is | use | checks `aud` / `iss` | yields |
|---|---|---|---|
| another service | `JwtServiceAuthenticator` | **yes**, both | `Service` |
| a person, via our own issuer | `JwtUserAuthenticator` | no, deliberately | `User.Authenticated`, or `User.Anonymous` from `any` |

**When to prefer which verifier**, in the words that actually decide it:

- Take **`k8s`** when a deleted pod or Secret must stop working promptly — anything that mutates, anything
  with a blast radius. The `ttl` is the whole freshness dial: it is how long a revoked token keeps working.
  Keep it near a minute.
- Take **`k8sOffline`** when the service must keep serving while the apiserver is unreachable, or when you
  cannot be granted `system:auth-delegator`. Accept that a leaked token is good until its own `exp`.
- Take **`from`** for anything that is not the cluster issuer — our own registration issuer, or a third
  party. This is also the path a **user** token takes.

A service that accepts both kinds of caller builds **two** verifiers and **two** authenticators. They share
nothing, and should not: the trust roots are different.

## Using it

No in-homelab service wires this yet, so the recipes below are prescriptive rather than copied from a
caller. Each is the whole of what a composition root needs.

**Accepting calls from other services, authoritatively.** `k8s` needs a `Scope` because it holds an HTTP
client for the apiserver:

```scala
import homelab.auth.{ JwtServiceAuthenticator, TokenVerifier }

ZIO.scoped:
  for
    verifier <- TokenVerifier.k8s(audience = "meal-planner", ttl = 1.minute)
    services  = JwtServiceAuthenticator(
                  verifier,
                  JwtServiceAuthenticator.Expectations(
                    audience = "meal-planner",
                    issuer   = "https://kubernetes.default.svc.cluster.local",
                  ),
                  monitor,
                )
  yield services
```

The audience appears twice on purpose: once so the apiserver reviews the token *for that audience*, and once
so the authenticator refuses a token minted for a different one. Both are needed — a shared issuer mints for
many audiences, and a signature proves nothing about intent.

**The same, offline.** No scope, no RBAC, no revocation:

```scala
for
  verifier <- TokenVerifier.k8sOffline()          // defaults are the standard in-cluster paths
  services  = JwtServiceAuthenticator(verifier, expectations, monitor)
yield services
```

`K8sJwksSource.Config` carries the three paths — the JWKS URI, the cluster CA, and the pod's own token —
and their defaults are the conventional in-cluster locations. Override them only where the deployment does.

**Accepting user tokens from our own issuer:**

```scala
for
  verifier <- TokenVerifier.from(URI.create("https://registration.homelab.svc/.well-known/jwks.json"))
  users     = JwtUserAuthenticator(verifier, monitor)
yield users
```

Then, at the edge, `users.any(bearerToken)` turns an optional token into a `User` — `User.Anonymous` when
there is none — and `users.authenticate(token)` insists on a real one.

**Presenting our own credential to someone else:**

```scala
for
  provider <- CachedTokenProvider.make(
                ProjectedTokenProvider(Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token")),
                refreshSkew = 1.minute,
              )
  token    <- provider.get                        // on every outbound call
yield token
```

Call `get` per request rather than holding the result: the cache is what avoids the filesystem read, and
holding the `SignedToken` yourself defeats the rotation handling the cache exists for.

**What the deployment owes**, whichever path is taken:

- `k8s` requires the pod's service account to be bound to `system:auth-delegator`.
- `k8sOffline` and any use of `K8sJwksSource` require `system:service-account-issuer-discovery`.
- Anything reading the projected token requires that automounting is on and that `defaultMode` lets the
  container's user read the file. A restrictive mode under a non-root user surfaces as `TokenUnavailable`,
  not as a silent anonymous call.
- Callers must request a token **for this service's audience**; the cluster default audience is the
  apiserver's, and it will be refused.

## Inbound: how the two Kubernetes paths differ

The table above says which to take; this is what each does, since the trade is not obvious from the names.
Both verify a service-account token; they differ on one axis, and the doc on each says so:

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
- **Nothing in the homelab uses this yet.** The recipes in *Using it* are written from the signatures rather
  than from a caller, so the first real consumer is also the first test of whether the composition root
  reads as intended — particularly the two-verifier case, where a service accepts both service and user
  callers and the wiring doubles.
- **No `Module.scala`.** Every other concern in the toolkit composes its own contents; this module leaves
  wiring to the caller, which is why *Using it* has to spell it out. Whether that is right depends on
  whether a service ever wants the pieces separately — and none has yet.
