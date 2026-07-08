---
title: "Checkpoint — 2026-07-05 — toolkit bootstrapped; auth verification prototyped in `incubator`"
type: session
status: current
updated: 2026-07-05
tags: [homelab-toolkit-zio, auth, jwks, zio, checkpoint]
---

# Checkpoint — 2026-07-05 — toolkit bootstrapped; auth verification prototyped in `incubator`

> First real session on `homelab-toolkit-zio`. Stood up the repo, populated `common` with data + ports,
> and prototyped the whole service-token verification story in `incubator` (JWKS fetch → key
> reconstruction → JWT verify → identity). What's built, what's decided, what's next below.

## Current state

**`common`** (`homelab-common`) — data + ports, all documented, compiling:
- data: `error/{ApplicationError, ValidationError}`, `types` (opaque value objects), `auth/Requester`
- ports: `auth/{ServiceAuthenticator, UserAuthenticator}`, `database/Database[Tx]`, `monitor/Monitor`,
  `store/KeyValueStore` (`delete` returns `Boolean` — reports whether the key existed)
- primitives: `flow/{Loop, Stepper}` (+ `LoopSpec`, which also proved the covariant `Next` infers cleanly)

**Adapter modules** (`magnum`, `inmemory`, `auth`) — declared, empty scaffolds.

**`incubator`** (`publish/skip`) — throwaway sketches, all proven with real local HTTP servers:
- `v1/` — the exploratory auth sketches: `TokenVerifier` seam, `KeySource`/`JwksKeySource`,
  `RegistrationTokenVerifier` (EdDSA), `K8sServiceAccountVerifier` (via `TokenReviewer`/`HttpTokenReviewer`),
  `HttpKeySource`.
- `v2/` — the cleaner, **current direction**: `JsonWebKey` (kty-discriminated decoder + `publicKey` for
  RSA & Ed25519), `PublicKeySource` port, `HttpPublicKeySource` (real zio-http JWKS fetch, `Config` with
  optional bearer, `layer(config)` wiring a cluster-CA `Client`), `JwksServiceAuthenticator`
  (`Ref`-cached keys, self-decoding).

**Deps:** Scala 3.3.4, zio 2.1.23, zio-prelude 1.0.0-RC47, zio-json (via jwt-zio-json 11.0.4),
jwt-scala 11.0.4, zio-http 3.0.1, zio-test.

## Decisions & findings this session

- **Stack = ZIO** until Kyo matures — full rationale in
  `../../../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md`.
- **Two token trust roots** modelled: registration (our own **EdDSA** JWKS) vs k8s SA (**RS256**; verify
  via JWKS *or* the more idiomatic in-cluster **TokenReview**). Both map to `Requester.Service` and the
  same `AdapterError` / `UnauthorisedError` split.
- **Ed25519 from a JWK `x`:** wrap the raw 32 bytes in the fixed 12-byte SPKI DER header and use
  `X509EncodedKeySpec` — the JDK decodes the point, no manual y/x-sign parsing. (RSA: `RSAPublicKeySpec`
  from `n`/`e`.)
- **Authenticator error rule:** forward the source's `AdapterError`, but an unknown `kid` → untrusted
  token → `UnauthorisedError`.
- **Scaladoc convention** (per `common/.../ServiceAuthenticator.scala`): a full block on **every** method
  — description + `@param` + `@return` with "fails with …" — including overrides and privates.
- **Build gotcha:** zio-prelude/zio-http drag zio-core to **2.1.23**; `zio-test` must match or its layer
  macros throw `NoSuchMethodError`. Keep `zioVersion` the single knob for all of zio/zio-test.

## What's next

1. **`aud` + `iss` validation** in `JwksServiceAuthenticator` (config-driven `expectedIssuer`/`expectedAudience`)
   — the security-critical gap for k8s SA projected tokens; `Jwt.decode` checks only signature + expiry, so
   without it any validly-signed SA token (any audience) passes.
2. **Consolidate the `incubator`** — `v2` is the direction; prune the `v1` sketches (drop the redundant ones).
3. **Promote `v2` auth** into the real `auth` module (decide what stays a `common` port vs adapter-internal,
   e.g. `PublicKeySource`).
4. Implement the persistence + in-memory adapters.
5. End-to-end spec for `JwksServiceAuthenticator` (issue a token → serve its JWKS → authenticate).
