# Checkpoint — 2026-07-06 — auth stack built and promoted to the `auth` module

> Built the whole service/user token-verification stack in `incubator/v3`, then promoted it to a real
> `homelab-auth` module. It fetches a JWKS (public issuer or in-cluster Kubernetes), caches it, verifies a
> JWT locally, and maps the claims to a `Requester`. JDK `HttpClient` throughout — `zio-http` is gone from
> the path. Prior sketches (`v1`/`v2`) still sit in `incubator`, now dead, awaiting a prune.

## Current state

**`auth`** (`homelab-auth`, package `homelab.auth`) — the promoted v3 stack, 22 tests, compiles under the
strict prod flags (Java crypto/HTTP interop is clean; no `scalacOptions := Nil`):

- **Token providers:** `JwtProvider` port; `ProjectedTokenProvider` (reads the pod's SA token *fresh per
  call* → rotation-safe, `TokenUnavailable` if missing); `CachedTokenProvider` (caches until `exp − skew`,
  reads `exp` via an unverified decode).
- **JWKS sources:** `JwksSource` port (`all` + default `get(kid)`); `HttpJwksSource` (abstract base — the
  only extension point is the effectful `request`, everything else handled); `K8sJwksSource` (extends it —
  `make(Config)` bundles the cluster-CA client + projected-token bearer); `CachedJwksSource` (caches the
  set, refetches on unknown `kid` = rotation).
- **Keys:** `JsonWebKey` (kty-discriminated decoder) + `PublicKeyDecoder` (RSA `n`/`e`, Ed25519 via the
  12-byte SPKI header trick).
- **Verify + authenticate:** `TokenVerifier` port + `JwksTokenVerifier` (own `kid → PublicKey` cache;
  `JwtZIOJson` decode so `sub`/`aud`/`iss` are structured; errors split `UnauthorisedError` vs
  `AdapterError`); `TokenVerifier.{from(uri), k8s(config)}` DevX shortcuts; `JwtServiceAuthenticator`
  (adds `aud`/`iss` via `Expectations`) and `JwtUserAuthenticator` (`sub`→`UserId`, `name`→`UserName`;
  `any` downgrades invalid → `Anonymous` but propagates `AdapterError`), both implementing the `common` ports.

**`common`** — unchanged; already holds the ports (`ServiceAuthenticator`, `UserAuthenticator`), `Requester`,
value objects, errors (4 tests).

**`incubator`** — `v1` (12 files) and `v2` (4 files) remain, still green (43 tests), now superseded and
awaiting prune. `v2` is the last `zio-http` user.

**`magnum` / `inmemory`** — still empty scaffolds.

## Decisions & findings this session

- **Transport = JDK `java.net.http.HttpClient`, not `zio-http`.** No resource layer, no `Client` in the
  port env; construction is plain values. `sttp` doesn't help with the CA `SSLContext` — that ceremony is
  contained in one `caTrustingClient` helper (lives on `K8sJwksSource`).
- **TokenReview dropped — JWKS-only.** Its only real edge over JWKS is *immediate revocation*, mitigated by
  the ~1h SA-token TTL; v1's impl even discarded groups/uid. Asymmetric-only (EdDSA + RS256).
- **`pdi.jwt.Jwt` (core) only lifts timestamp claims** — `sub`/`iss`/`aud` stay in `content`. Must use
  **`JwtZIOJson`** for structured claims. (v2's `JwksServiceAuthenticator` carries this latent bug; it dies
  with the prune.)
- **`aud`/`iss` live in `JwtServiceAuthenticator`** (`Expectations`) — the check that stops a validly-signed
  SA token minted for another audience. User auth omits it: our own issuer's keys already scope trust.
- **Projected SA token:** has `exp` (~1h, kubelet-rotated in place at ~80% TTL); world-readable `0644` on a
  separate mount, so `runAsUser` / `allowPrivilegeEscalation:false` / `readOnlyRootFilesystem:true` don't
  block it. Read fresh per call → the `JwtProvider` effect.
- **Ed25519 JWK `x`** = last 32 bytes of the X.509/SPKI DER; the toolkit re-prepends the fixed 12-byte
  header to decode. Inverse of `registration-service`'s `Jwks.scala` (verified that encoding is correct).
- **Error consistency:** build methods (`make`/`k8s`) fail with the precise `CaUnreadable` (an
  `ApplicationError`, not `AdapterError` — it's a build error); `verify`/`authenticate` use
  `AdapterError | UnauthorisedError`.
- **Per-request cost is fine:** verify is a sub-ms public-key op; the `kid → PublicKey` cache means
  `KeyFactory` runs once per key, and the JWKS fetch is cached upstream.

## What's next

1. **Prune `incubator/v1` + `v2`** (dead). Deleting `v2` removes the last `zio-http` user → drop `zio-http`
   from `incubator`'s deps. Then decide `incubator`'s fate (keep empty for sketches, or remove).
2. **Fill coverage gaps** in `auth`: no specs yet for `CachedTokenProvider` (TestClock — cache-hit-then-
   refetch), `CachedJwksSource`, `HttpJwksSource`/`K8sJwksSource` (local server).
3. **Update `build.sbt` header comment** (still says only `common` data is populated; `auth` now is too).
4. Implement the persistence (`magnum`) + `inmemory` adapters.
