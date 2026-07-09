package homelab.auth


import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import homelab.auth.K8sJwksSource.CaUnreadable
import homelab.auth.K8sTokenReviewer.ClientUnavailable
import pdi.jwt.JwtClaim
import zio.*

import java.net.URI


/** Verify a signed token and return its claims. */
trait TokenVerifier:

  /**
   * Verify `token` and return its claims.
   *
   * @param token the signed token to verify
   * @return the verified claims; fails with an `UnauthorisedError` if the token is invalid, or an
   *         `AdapterError` if verification can't be carried out
   */
  def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim]


object TokenVerifier:

  /**
   * A verifier over a public JWKS at `uri` — a basic [[HttpJwksSource]] wrapped in a [[CachedJwksSource]].
   *
   * @param uri the JWKS endpoint
   * @return the verifier
   */
  def from(uri: URI): UIO[TokenVerifier] =
    verifierFor(HttpJwksSource.make(uri))

  /**
   * A verifier for the in-cluster Kubernetes issuer that authenticates each token via the **TokenReview**
   * API ([[K8sTokenReviewer]]) and caches successful reviews for `ttl` ([[CachedTokenVerifier]]).
   *
   * Authoritative — it honours revocation of bound tokens — at the cost of an apiserver round-trip per
   * uncached token; the pod's ServiceAccount needs the `system:auth-delegator` ClusterRole. For an offline,
   * self-contained alternative that cannot see revocation, see [[k8sOffline]].
   *
   * @param audience the audience a token must be valid for
   * @param ttl      how long a successful review is reused before re-verifying (default one minute)
   * @return the verifier, owning a Kubernetes client closed with the scope; fails with
   *         [[K8sTokenReviewer.ClientUnavailable]] if the client can't be built
   */
  def k8s(audience: String, ttl: Duration = 1.minute): ZIO[Scope, ClientUnavailable, TokenVerifier] =
    K8sTokenReviewer.make(audience).flatMap(CachedTokenVerifier.make(_, ttl))

  /**
   * A verifier over the in-cluster Kubernetes issuer's public JWKS — a [[K8sJwksSource]] wrapped in a
   * [[CachedJwksSource]]. Offline and self-contained (no apiserver round-trip, no RBAC), but it cannot see
   * revocation: a token is accepted until its own `exp`. Prefer [[k8s]] when revocation matters.
   *
   * @param config the in-cluster issuer configuration
   * @return the verifier; fails with [[K8sJwksSource.CaUnreadable]] if the cluster CA can't be read
   */
  def k8sOffline(config: K8sJwksSource.Config = K8sJwksSource.Config()): IO[CaUnreadable, TokenVerifier] =
    K8sJwksSource.make(config).flatMap(verifierFor)

  /**
   * Wrap `source` in a fresh key-set cache and a caching [[JwksTokenVerifier]].
   *
   * @param source the JWKS source to verify against
   * @return the verifier
   */
  private def verifierFor(source: JwksSource): UIO[TokenVerifier] =
    for
      cache    <- Ref.make(Option.empty[JsonWebKey.Set])
      verifier <- JwksTokenVerifier.make(CachedJwksSource(source, cache))
    yield verifier
