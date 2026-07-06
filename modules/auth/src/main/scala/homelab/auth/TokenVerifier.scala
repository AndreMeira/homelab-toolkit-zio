package homelab.auth


import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import homelab.auth.K8sJwksSource.CaUnreadable
import pdi.jwt.JwtClaim
import zio.*

import java.net.URI


/** Verify a signed token against an issuer's keys and return its claims. */
trait TokenVerifier:

  /**
   * Verify `token` and return its claims.
   *
   * @param token the signed token to verify
   * @return the verified claims; fails with an `UnauthorisedError` if the token is invalid, or an
   *         `AdapterError` if the keys can't be fetched or used
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
   * A verifier over the in-cluster Kubernetes issuer described by `config` — a [[K8sJwksSource]] wrapped in
   * a [[CachedJwksSource]].
   *
   * @param config the in-cluster issuer configuration
   * @return the verifier; fails with [[K8sJwksSource.CaUnreadable]] if the cluster CA can't be read
   */
  def k8s(config: K8sJwksSource.Config): IO[CaUnreadable, TokenVerifier] =
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
