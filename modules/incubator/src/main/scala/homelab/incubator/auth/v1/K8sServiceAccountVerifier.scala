package homelab.incubator.auth.v1


import homelab.common.types.SignedToken
import zio.*


/**
 * Sketch: verifies a Kubernetes ServiceAccount token by delegating to the cluster's `TokenReview` API
 * (via [[TokenReviewer]]) rather than checking a signature locally — the idiomatic way to validate
 * projected, audience-bound SA tokens from inside the cluster.
 *
 * An unauthenticated token → `Invalid`; the review call itself failing (API server unreachable) →
 * `Unavailable`. The subject is the SA's canonical name `system:serviceaccount:<ns>:<name>`.
 */
final class K8sServiceAccountVerifier(reviewer: TokenReviewer) extends TokenVerifier:

  def verify(token: SignedToken): IO[TokenVerifier.Failure, Claims] =
    reviewer
      .review(token)
      .mapError(u => TokenVerifier.Failure.Unavailable(u.reason, u.cause))
      .flatMap {
        case TokenReviewer.Result(true, Some(username)) => ZIO.succeed(Claims(username, shortName(username)))
        case _                                          => ZIO.fail(TokenVerifier.Failure.Invalid("service account token not authenticated"))
      }

  /** `system:serviceaccount:<ns>:<name>` → `<name>`. */
  private def shortName(username: String): String = username.split(':').lastOption.getOrElse(username)
