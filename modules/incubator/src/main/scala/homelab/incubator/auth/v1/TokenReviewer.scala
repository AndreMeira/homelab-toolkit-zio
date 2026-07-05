package homelab.incubator.auth.v1

import homelab.common.types.SignedToken
import zio.*

/**
 * Sketch seam: the Kubernetes `TokenReview` — hand a ServiceAccount token to the API server and get back
 * whether it authenticated and, if so, the caller's username (`system:serviceaccount:<ns>:<name>`). A
 * real impl POSTs an `authentication.k8s.io/v1` TokenReview to the API server. Injected so the k8s
 * verifier is testable without a cluster.
 */
trait TokenReviewer:
  def review(token: SignedToken): IO[TokenReviewer.Unavailable, TokenReviewer.Result]

object TokenReviewer:
  /** The review outcome: whether the token authenticated, and the caller's username when it did. */
  final case class Result(authenticated: Boolean, username: Option[String])

  /** The review call itself failed (API server unreachable / RBAC denied) — infrastructure. */
  final case class Unavailable(reason: String, cause: Throwable)
