package homelab.auth


import homelab.auth.K8sTokenReviewer.{ CanNotBuildRequest, CanNotDecodeToken, CanNotReviewToken, TokenRejected }
import homelab.common.error.ApplicationError.*
import homelab.common.types.SignedToken
import io.fabric8.kubernetes.api.model.authentication.{ TokenReview, TokenReviewBuilder, TokenReviewStatus }
import io.fabric8.kubernetes.client.{ KubernetesClient, KubernetesClientBuilder }
import pdi.jwt.{ JwtClaim, JwtOptions, JwtZIOJson }
import zio.*


/**
 * A [[TokenVerifier]] that validates a token via the Kubernetes **TokenReview** API rather than checking
 * its signature offline.
 *
 * Unlike [[JwksTokenVerifier]] (which verifies a signature against fetched JWKS), this asks the API server
 * to authenticate the token on every call. That is a network round-trip, but it is authoritative: it also
 * honours **revocation** — a bound service-account token whose pod or secret has been deleted is rejected,
 * which offline signature verification cannot see.
 *
 * The `audience` is both sent in the review request AND checked against the returned `status.audiences`: a
 * TokenReview server that isn't audience-aware would otherwise authenticate the token for the apiserver's
 * own audience, so the returned audiences must be validated (per the Kubernetes TokenReview contract).
 *
 * After a successful review the API server has already validated the token's signature, so its claims are
 * trustworthy; the token is decoded (verification off) only to produce the [[JwtClaim]] the port returns.
 * For a service-account token the decoded `sub` (`system:serviceaccount:ns:name`) equals the API server's
 * resolved `status.user.username`.
 *
 * Operational note: the caller's ServiceAccount must be allowed to create `tokenreviews` — the
 * `system:auth-delegator` ClusterRole.
 *
 * @param client   the Kubernetes client used to submit TokenReviews
 * @param audience the audience the token must be valid for
 */
final class K8sTokenReviewer(client: KubernetesClient, audience: String) extends TokenVerifier:

  /**
   * Verify `token` by submitting a Kubernetes TokenReview.
   *
   * @param token the signed token to verify
   * @return the token's claims; fails with [[TokenRejected]] (`UnauthorisedError`) if the API server does
   *         not authenticate the token or it isn't valid for `audience`, [[CanNotBuildRequest]] /
   *         [[CanNotReviewToken]] if the review can't be built or submitted, or [[CanNotDecodeToken]] if an
   *         authenticated token can't be decoded (all `AdapterError`)
   */
  override def verify(token: SignedToken): IO[K8sTokenReviewer.Error | UnauthorisedError, JwtClaim] =
    for
      request  <- buildRequest(token)
      response <- send(request)
      claims   <- outcome(response, token)
    yield claims

  /**
   * Build the TokenReview request carrying `token` and the expected `audience`.
   *
   * @param token the token to review
   * @return the review request; fails with [[CanNotBuildRequest]] if it can't be constructed
   */
  private def buildRequest(token: SignedToken): IO[CanNotBuildRequest, TokenReview] =
    ZIO
      .attempt(TokenReviewBuilder().withNewSpec().withToken(token).withAudiences(audience).endSpec().build())
      .mapError(CanNotBuildRequest(_))

  /**
   * Submit the review to the API server — a blocking network call.
   *
   * @param review the review request to submit
   * @return the completed review, carrying its status; fails with [[CanNotReviewToken]] if the call fails
   */
  private def send(review: TokenReview): IO[CanNotReviewToken, TokenReview] =
    ZIO.attemptBlocking(client.tokenReviews().create(review)).mapError(CanNotReviewToken(_))

  /**
   * Interpret the completed review: the token is accepted only if the API server authenticated it AND it is
   * valid for `audience`; on success its claims are decoded.
   *
   * @param review the completed review returned by the API server
   * @param token  the original token, decoded on success
   * @return the claims; fails with [[TokenRejected]] if there's no status, the token isn't authenticated, or
   *         the audience doesn't match, or [[CanNotDecodeToken]] if an authenticated token can't be decoded
   */
  private def outcome(review: TokenReview, token: SignedToken): IO[TokenRejected | CanNotDecodeToken, JwtClaim] =
    Option(review.getStatus) match
      case None                                   => ZIO.fail(TokenRejected("Kubernetes returned no review status"))
      case Some(status) if !authenticated(status) => ZIO.fail(TokenRejected(reason(status)))
      case Some(status) if !validFor(status)      => ZIO.fail(TokenRejected(s"token not valid for audience '$audience'"))
      case Some(_)                                => decode(token)

  /**
   * Whether the API server marked the token authenticated.
   *
   * @param status the review status returned by the API server
   * @return `true` if authenticated; `false` when the flag is unset (it is a nullable `java.lang.Boolean`)
   */
  private def authenticated(status: TokenReviewStatus): Boolean =
    Option(status.getAuthenticated).exists(_.booleanValue)

  /**
   * Whether the review's returned audiences include the expected `audience` — the check that the TokenReview
   * server actually honoured the requested audience rather than falling back to the apiserver's own.
   *
   * @param status the review status returned by the API server
   * @return `true` if `status.audiences` contains `audience`
   */
  private def validFor(status: TokenReviewStatus): Boolean =
    Option(status.getAudiences).exists(_.contains(audience))

  /**
   * The API server's explanation for a failed review.
   *
   * @param status the review status returned by the API server
   * @return the status error message, or a default when the API server gave none
   */
  private def reason(status: TokenReviewStatus): String =
    Option(status.getError).filter(_.nonEmpty).getOrElse("token not authenticated")

  /**
   * Decode an already-validated token into its claims — signature, expiry and not-before checks are off
   * because the API server has already vouched for it; this only extracts the payload.
   *
   * @param token the authenticated token to decode
   * @return the claims; fails with [[CanNotDecodeToken]] if the payload can't be parsed
   */
  private def decode(token: SignedToken): IO[CanNotDecodeToken, JwtClaim] =
    ZIO
      .fromTry(JwtZIOJson.decode(token, JwtOptions(signature = false, expiration = false, notBefore = false)))
      .mapError(CanNotDecodeToken(_))


object K8sTokenReviewer:

  /**
   * Build an in-cluster reviewer that verifies tokens for `audience`, owning a Kubernetes client that is
   * closed with the scope.
   *
   * @param audience the audience a token must be valid for
   * @return the reviewer; fails with [[ClientUnavailable]] if the Kubernetes client can't be built
   */
  def make(audience: String): ZIO[Scope, ClientUnavailable, K8sTokenReviewer] =
    ZIO
      .fromAutoCloseable(ZIO.attempt(KubernetesClientBuilder().build()))
      .mapError(ClientUnavailable(_))
      .map(new K8sTokenReviewer(_, audience))

  /** Base type for adapter-level failures of the reviewer (building the client, submitting, or decoding). */
  sealed trait Error extends AdapterError

  /** The Kubernetes client that [[make]] needs couldn't be built. */
  case class ClientUnavailable(cause: Throwable) extends Error:
    override def message: String = s"Failed to build the Kubernetes client: ${cause.getMessage}"

  /** The token payload couldn't be decoded after the API server authenticated it. */
  case class CanNotDecodeToken(cause: Throwable) extends Error, DecodingError:
    override def message: String = s"Failed to decode token: ${cause.getMessage}"

  /** The TokenReview request couldn't be built. */
  case class CanNotBuildRequest(cause: Throwable) extends Error:
    override def message: String = s"Failed to build token review request: ${cause.getMessage}"

  /** The TokenReview call to the API server failed — transient (apiserver unreachable, network blip). */
  case class CanNotReviewToken(cause: Throwable) extends Error, TransientError:
    override def message: String = s"Failed to review token: ${cause.getMessage}"

  /** The API server did not accept the token: not authenticated, wrong audience, or no status returned. */
  case class TokenRejected(reason: String) extends UnauthorisedError:
    override def message: String = s"Token rejected by Kubernetes API: $reason"
