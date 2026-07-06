package homelab.auth


import homelab.common.auth.Requester.User
import homelab.common.auth.UserAuthenticator
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ SignedToken, UserId, UserName }
import homelab.auth.JwtUserAuthenticator.*
import pdi.jwt.JwtClaim
import zio.*
import zio.json.*

import java.util.UUID
import scala.util.Try


/**
 * A [[UserAuthenticator]] over a [[TokenVerifier]]: verifies the user token and maps its claims to a
 * [[Requester.User.Authenticated]] — the subject to a [[UserId]], the `name` claim to a [[UserName]].
 *
 * No `aud`/`iss` check here (unlike [[JwtServiceAuthenticator]]): user tokens are our own registration
 * issuer's, so trust is already scoped by which JWKS the verifier draws from — there's no shared issuer
 * minting tokens for other audiences to guard against.
 *
 * @param verifier verifies the token's signature and expiry
 */
final class JwtUserAuthenticator(verifier: TokenVerifier) extends UserAuthenticator:

  /**
   * Optional auth — a missing or unverifiable token yields [[Requester.User.Anonymous]], never a failure,
   * except an infrastructure failure which still propagates.
   *
   * @param token the bearer token, if one was presented
   * @return the caller; fails only with `AdapterError` — a present-but-invalid token downgrades to anonymous
   */
  def any(token: Option[SignedToken]): IO[AdapterError, User] =
    token match
      case None    => ZIO.succeed(User.Anonymous)
      case Some(t) =>
        authenticate(t).catchAll {
          case _: UnauthorisedError  => ZIO.succeed(User.Anonymous)
          case adapter: AdapterError => ZIO.fail(adapter)
        }

  /**
   * Required auth — a signed-in user or a rejection.
   *
   * @param token the bearer token presented on the request
   * @return the authenticated caller; fails with [[InvalidUserToken]] if the claims can't be mapped, or
   *         forwards the verifier's `AdapterError | UnauthorisedError`
   */
  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, User.Authenticated] =
    for
      claim <- verifier.verify(token)
      user  <- authenticated(claim)
    yield user

  /**
   * Map verified claims to an authenticated user.
   *
   * @param claim the verified claims
   * @return the authenticated user; fails with [[InvalidUserToken]] if the id or name can't be read
   */
  private def authenticated(claim: JwtClaim): IO[UnauthorisedError, User.Authenticated] =
    for
      id   <- userId(claim)
      name <- userName(claim)
    yield User.Authenticated(id, name)

  /**
   * Read the user id from the subject claim (a UUID).
   *
   * @param claim the verified claims
   * @return the user id; fails with [[InvalidUserToken]] if the subject is missing or isn't a UUID
   */
  private def userId(claim: JwtClaim): IO[UnauthorisedError, UserId] =
    for
      sub <- ZIO.fromOption(claim.subject).orElseFail(InvalidUserToken("token has no subject"))
      id  <- ZIO.fromTry(Try(UUID.fromString(sub))).mapBoth(_ => InvalidUserToken(s"subject '$sub' is not a user id"), UserId(_))
    yield id

  /**
   * Read the user name from the `name` claim (a custom claim in the token content).
   *
   * @param claim the verified claims
   * @return the user name; fails with [[InvalidUserToken]] if the content is unreadable or has no `name`
   */
  private def userName(claim: JwtClaim): IO[UnauthorisedError, UserName] =
    for
      identity <- ZIO.fromEither(claim.content.fromJson[Identity]).mapError(reason => InvalidUserToken(s"unreadable claims: $reason"))
      name     <- ZIO.fromOption(identity.name).orElseFail(InvalidUserToken("token has no name claim"))
    yield UserName(name)


object JwtUserAuthenticator:

  /** The custom claims a user token carries beyond the registered ones — the display `name`. */
  final private case class Identity(name: Option[String]) derives JsonDecoder

  /** The token verified cryptographically but its claims don't map to a user — no subject/id or no name. */
  final case class InvalidUserToken(reason: String) extends UnauthorisedError:
    override def message: String = s"invalid user token: $reason"
