package homelab.incubator.auth.v1

import homelab.common.types.SignedToken
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim }
import zio.*
import zio.json.*

import java.nio.charset.StandardCharsets.UTF_8
import java.security.PublicKey
import java.util.Base64

/**
 * Sketch: verifies tokens issued by the registration service (EdDSA). Reads the `kid` from the JWT
 * header, resolves the matching public key from the [[KeySource]] (registration's JWKS), then verifies
 * the Ed25519 signature + expiry with jwt-scala.
 *
 * Error mapping is the whole point: `PublicKeySource.Failure.UnknownKey → Invalid` (the token is signed by a
 * key we don't trust) and `Unavailable → Unavailable` (the JWKS is unreachable → infrastructure).
 */
final class RegistrationTokenVerifier(keySource: KeySource) extends TokenVerifier:

  def verify(token: SignedToken): IO[TokenVerifier.Failure, Claims] =
    for
      kid    <- keyId(token)
      key    <- keySource.publicKey(kid).mapError(fromKeySource)
      claim  <- decode(token, key)
      claims <- extract(claim)
    yield claims

  /** Read the `kid` from the (unverified) header — needed to pick the key before we can verify. */
  private def keyId(token: SignedToken): IO[TokenVerifier.Failure, String] =
    ZIO
      .fromEither {
        for
          segment <- token.split('.').headOption.toRight("malformed token")
          header  <- new String(Base64.getUrlDecoder.decode(segment), UTF_8).fromJson[RegistrationTokenVerifier.Head]
          kid     <- header.kid.toRight("missing kid in header")
        yield kid
      }
      .mapError(reason => TokenVerifier.Failure.Invalid(reason))

  private def decode(token: SignedToken, key: PublicKey): IO[TokenVerifier.Failure, JwtClaim] =
    ZIO.suspendSucceed {
      Jwt
        .decode(token, key, Seq(JwtAlgorithm.EdDSA))
        .fold(error => ZIO.fail(TokenVerifier.Failure.Invalid(Option(error.getMessage).getOrElse(error.toString))), ZIO.succeed)
    }

  private def extract(claim: JwtClaim): IO[TokenVerifier.Failure, Claims] =
    for
      subject <- ZIO.fromOption(claim.subject).orElseFail(TokenVerifier.Failure.Invalid("missing subject claim"))
      name <- ZIO
                .fromEither(claim.content.fromJson[RegistrationTokenVerifier.Name])
                .mapError(reason => TokenVerifier.Failure.Invalid(s"malformed claims: $reason"))
    yield Claims(subject, name.name)

  private def fromKeySource(failure: KeySource.Failure): TokenVerifier.Failure =
    failure match
      case KeySource.Failure.UnknownKey(kid)            => TokenVerifier.Failure.Invalid(s"unknown signing key: $kid")
      case KeySource.Failure.Unavailable(reason, cause) => TokenVerifier.Failure.Unavailable(reason, cause)

object RegistrationTokenVerifier:
  private final case class Head(kid: Option[String]) derives JsonDecoder
  private final case class Name(name: String) derives JsonDecoder
