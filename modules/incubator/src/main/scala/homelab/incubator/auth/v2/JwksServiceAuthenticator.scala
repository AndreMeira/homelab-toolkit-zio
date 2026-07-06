package homelab.incubator.auth.v2

import homelab.common.auth.Requester.Service
import homelab.common.auth.ServiceAuthenticator
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ ServiceName, SignedToken }
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim }
import zio.*
import zio.json.*

import java.nio.charset.StandardCharsets.UTF_8
import java.security.PublicKey
import java.util.Base64

/**
 * A [[ServiceAuthenticator]] that verifies a service token against keys from a [[PublicKeySource]]. It
 * caches resolved public keys by `kid` in a `Ref` (a new `kid` triggers a fetch; known ones are served
 * from the cache), then decodes and verifies the JWT itself.
 *
 * Source (`AdapterError`) failures are forwarded — except an unknown `kid`, which means the token is
 * signed by a key we don't publish (untrusted), so that becomes an `UnauthorisedError`.
 */
final class JwksServiceAuthenticator private (source: PublicKeySource, cache: Ref[Map[String, PublicKey]])
    extends ServiceAuthenticator:
  import JwksServiceAuthenticator.*

  /**
   * Authenticate a calling service from its bearer token: pick the key named by the header, verify the
   * JWT, and lift the `sub` claim to the calling [[Service]].
   *
   * @param token the signed service token presented on the call
   * @return the calling service; fails with `UnauthorisedError` if the token is missing, malformed,
   *         signed by an unknown key, unverifiable, or expired, or with `AdapterError` if the key source
   *         can't be reached or its response can't be decoded
   */
  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, Service] =
    for
      kid     <- keyId(token)
      key     <- publicKey(kid)
      claim   <- verify(token, key)
      service <- toService(claim)
    yield service

  /**
   * Read the `kid` from the token's (unverified) header — needed to pick the key before verifying.
   *
   * @param token the token whose header is parsed
   * @return the `kid`; fails with `UnauthorisedError` when the token is malformed or its header has no `kid`
   */
  private def keyId(token: SignedToken): IO[UnauthorisedError, String] =
    ZIO
      .fromEither {
        for
          segment <- token.split('.').headOption.toRight("malformed token")
          header  <- new String(Base64.getUrlDecoder.decode(segment), UTF_8).fromJson[Header]
          kid     <- header.kid.toRight("missing kid in token header")
        yield kid
      }
      .mapError(InvalidServiceToken(_))

  /**
   * Resolve the public key for `kid` from the cache, falling back to [[fetch]] on a miss.
   *
   * @param kid the key id read from the token header
   * @return the public key; fails as [[fetch]] does when the cache misses
   */
  private def publicKey(kid: String): IO[AdapterError | UnauthorisedError, PublicKey] =
    cache.get.flatMap {
      _.get(kid) match
        case Some(key) => ZIO.succeed(key)
        case None      => fetch(kid)
    }

  /**
   * Fetch the key for `kid` from the source, cache it on success, and classify its failures.
   *
   * @param kid the key id to fetch and cache
   * @return the fetched public key; fails with `UnauthorisedError` when `kid` is unknown (untrusted
   *         token), otherwise forwards the source's `AdapterError`
   */
  private def fetch(kid: String): IO[AdapterError | UnauthorisedError, PublicKey] =
    source.get(kid).mapError(untrustedKeyOrForward(kid)).tap(key => cache.update(_.updated(kid, key)))

  /**
   * Verify the JWT's signature and expiry with `key`, restricted to the supported asymmetric algorithms.
   *
   * @param token the token to verify
   * @param key   the public key to verify the signature against
   * @return the verified claims; fails with `UnauthorisedError` when the signature, expiry, or format is invalid
   */
  private def verify(token: SignedToken, key: PublicKey): IO[UnauthorisedError, JwtClaim] =
    ZIO
      .fromTry(Jwt.decode(token, key, Seq(JwtAlgorithm.EdDSA, JwtAlgorithm.RS256)))
      .mapError(e => InvalidServiceToken(Option(e.getMessage).getOrElse(e.toString)))

  /**
   * Map the verified claims to the calling service — the `sub` claim is the service identity.
   *
   * @param claim the verified JWT claims
   * @return the calling service; fails with `UnauthorisedError` when the `sub` claim is absent
   */
  private def toService(claim: JwtClaim): IO[UnauthorisedError, Service] =
    ZIO.fromOption(claim.subject).orElseFail(InvalidServiceToken("missing subject claim")).map(sub => Service(ServiceName(sub)))

  /**
   * Classify a key-source failure: an unknown `kid` is an untrusted token, everything else is infra.
   *
   * @param kid   the key id that was looked up (for the message)
   * @param error the failure the source produced
   * @return `UnauthorisedError` when the key is unknown, otherwise the original `AdapterError` unchanged
   */
  private def untrustedKeyOrForward(kid: String)(error: AdapterError): AdapterError | UnauthorisedError =
    error match
      case _: HttpPublicKeySource.KeyNotFound => InvalidServiceToken(s"unknown signing key: $kid")
      case other                              => other

object JwksServiceAuthenticator:

  /**
   * Build an authenticator over the given key source, with a fresh (empty) key cache.
   *
   * @param source the public-key source tokens are verified against
   * @return a ready authenticator; never fails
   */
  def make(source: PublicKeySource): UIO[JwksServiceAuthenticator] =
    Ref.make(Map.empty[String, PublicKey]).map(new JwksServiceAuthenticator(source, _))

  /**
   * The presented service token could not be accepted — missing/malformed, signed by an unknown key,
   * failing signature or expiry checks, or lacking a subject. `reason` carries the specific cause.
   */
  final case class InvalidServiceToken(reason: String) extends UnauthorisedError:
    override def message: String = s"invalid service token: $reason"

  /** The subset of a JWT header read before verification — just the `kid`. */
  private final case class Header(kid: Option[String]) derives JsonDecoder
