package homelab.auth


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import homelab.auth.JwksTokenVerifier.*
import pdi.jwt.{ JwtAlgorithm, JwtClaim, JwtZIOJson }
import zio.*
import zio.json.*

import java.nio.charset.StandardCharsets.UTF_8
import java.security.PublicKey
import java.util.Base64
import scala.util.Try


/**
 * A [[TokenVerifier]] backed by a [[JwksSource]]: reads the token's `kid`, resolves the matching JWK from
 * the source, reconstructs its public key ([[PublicKeyDecoder]]), and verifies the token's signature and
 * expiry against it — returning the raw claims on success.
 *
 * It does no claim → principal mapping and no `aud`/`iss` checks; that's a higher-level authenticator's
 * job. Supported signing algorithms are EdDSA (our own registration issuer) and RS256 (Kubernetes
 * service-account tokens); the key type resolved by `kid` determines which actually verifies.
 *
 * Reconstructed public keys are cached by `kid`, so `KeyFactory` runs once per signing key rather than on
 * every request (the JWKS fetch itself is cached upstream by the source).
 *
 * A bad token (unreadable header, unknown `kid`, invalid signature or expiry) fails with an
 * `UnauthorisedError`; only a broken *published* key ([[KeyUnusable]]) or the source failing to fetch is
 * an `AdapterError`.
 *
 * @param source the JWKS the signing keys are drawn from
 * @param cache  reconstructed public keys, keyed by `kid`
 */
final class JwksTokenVerifier private (source: JwksSource, cache: Ref[Map[String, PublicKey]]) extends TokenVerifier:

  /**
   * Verify `token` against the source's keys.
   *
   * @param token the signed token to verify
   * @return the verified claims; fails with [[MalformedToken]] if the header/`kid` can't be read,
   *         [[UnknownKey]] if no JWK matches the `kid`, [[UntrustedToken]] if the signature or expiry check
   *         fails (all `UnauthorisedError`), [[KeyUnusable]] if the JWK can't be turned into a key, or the
   *         source's own `AdapterError` if the keys can't be fetched
   */
  def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] =
    for
      kid   <- keyId(token)
      key   <- publicKey(kid)
      claim <- decode(token, key)
    yield claim

  /**
   * Read the `kid` from the token's header segment, decoded as JSON — cheaper than a full JWT decode, and
   * this is the only place the header is inspected.
   *
   * @param token the token whose header to read
   * @return the `kid`; fails with [[MalformedToken]] if the header segment can't be decoded or carries no `kid`
   */
  private def keyId(token: SignedToken): IO[UnauthorisedError, String] =
    ZIO.fromEither(headerKeyId(token)).mapError(MalformedToken(_))

  /**
   * Pull the `kid` out of the token's first (header) segment: base64url-decode it and read the `kid` field.
   *
   * @param token the token to inspect
   * @return the `kid`, or a reason it couldn't be read
   */
  private def headerKeyId(token: SignedToken): Either[String, String] =
    for
      segment <- token.split('.').headOption.toRight("token has no header segment")
      json    <- Try(String(Base64.getUrlDecoder.decode(segment), UTF_8)).toEither.left
                   .map(_ => "header isn't valid base64url")
      header  <- json.fromJson[Header]
      kid     <- header.kid.toRight("token header has no kid")
    yield kid

  /**
   * The public key for `keyId`, cached: served from the cache if present, otherwise fetched from the
   * source, reconstructed, and cached. Two concurrent misses may reconstruct the same key twice — harmless,
   * since reconstruction is deterministic and the last write wins.
   *
   * @param keyId the `kid` to resolve a key for
   * @return the public key; fails with [[UnknownKey]] if no JWK matches, [[KeyUnusable]] if it can't be
   *         reconstructed, or the source's own `AdapterError` if the keys can't be fetched
   */
  private def publicKey(keyId: String): IO[AdapterError | UnauthorisedError, PublicKey] =
    cache.get.map(_.get(keyId)).flatMap {
      case Some(key) => ZIO.succeed(key)
      case None      =>
        for
          jwk <- jwkFor(keyId)
          key <- reconstruct(jwk)
          _   <- cache.update(_.updated(keyId, key))
        yield key
    }

  /**
   * Resolve the JWK the token was signed with.
   *
   * @param keyId the `kid` to look up
   * @return the matching JWK; fails with [[UnknownKey]] if the source has none, or the source's own
   *         `AdapterError` if the keys can't be fetched
   */
  private def jwkFor(keyId: String): IO[AdapterError | UnauthorisedError, JsonWebKey] =
    source.get(keyId).flatMap {
      case Some(jwk) => ZIO.succeed(jwk)
      case None      => ZIO.fail(UnknownKey(keyId))
    }

  /**
   * Reconstruct the public key from a JWK.
   *
   * @param jwk the JWK to reconstruct
   * @return the public key; fails with [[KeyUnusable]] if the key material is unsupported or corrupt
   */
  private def reconstruct(jwk: JsonWebKey): IO[AdapterError, PublicKey] =
    ZIO.fromEither(PublicKeyDecoder.decode(jwk)).mapError(failure => KeyUnusable(failure.message))

  /**
   * Verify the token's signature and expiry against `key`, returning its claims.
   *
   * @param token the token to verify
   * @param key   the public key to verify against
   * @return the verified claims; fails with [[UntrustedToken]] if the signature is invalid or the token has expired
   */
  private def decode(token: SignedToken, key: PublicKey): IO[UnauthorisedError, JwtClaim] =
    ZIO
      .fromTry(JwtZIOJson.decode(token, key, Seq(JwtAlgorithm.EdDSA, JwtAlgorithm.RS256)))
      .mapError(err => UntrustedToken(err.getMessage))


object JwksTokenVerifier:

  /**
   * A verifier over `source` with a fresh, empty key cache.
   *
   * @param source the JWKS the signing keys are drawn from
   * @return the verifier
   */
  def make(source: JwksSource): UIO[JwksTokenVerifier] =
    Ref.make(Map.empty[String, PublicKey]).map(new JwksTokenVerifier(source, _))

  /** Just enough of a JWT header to route to a signing key — its `kid`. */
  final private case class Header(kid: Option[String]) derives JsonDecoder

  /** Marker for every failure this verifier can raise itself (source failures pass through unchanged). */
  sealed trait Failure extends ApplicationError

  /** The token header couldn't be decoded, or carries no `kid` — there's nothing to look a key up by. */
  final case class MalformedToken(reason: String) extends Failure, UnauthorisedError:
    override def message: String = s"malformed token: $reason"

  /** No JWK in the source carries the token's `kid` — signed by a key we don't publish or trust. */
  final case class UnknownKey(keyId: String) extends Failure, UnauthorisedError:
    override def message: String = s"no JWK with kid '$keyId'"

  /** The signature or expiry check failed — the token is invalid. */
  final case class UntrustedToken(reason: String) extends Failure, UnauthorisedError:
    override def message: String = s"token failed verification: $reason"

  /** A JWK was found but couldn't be turned into a public key (unsupported or corrupt key material). */
  final case class KeyUnusable(reason: String) extends Failure, AdapterError:
    override def message: String = s"the token's key is unusable: $reason"
