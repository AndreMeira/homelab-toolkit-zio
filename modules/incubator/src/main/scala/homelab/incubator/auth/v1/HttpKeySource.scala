package homelab.incubator.auth.v1


import zio.*
import zio.json.*

import java.math.BigInteger
import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{ HttpClient, HttpRequest }
import java.security.spec.{ EdECPoint, EdECPublicKeySpec, NamedParameterSpec }
import java.security.{ KeyFactory, PublicKey }
import java.util.Base64


/**
 * A working [[KeySource]] that fetches the issuer's JWKS over HTTP (e.g. the registration service's
 * `/.well-known/jwks.json`) and reconstructs Ed25519 public keys from it. Uses the JDK's async
 * `HttpClient` (no extra dependency) and the caching from [[JwksKeySource]].
 *
 * A failed request, a non-2xx status, or a malformed body all surface as
 * [[KeySource.Failure.Unavailable]] — the key source is the infrastructure here.
 */
object HttpKeySource:

  final case class Config(jwksUri: URI, connectTimeout: Duration = 10.seconds, requestTimeout: Duration = 10.seconds)

  /** A caching, HTTP-backed key source. */
  def make(config: Config): UIO[KeySource] =
    ZIO
      .succeed(HttpClient.newBuilder.connectTimeout(config.connectTimeout).build())
      .flatMap(client => JwksKeySource.make(fetch(config, client)))

  private def fetch(config: Config, client: HttpClient): JwksKeySource.FetchAll =
    get(config, client).flatMap(parse)

  private def get(config: Config, client: HttpClient): IO[KeySource.Failure, String] =
    val request = HttpRequest.newBuilder(config.jwksUri).timeout(config.requestTimeout).GET().build()
    ZIO
      .fromCompletableFuture(client.sendAsync(request, BodyHandlers.ofString()))
      .mapError(e => KeySource.Failure.Unavailable(s"could not reach JWKS at ${config.jwksUri}", e))
      .flatMap { response =>
        if response.statusCode() / 100 == 2 then ZIO.succeed(response.body())
        else
          ZIO.fail(
            KeySource.Failure.Unavailable(s"JWKS at ${config.jwksUri} returned HTTP ${response.statusCode()}", HttpError(response.statusCode()))
          )
      }

  private def parse(body: String): IO[KeySource.Failure, Map[String, PublicKey]] =
    ZIO
      .fromEither(body.fromJson[Jwks])
      .mapError(reason => KeySource.Failure.Unavailable(s"malformed JWKS: $reason", MalformedJwks(reason)))
      .flatMap { jwks =>
        ZIO
          .attempt(jwks.keys.collect { case Jwk("OKP", Some("Ed25519"), Some(kid), Some(x)) => kid -> ed25519(x) }.toMap)
          .mapError(e => KeySource.Failure.Unavailable("could not decode JWKS keys", e))
      }

  /**
   * Reconstruct an Ed25519 public key from a JWK `x` (RFC 8037 / 8032): 32 bytes, little-endian y with
   * the sign of x in the MSB of the final byte.
   */
  private def ed25519(x: String): PublicKey =
    val bigEndian = Base64.getUrlDecoder.decode(x).reverse
    val xIsOdd    = (bigEndian(0) & 0x80) != 0
    bigEndian(0) = (bigEndian(0) & 0x7f).toByte
    val point     = EdECPoint(xIsOdd, BigInteger(1, bigEndian))
    KeyFactory.getInstance("Ed25519").generatePublic(EdECPublicKeySpec(NamedParameterSpec.ED25519, point))

  final private case class Jwks(keys: List[Jwk]) derives JsonDecoder
  final private case class Jwk(kty: String, crv: Option[String], kid: Option[String], x: Option[String]) derives JsonDecoder

  final private case class HttpError(status: Int)        extends RuntimeException(s"HTTP $status")
  final private case class MalformedJwks(reason: String) extends RuntimeException(reason)
