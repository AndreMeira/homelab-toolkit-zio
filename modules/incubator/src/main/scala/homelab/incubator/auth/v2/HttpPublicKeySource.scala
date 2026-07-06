package homelab.incubator.auth.v2

import HttpPublicKeySource.*
import homelab.common.error.ApplicationError.{ AdapterError, DecodingError, TransientError }
import zio.*
import zio.http.*
import zio.http.netty.NettyConfig
import zio.http.netty.client.NettyClientDriver
import zio.json.*

import java.net.URI
import java.security.PublicKey

/**
 * A [[PublicKeySource]] that fetches a JWKS over HTTP (ZIO HTTP `Client.batched`), decodes it into a
 * [[JsonWebKey.Set]], selects the key by `kid`, and reconstructs its `PublicKey` via
 * [[JsonWebKey.publicKey]]. The endpoint and an optional bearer credential come from a [[Config]].
 *
 * The `Client` is captured at construction because the port's `IO[AdapterError, …]` has environment
 * `Any` (it can't require a `Client`). Failures all narrow to `AdapterError`: an unreachable endpoint /
 * bad status → [[Unreachable]] (retryable), a body or key that won't decode → [[JwksDecodingFailed]]
 * (also a `DecodingError`), an absent `kid` → [[KeyNotFound]].
 */
final class HttpPublicKeySource(config: HttpPublicKeySource.Config, client: Client) extends PublicKeySource:

  private val batched = client.batched

  /**
   * Fetch the JWKS, decode it, and return the public key for `keyId`.
   *
   * @param keyId the `kid` to resolve
   * @return the public key; fails with [[Unreachable]] if the endpoint can't be reached or returns a
   *         non-2xx status, [[JwksDecodingFailed]] if the body or a key can't be decoded, or
   *         [[KeyNotFound]] if no JWK carries that `kid`
   */
  def get(keyId: String): IO[AdapterError, PublicKey] =
    for
      response <- fetch
      body     <- response.body.asString.mapError(badResponseBody(config.uri, _))
      set      <- ZIO.fromEither(body.fromJson[JsonWebKey.Set]).mapError(JwksDecodingFailed(_))
      jwk      <- ZIO.fromOption(set.keys.find(_.keyId == keyId)).orElseFail(KeyNotFound(keyId))
      key      <- ZIO.fromEither(jwk.publicKey).mapError(e => JwksDecodingFailed(e.message))
    yield key

  /**
   * GET the JWKS endpoint, failing on a non-2xx status.
   *
   * @return the successful response; fails with [[Unreachable]] if the request errors or the status isn't 2xx
   */
  private def fetch: IO[Unreachable, Response] = batched
    .request(jwksRequest)
    .mapError(e => Unreachable(s"could not reach JWKS at ${config.uri}", e))
    .flatMap { response =>
      if response.status.isSuccess then ZIO.succeed(response)
      else ZIO.fail(badStatus(config.uri, response))
    }

  /**
   * The JWKS GET request, carrying the configured bearer credential when one is set.
   *
   * @return the request to send
   */
  private def jwksRequest: Request =
    val base = Request.get(config.uri.toString)
    config.bearer.fold(base)(token => base.addHeader("Authorization", s"Bearer $token"))

object HttpPublicKeySource:

  /**
   * Where and how to fetch the JWKS.
   *
   * @param uri    the JWKS endpoint (e.g. an OIDC `jwks_uri`)
   * @param bearer an optional bearer credential — required by endpoints behind auth (e.g. the in-cluster
   *               Kubernetes issuer), omitted for public ones
   */
  final case class Config(uri: URI, bearer: Option[String] = None)

  /** The in-pod path of the cluster CA that signs the Kubernetes API server's TLS certificate. */
  val ClusterCaPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

  /** A ZIO HTTP `Client` whose TLS trust store is just the cluster CA (from [[ClusterCaPath]]). */
  private val clusterCaClient: ZLayer[Any, Throwable, Client] =
    ZLayer.make[Client](
      ZLayer.succeed(ZClient.Config.default.ssl(ClientSSLConfig.FromCertFile(ClusterCaPath))),
      ZLayer.succeed(NettyConfig.default),
      DnsResolver.default,
      NettyClientDriver.live,
      Client.customized,
    )

  /**
   * An [[HttpPublicKeySource]] over `config`'s JWKS endpoint, using a `Client` that trusts the Kubernetes
   * cluster CA (from the pod's projected service-account volume, [[ClusterCaPath]]) — for the in-cluster issuer.
   *
   * @param config where to fetch the JWKS, and the bearer credential to present
   * @return a layer producing a ready source; fails with `Throwable` if the CA client can't be built
   */
  def layer(config: Config): ZLayer[Any, Throwable, HttpPublicKeySource] =
    clusterCaClient >>> ZLayer.fromFunction((client: Client) => new HttpPublicKeySource(config, client))

  /**
   * Turn a non-2xx response into an [[Unreachable]] failure.
   *
   * @param uri      the endpoint that was called
   * @param response the response whose status wasn't 2xx
   * @return the [[Unreachable]] error carrying the status code
   */
  def badStatus(uri: URI, response: Response): Unreachable =
    Unreachable(s"JWKS at $uri returned HTTP ${response.status.code}", StatusError(response.status.code))

  /**
   * Turn a body-read failure into an [[Unreachable]] failure.
   *
   * @param uri the endpoint that was called
   * @param err the underlying read error
   * @return the [[Unreachable]] error wrapping `err`
   */
  def badResponseBody(uri: URI, err: Throwable): Unreachable =
    Unreachable(s"could not read JWKS body from $uri", err)

  /** The JWKS endpoint couldn't be reached, or returned a non-2xx status — retryable infrastructure. */
  final case class Unreachable(reason: String, cause: Throwable) extends AdapterError, TransientError:
    override def message: String = reason

  /** The requested `kid` isn't present in the fetched key set. */
  final case class KeyNotFound(keyId: String) extends AdapterError:
    override def message: String = s"no JWK with kid '$keyId' in the key set"

  /** The JWKS body — or one of its keys — couldn't be decoded: both a decoding and an adapter failure. */
  final case class JwksDecodingFailed(reason: String) extends DecodingError, AdapterError:
    override def message: String = s"could not decode JWKS: $reason"

  final private case class StatusError(status: Int) extends RuntimeException(s"HTTP $status")
