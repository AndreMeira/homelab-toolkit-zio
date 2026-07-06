package homelab.auth


import homelab.common.error.ApplicationError.{ AdapterError, DecodingError, TransientError }
import homelab.auth.HttpJwksSource.*
import zio.*
import zio.json.*

import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{ HttpClient, HttpRequest }


/**
 * Abstract base for a [[JwksSource]] served over HTTP with the JDK's `java.net.http.HttpClient`.
 *
 * A concrete source supplies only the [[client]] and, crucially, the [[request]] to send — everything
 * else (dispatch, status check, body read, JWKS decode) is handled here, so a new variant is just a
 * different [[request]]: unauthenticated for a public issuer, or carrying a freshly-fetched bearer for
 * the in-cluster Kubernetes issuer. [[request]] is effectful precisely so an implementation can pull that
 * rotating credential (e.g. a service-account token from a `JwtProvider`) per fetch, rather than freezing
 * it in a config.
 *
 * Failures all narrow to `AdapterError`: the request failing to reach the endpoint → [[Unreachable]]
 * (retryable), a non-2xx status → [[BadStatus]], a body that isn't a valid JWKS → [[JwksDecodingFailed]].
 */
trait HttpJwksSource extends JwksSource:

  /**
   * The HTTP client used to send [[request]] — a plain client for public issuers, or one trusting a
   * specific CA (as [[K8sJwksSource]] does) for the in-cluster issuer.
   *
   * @return the client to send with
   */
  protected def client: HttpClient

  /**
   * The JWKS request to send — the single extension point. Effectful so an implementation can attach a
   * freshly-fetched, rotating credential (e.g. a service-account token from a `JwtProvider`) each time.
   *
   * @return the request to send; fails with `AdapterError` if building it (e.g. reading a token) fails
   */
  protected def request: IO[AdapterError, HttpRequest]

  /**
   * Build the request, send it, and decode the response body into the key set.
   *
   * @return the [[JsonWebKey.Set]]; fails with whatever [[request]] fails with, [[Unreachable]] if the
   *         endpoint can't be reached, [[BadStatus]] on a non-2xx status, or [[JwksDecodingFailed]] if the
   *         body isn't a valid JWKS
   */
  final override def all: IO[AdapterError, JsonWebKey.Set] =
    for
      req  <- request
      body <- send(req)
      set  <- ZIO.fromEither(body.fromJson[JsonWebKey.Set]).mapError(JwksDecodingFailed(_))
    yield set

  /**
   * Send `req` and return the response body, failing on a non-2xx status.
   *
   * @param req the request to send
   * @return the response body; fails with [[Unreachable]] if the request errors, or [[BadStatus]] if the status isn't 2xx
   */
  private def send(req: HttpRequest): IO[AdapterError, String] =
    ZIO
      .fromCompletableFuture(client.sendAsync(req, BodyHandlers.ofString()))
      .mapError(Unreachable(req.uri(), _))
      .flatMap: response =>
        if response.statusCode() / 100 == 2 then ZIO.succeed(response.body())
        else ZIO.fail(BadStatus(req.uri(), response.statusCode()))


object HttpJwksSource:

  /**
   * A basic source that GETs `uri` unauthenticated with a default client — for a public JWKS endpoint
   * whose TLS certificate chains to a CA in the system trust store.
   *
   * @param uri the JWKS endpoint
   * @return a ready source
   */
  def make(uri: URI): HttpJwksSource = new HttpJwksSource:
    protected val client: HttpClient                     = HttpClient.newHttpClient()
    protected def request: IO[AdapterError, HttpRequest] =
      ZIO.succeed(HttpRequest.newBuilder(uri).GET().build())

  /** Marker for every failure this source can produce (all `AdapterError`). */
  sealed trait Error extends AdapterError

  /** The request failed to reach the endpoint (connection/read error) — retryable infrastructure. */
  final case class Unreachable(uri: URI, cause: Throwable) extends Error, TransientError:
    override def message: String = s"could not reach JWKS at $uri: ${cause.getMessage}"

  /** The JWKS endpoint answered with a non-2xx status. */
  final case class BadStatus(uri: URI, status: Int) extends Error:
    override def message: String = s"JWKS at $uri returned HTTP $status"

  /** The response body wasn't a valid JWKS — both a decoding and an adapter failure. */
  final case class JwksDecodingFailed(reason: String) extends Error, DecodingError:
    override def message: String = s"could not decode JWKS: $reason"
