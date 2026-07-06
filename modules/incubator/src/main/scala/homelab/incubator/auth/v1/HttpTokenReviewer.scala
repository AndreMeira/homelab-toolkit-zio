package homelab.incubator.auth.v1


import homelab.common.types.SignedToken
import zio.*
import zio.json.*

import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{ HttpClient, HttpRequest }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Path }
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.{ SSLContext, TrustManagerFactory }


/**
 * A working [[TokenReviewer]] that POSTs an `authentication.k8s.io/v1` TokenReview to the Kubernetes API
 * server and reads back whether the ServiceAccount token authenticated. Uses the JDK `HttpClient`.
 *
 * [[HttpTokenReviewer.inCluster]] builds the in-pod configuration: it reads the pod's own projected SA
 * token (to authenticate the request) and the cluster CA (to trust the API server's TLS), and targets
 * `https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT`. A failed request or a non-2xx status →
 * [[TokenReviewer.Unavailable]].
 */
final class HttpTokenReviewer(config: HttpTokenReviewer.Config, client: HttpClient) extends TokenReviewer:
  import HttpTokenReviewer.*

  def review(token: SignedToken): IO[TokenReviewer.Unavailable, TokenReviewer.Result] =
    val body    = ReviewRequest(spec = ReviewSpec(token, Option.when(config.audiences.nonEmpty)(config.audiences))).toJson
    val request = HttpRequest
      .newBuilder(config.apiServer.resolve("/apis/authentication.k8s.io/v1/tokenreviews"))
      .timeout(config.requestTimeout)
      .header("Authorization", s"Bearer ${config.authToken}")
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    ZIO
      .fromCompletableFuture(client.sendAsync(request, BodyHandlers.ofString()))
      .mapError(e => TokenReviewer.Unavailable("token review request failed", e))
      .flatMap { response =>
        if response.statusCode() / 100 != 2 then
          ZIO.fail(TokenReviewer.Unavailable(s"token review returned HTTP ${response.statusCode()}", HttpError(response.statusCode())))
        else
          ZIO
            .fromEither(response.body().fromJson[ReviewResponse])
            .mapError(reason => TokenReviewer.Unavailable(s"malformed token review response: $reason", MalformedResponse(reason)))
            .map(toResult)
      }


object HttpTokenReviewer:

  final case class Config(apiServer: URI, authToken: String, audiences: List[String] = Nil, requestTimeout: Duration = 10.seconds)

  /** In-cluster wiring: read the pod's SA token + cluster CA and target the API server over TLS. */
  def inCluster(audiences: List[String] = Nil): Task[HttpTokenReviewer] =
    for
      token  <- readFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
      caPem  <- readFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
      client <- ZIO.attempt(tlsClient(caPem))
      host    = sys.env.getOrElse("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
      port    = sys.env.getOrElse("KUBERNETES_SERVICE_PORT", "443")
    yield new HttpTokenReviewer(Config(URI.create(s"https://$host:$port"), token.trim, audiences), client)

  private def readFile(path: String): Task[String] =
    ZIO.attemptBlocking(Files.readString(Path.of(path)))

  private def tlsClient(caPem: String): HttpClient =
    val cert     = CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(caPem.getBytes(UTF_8)))
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(null, null)
    keyStore.setCertificateEntry("kubernetes-ca", cert)
    val trust    = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trust.init(keyStore)
    val ssl      = SSLContext.getInstance("TLS")
    ssl.init(null, trust.getTrustManagers, null)
    HttpClient.newBuilder.sslContext(ssl).build()

  private def toResult(response: ReviewResponse): TokenReviewer.Result =
    val authenticated = response.status.flatMap(_.authenticated).getOrElse(false)
    val username      = response.status.flatMap(_.user).flatMap(_.username)
    TokenReviewer.Result(authenticated, if authenticated then username else None)

  final private case class ReviewRequest(apiVersion: String = "authentication.k8s.io/v1", kind: String = "TokenReview", spec: ReviewSpec)
      derives JsonEncoder

  final private case class ReviewSpec(token: String, audiences: Option[List[String]]) derives JsonEncoder
  final private case class ReviewResponse(status: Option[ReviewStatus]) derives JsonDecoder
  final private case class ReviewStatus(authenticated: Option[Boolean], user: Option[ReviewUser]) derives JsonDecoder
  final private case class ReviewUser(username: Option[String]) derives JsonDecoder

  final private case class HttpError(status: Int)            extends RuntimeException(s"HTTP $status")
  final private case class MalformedResponse(reason: String) extends RuntimeException(reason)
