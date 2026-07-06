package homelab.auth

import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*

import java.net.URI
import java.net.http.{HttpClient, HttpRequest}
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.security.cert.CertificateFactory
import javax.net.ssl.{SSLContext, TrustManagerFactory}

/**
 * A [[HttpJwksSource]] for the in-cluster Kubernetes service-account token issuer: fetches the cluster's
 * JWKS from the API server's OIDC endpoint over TLS that trusts the cluster CA, presenting the pod's own
 * service-account token as the bearer (the discovery endpoints require the
 * `system:service-account-issuer-discovery` role, so the call must be authenticated).
 *
 * Build it with [[K8sJwksSource.make]], which bundles the CA-trusting client and the token provider from a
 * [[K8sJwksSource.Config]] — the constructor is private so those pieces can't drift apart.
 *
 * @param uri    the JWKS endpoint
 * @param tokens the provider of the bearer — the pod's rotating service-account token, read per fetch
 * @param client the HTTP client trusting the cluster CA
 */
final class K8sJwksSource private (uri: URI, tokens: JwtProvider, protected val client: HttpClient) extends HttpJwksSource:

  /**
   * The JWKS GET request, presenting a freshly-read service-account token as the bearer.
   *
   * @return the request; fails with the token provider's `AdapterError` if the token can't be read
   */
  protected def request: IO[AdapterError, HttpRequest] =
    tokens.get.map(token => HttpRequest.newBuilder(uri).header("Authorization", s"Bearer $token").GET().build())

object K8sJwksSource:

  /**
   * Where and how to reach the in-cluster issuer — in-pod defaults, overridable for tests or unusual setups.
   *
   * @param jwksUri    the JWKS endpoint (defaults to the in-cluster API server's OIDC JWKS)
   * @param caCertPath path to the cluster CA certificate (defaults to the projected service-account CA)
   * @param tokenPath  path to the projected service-account token (defaults to the projected SA token)
   */
  final case class Config(
    jwksUri: URI = URI.create("https://kubernetes.default.svc/openid/v1/jwks"),
    caCertPath: Path = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"),
    tokenPath: Path = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token"),
  )

  /**
   * Build a source for the in-cluster issuer from `config`: constructs the CA-trusting HTTP client and the
   * projected-token provider, and wires them into the source.
   *
   * @param config where to fetch the JWKS, the CA to trust, and the token to present
   * @return the source; fails with [[CaUnreadable]] if the cluster CA certificate can't be read or parsed
   */
  def make(config: Config): IO[CaUnreadable, K8sJwksSource] =
    ZIO
      .attempt(caTrustingClient(config.caCertPath))
      .mapError(CaUnreadable(config.caCertPath, _))
      .map(client => new K8sJwksSource(config.jwksUri, ProjectedTokenProvider(config.tokenPath), client))

  /**
   * A JDK HTTP client whose TLS trust store is just the CA cert at `caCertPath` — for the in-cluster API
   * server, whose certificate chains to a CA not in the system trust store.
   *
   * @param caCertPath path to a PEM/DER CA certificate
   * @return a client trusting only that CA
   */
  private def caTrustingClient(caCertPath: Path): HttpClient = {
    val file  = Files.newInputStream(caCertPath)
    val cert  = CertificateFactory.getInstance("X.509").generateCertificate(file)
    val store = KeyStore.getInstance(KeyStore.getDefaultType)
    store.load(null, null)
    store.setCertificateEntry("ca", cert)
    val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trust.init(store)
    val ssl   = SSLContext.getInstance("TLS")
    ssl.init(null, trust.getTrustManagers, null)
    HttpClient.newBuilder.sslContext(ssl).build()
  }

  /** The cluster CA certificate at `path` couldn't be read or parsed — the trusting client can't be built. */
  final case class CaUnreadable(path: Path, cause: Throwable) extends ApplicationError:
    override def message: String = s"could not read the cluster CA at $path: ${cause.getMessage}"
