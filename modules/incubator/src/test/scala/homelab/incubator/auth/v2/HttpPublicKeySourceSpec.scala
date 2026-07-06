package homelab.incubator.auth.v2

import com.sun.net.httpserver.HttpServer
import zio.*
import zio.http.Client
import zio.test.*

import java.math.BigInteger
import java.net.{ InetSocketAddress, URI }
import java.nio.charset.StandardCharsets.UTF_8
import java.security.KeyPairGenerator
import java.security.interfaces.RSAPublicKey
import java.util.Base64

object HttpPublicKeySourceSpec extends ZIOSpecDefault:

  private val kid     = "http-rsa-1"
  private val keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair
  private val pub     = keyPair.getPublic.asInstanceOf[RSAPublicKey]

  private def b64url(v: BigInteger): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(v.toByteArray.dropWhile(_ == 0.toByte)) // minimal unsigned big-endian

  private val jwksJson =
    s"""{"keys":[{"use":"sig","kty":"RSA","kid":"$kid","alg":"RS256","n":"${b64url(pub.getModulus)}","e":"${b64url(pub.getPublicExponent)}"}]}"""

  private def jwksServer(body: String): ZIO[Scope, Throwable, HttpServer] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
        server.createContext(
          "/jwks",
          exchange => {
            val bytes = body.getBytes(UTF_8)
            exchange.sendResponseHeaders(200, bytes.length.toLong)
            val os = exchange.getResponseBody
            os.write(bytes)
            os.close()
          },
        )
        server.start()
        server
      }
    )(server => ZIO.succeed(server.stop(0)))

  private def sourceAt(server: HttpServer): URIO[Client, HttpPublicKeySource] =
    ZIO.serviceWith[Client](new HttpPublicKeySource(HttpPublicKeySource.Config(URI.create(s"http://localhost:${server.getAddress.getPort}/jwks")), _))

  def spec = suite("HttpPublicKeySource")(
    test("fetches JWKS and reconstructs the public key for a kid") {
      ZIO.scoped {
        for
          server <- jwksServer(jwksJson)
          source <- sourceAt(server)
          key    <- source.get(kid)
        yield assertTrue(key.getAlgorithm == "RSA", key.asInstanceOf[RSAPublicKey].getModulus == pub.getModulus)
      }
    },
    test("unknown kid → KeyNotFound") {
      ZIO.scoped {
        for
          server <- jwksServer(jwksJson)
          source <- sourceAt(server)
          exit   <- source.get("nope").either
        yield assertTrue(exit.swap.exists(_.isInstanceOf[HttpPublicKeySource.KeyNotFound]))
      }
    },
    test("unreachable endpoint → Unreachable") {
      for
        client <- ZIO.service[Client]
        exit   <- new HttpPublicKeySource(HttpPublicKeySource.Config(URI.create("http://localhost:1/jwks")), client).get(kid).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[HttpPublicKeySource.Unreachable]))
    },
  ).provideShared(Client.default)
