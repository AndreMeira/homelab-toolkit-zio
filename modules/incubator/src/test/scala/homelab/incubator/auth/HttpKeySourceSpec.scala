package homelab.incubator.auth


import com.sun.net.httpserver.HttpServer
import homelab.common.types.SignedToken
import homelab.incubator.auth.v1.{ Claims, HttpKeySource, KeySource, RegistrationTokenVerifier }
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim, JwtHeader }
import zio.*
import zio.test.*

import java.net.{ InetSocketAddress, URI }
import java.nio.charset.StandardCharsets.UTF_8
import java.security.KeyPairGenerator
import java.security.interfaces.EdECPublicKey
import java.time.Instant
import java.util.{ Base64, UUID }


object HttpKeySourceSpec extends ZIOSpecDefault:

  private val userId  = UUID.fromString("00000000-0000-0000-0000-00000000000b")
  private val kid     = "reg-key-http"
  private val keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair


  private def sign(name: String): SignedToken =
    val header = JwtHeader(algorithm = Some(JwtAlgorithm.EdDSA), keyId = Some(kid))
    val claim  =
      JwtClaim(content = s"""{"name":"$name"}""", subject = Some(userId.toString), expiration = Some(Instant.now.plusSeconds(3600).getEpochSecond))
    SignedToken(Jwt.encode(header, claim, keyPair.getPrivate))


  /** Encode an Ed25519 public key into its JWK `x` (inverse of HttpKeySource's decode). */
  private def jwkX(pub: EdECPublicKey): String =
    val be  = Array.fill[Byte](32)(0)
    val src = pub.getPoint.getY.toByteArray.dropWhile(_ == 0.toByte)
    java.lang.System.arraycopy(src, 0, be, 32 - src.length, src.length)
    if pub.getPoint.isXOdd then be(0) = (be(0) | 0x80).toByte
    Base64.getUrlEncoder.withoutPadding.encodeToString(be.reverse)


  private val jwksJson =
    s"""{"keys":[{"kty":"OKP","crv":"Ed25519","use":"sig","alg":"EdDSA","kid":"$kid","x":"${jwkX(
        keyPair.getPublic.asInstanceOf[EdECPublicKey]
      )}"}]}"""


  /** Start a local HTTP server serving `body` at `/jwks`; stop it when the scope closes. */
  private def jwksServer(body: String): ZIO[Scope, Throwable, HttpServer] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
        server.createContext(
          "/jwks",
          exchange => {
            val bytes = body.getBytes(UTF_8)
            exchange.sendResponseHeaders(200, bytes.length.toLong)
            val os    = exchange.getResponseBody
            os.write(bytes)
            os.close()
          },
        )
        server.start()
        server
      }
    )(server => ZIO.succeed(server.stop(0)))


  def spec = suite("HttpKeySource")(
    test("fetches JWKS over HTTP and verifies a registration token end-to-end") {
      ZIO.scoped {
        for
          server    <- jwksServer(jwksJson)
          uri        = URI.create(s"http://localhost:${server.getAddress.getPort}/jwks")
          keySource <- HttpKeySource.make(HttpKeySource.Config(uri))
          claims    <- new RegistrationTokenVerifier(keySource).verify(sign("alice"))
        yield assertTrue(claims == Claims(userId.toString, "alice"))
      }
    },
    test("unreachable endpoint → Unavailable") {
      val deadUri = URI.create("http://localhost:1/jwks")
      for
        keySource <- HttpKeySource.make(HttpKeySource.Config(deadUri, connectTimeout = 2.seconds, requestTimeout = 2.seconds))
        exit      <- keySource.publicKey(kid).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[KeySource.Failure.Unavailable]))
    },
  )
