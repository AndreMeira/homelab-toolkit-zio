package homelab.incubator.auth


import homelab.common.auth.Requester.User
import homelab.common.types.{ SignedToken, UserId, UserName }
import homelab.incubator.auth.v1.{ Claims, JwksKeySource, JwtUserAuthenticator, KeySource, RegistrationTokenVerifier, TokenVerifier }
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim, JwtHeader }
import zio.*
import zio.test.*

import java.security.{ KeyPairGenerator, PrivateKey, PublicKey }
import java.time.Instant
import java.util.UUID


object RegistrationTokenVerifierSpec extends ZIOSpecDefault:

  private val userId  = UUID.fromString("00000000-0000-0000-0000-00000000000a")
  private val kid     = "reg-key-1"
  private val keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair
  private val jwks    = Map(kid -> keyPair.getPublic)


  /** The registration "issuer" side of the round-trip: sign an EdDSA token with a `kid` header. */
  private def sign(signingKey: PrivateKey, keyId: String, name: String, expiresInSeconds: Long = 3600): SignedToken =
    val header = JwtHeader(algorithm = Some(JwtAlgorithm.EdDSA), keyId = Some(keyId))
    val claim  = JwtClaim(
      content = s"""{"name":"$name"}""",
      subject = Some(userId.toString),
      expiration = Some(Instant.now.plusSeconds(expiresInSeconds).getEpochSecond),
    )
    SignedToken(Jwt.encode(header, claim, signingKey))


  private def verifierWith(fetch: JwksKeySource.FetchAll): UIO[RegistrationTokenVerifier] =
    JwksKeySource.make(fetch).map(new RegistrationTokenVerifier(_))


  def spec = suite("RegistrationTokenVerifier")(
    test("a registration-issued token verifies to the user claims") {
      for
        verifier <- verifierWith(ZIO.succeed(jwks))
        claims   <- verifier.verify(sign(keyPair.getPrivate, kid, "alice"))
      yield assertTrue(claims == Claims(userId.toString, "alice"))
    },
    test("end-to-end via UserAuthenticator → Authenticated") {
      for
        verifier <- verifierWith(ZIO.succeed(jwks))
        who      <- JwtUserAuthenticator(verifier).authenticate(sign(keyPair.getPrivate, kid, "alice"))
      yield assertTrue(who == User.Authenticated(UserId(userId), UserName("alice")))
    },
    test("unknown kid → Invalid (untrusted)") {
      for
        verifier <- verifierWith(ZIO.succeed(Map.empty[String, PublicKey]))
        exit     <- verifier.verify(sign(keyPair.getPrivate, kid, "alice")).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[TokenVerifier.Failure.Invalid]))
    },
    test("token signed by a different key → Invalid (bad signature)") {
      val other = KeyPairGenerator.getInstance("Ed25519").generateKeyPair
      for
        verifier <- verifierWith(ZIO.succeed(jwks)) // JWKS has the real key; token signed by `other`
        exit     <- verifier.verify(sign(other.getPrivate, kid, "alice")).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[TokenVerifier.Failure.Invalid]))
    },
    test("JWKS unreachable → Unavailable") {
      for
        verifier <- verifierWith(ZIO.fail(KeySource.Failure.Unavailable("jwks down", new RuntimeException("boom"))))
        exit     <- verifier.verify(sign(keyPair.getPrivate, kid, "alice")).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[TokenVerifier.Failure.Unavailable]))
    },
  )
