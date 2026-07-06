package homelab.auth

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.types.SignedToken
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim, JwtHeader }
import zio.*
import zio.test.*

import java.math.BigInteger
import java.security.interfaces.{ EdECPublicKey, RSAPublicKey }
import java.security.{ KeyPairGenerator, PrivateKey }
import java.time.Instant
import java.util.Base64

object JwksTokenVerifierSpec extends ZIOSpecDefault:

  private val subject = "system:serviceaccount:default:demo"
  private val edKid   = "ed-1"
  private val rsaKid  = "rsa-1"

  private val edPair  = KeyPairGenerator.getInstance("Ed25519").generateKeyPair
  private val rsaPair = KeyPairGenerator.getInstance("RSA").generateKeyPair
  private val otherEd = KeyPairGenerator.getInstance("Ed25519").generateKeyPair // signs a forgery under edKid

  // --- publish the two public keys as a JWKS -----------------------------------------------------

  private def b64url(v: BigInteger): String =
    Base64.getUrlEncoder.withoutPadding.encodeToString(v.toByteArray.dropWhile(_ == 0.toByte))

  private def jwkX(pub: EdECPublicKey): String =
    val be  = Array.fill[Byte](32)(0)
    val src = pub.getPoint.getY.toByteArray.dropWhile(_ == 0.toByte)
    java.lang.System.arraycopy(src, 0, be, 32 - src.length, src.length)
    if pub.getPoint.isXOdd then be(0) = (be(0) | 0x80).toByte
    Base64.getUrlEncoder.withoutPadding.encodeToString(be.reverse)

  private val edJwk = JsonWebKey.OKP(edKid, "sig", "Ed25519", "EdDSA", jwkX(edPair.getPublic.asInstanceOf[EdECPublicKey]))
  private val rsaJwk =
    val p = rsaPair.getPublic.asInstanceOf[RSAPublicKey]
    JsonWebKey.RSA(rsaKid, "sig", "RS256", b64url(p.getModulus), b64url(p.getPublicExponent))
  private val jwks = JsonWebKey.Set(List(edJwk, rsaJwk))

  // --- issue tokens ------------------------------------------------------------------------------

  private def claim(expiresInSeconds: Long): JwtClaim =
    JwtClaim(subject = Some(subject), expiration = Some(Instant.now.plusSeconds(expiresInSeconds).getEpochSecond))

  private def token(kid: String, algorithm: JwtAlgorithm, key: PrivateKey, claim: JwtClaim): SignedToken =
    SignedToken(Jwt.encode(JwtHeader(algorithm = Some(algorithm), keyId = Some(kid)), claim, key))

  private val edToken      = token(edKid, JwtAlgorithm.EdDSA, edPair.getPrivate, claim(3600))
  private val rsaToken     = token(rsaKid, JwtAlgorithm.RS256, rsaPair.getPrivate, claim(3600))
  private val expiredToken = token(edKid, JwtAlgorithm.EdDSA, edPair.getPrivate, claim(-3600))
  private val unknownKid   = token("nope", JwtAlgorithm.EdDSA, edPair.getPrivate, claim(3600))
  private val forgedToken  = token(edKid, JwtAlgorithm.EdDSA, otherEd.getPrivate, claim(3600))

  // --- an in-memory source that counts how often it's consulted ----------------------------------

  private def verifierCounting(counter: Ref[Int]): UIO[JwksTokenVerifier] =
    JwksTokenVerifier.make(new JwksSource:
      def all: IO[AdapterError, JsonWebKey.Set] = counter.update(_ + 1).as(jwks))

  private def verifier: UIO[JwksTokenVerifier] =
    Ref.make(0).flatMap(verifierCounting)

  def spec = suite("JwksTokenVerifier")(
    test("verifies an EdDSA token and returns its claims") {
      for
        v     <- verifier
        claim <- v.verify(edToken)
      yield assertTrue(claim.subject.contains(subject))
    },
    test("verifies an RS256 token and returns its claims") {
      for
        v     <- verifier
        claim <- v.verify(rsaToken)
      yield assertTrue(claim.subject.contains(subject))
    },
    test("a token signed by a different key → UntrustedToken") {
      for
        v    <- verifier
        exit <- v.verify(forgedToken).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.UntrustedToken]))
    },
    test("an expired token → UntrustedToken") {
      for
        v    <- verifier
        exit <- v.verify(expiredToken).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.UntrustedToken]))
    },
    test("an unknown kid → UnknownKey") {
      for
        v    <- verifier
        exit <- v.verify(unknownKid).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.UnknownKey]))
    },
    test("a token whose header isn't valid → MalformedToken") {
      for
        v    <- verifier
        exit <- v.verify(SignedToken("not-a-jwt")).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.MalformedToken]))
    },
    test("caches the reconstructed key: the source is consulted once across repeated verifies") {
      for
        counter <- Ref.make(0)
        v       <- verifierCounting(counter)
        _       <- v.verify(edToken)
        _       <- v.verify(edToken)
        hits    <- counter.get
      yield assertTrue(hits == 1)
    },
  )
