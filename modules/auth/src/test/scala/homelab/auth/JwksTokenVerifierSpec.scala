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
import java.lang.System as JavaSystem


object JwksTokenVerifierSpec extends ZIOSpecDefault:
  import Support.*

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

  /** Two key pairs, the JWKS publishing them, the tokens they sign, and a source that counts its callers. */
  private object Support {

    val subject = "system:serviceaccount:inmemory:demo"
    val edKid   = "ed-1"
    val rsaKid  = "rsa-1"

    val edPair  = KeyPairGenerator.getInstance("Ed25519").generateKeyPair
    val rsaPair = KeyPairGenerator.getInstance("RSA").generateKeyPair
    val otherEd = KeyPairGenerator.getInstance("Ed25519").generateKeyPair // signs a forgery under edKid

    // --- publish the two public keys as a JWKS -----------------------------------------------------

    def b64url(v: BigInteger): String =
      Base64.getUrlEncoder.withoutPadding.encodeToString(v.toByteArray.dropWhile(_ == 0.toByte))

    def jwkX(pub: EdECPublicKey): String =
      val be  = Array.fill[Byte](32)(0)
      val src = pub.getPoint.getY.toByteArray.dropWhile(_ == 0.toByte)
      JavaSystem.arraycopy(src, 0, be, 32 - src.length, src.length)
      if pub.getPoint.isXOdd then be(0) = (be(0) | 0x80).toByte
      Base64.getUrlEncoder.withoutPadding.encodeToString(be.reverse)

    val edJwk = edPair.getPublic match
      case key: EdECPublicKey => JsonWebKey.OKP(edKid, "sig", "Ed25519", "EdDSA", jwkX(key))
      case other              => JsonWebKey.OKP(edKid, "sig", "Ed25519", "EdDSA", s"not an Ed25519 key: $other")

    val rsaJwk = rsaPair.getPublic match
      case key: RSAPublicKey => JsonWebKey.RSA(rsaKid, "sig", "RS256", b64url(key.getModulus), b64url(key.getPublicExponent))
      case other             => JsonWebKey.RSA(rsaKid, "sig", "RS256", s"not an RSA key: $other", "")

    val jwks = JsonWebKey.Set(Chunk(edJwk, rsaJwk))

    // --- issue tokens ------------------------------------------------------------------------------

    def claim(expiresInSeconds: Long): JwtClaim =
      JwtClaim(subject = Some(subject), expiration = Some(Instant.now.plusSeconds(expiresInSeconds).getEpochSecond))

    def token(kid: String, algorithm: JwtAlgorithm, key: PrivateKey, claim: JwtClaim): SignedToken =
      SignedToken(Jwt.encode(JwtHeader(algorithm = Some(algorithm), keyId = Some(kid)), claim, key))

    val edToken      = token(edKid, JwtAlgorithm.EdDSA, edPair.getPrivate, claim(3600))
    val rsaToken     = token(rsaKid, JwtAlgorithm.RS256, rsaPair.getPrivate, claim(3600))
    val expiredToken = token(edKid, JwtAlgorithm.EdDSA, edPair.getPrivate, claim(-3600))
    val unknownKid   = token("nope", JwtAlgorithm.EdDSA, edPair.getPrivate, claim(3600))
    val forgedToken  = token(edKid, JwtAlgorithm.EdDSA, otherEd.getPrivate, claim(3600))

    // --- an in-memory source that counts how often it's consulted ----------------------------------

    def verifierCounting(counter: Ref[Int]): UIO[JwksTokenVerifier] = JwksTokenVerifier.make:
      new JwksSource:
        def all: IO[AdapterError, JsonWebKey.Set] = counter.update(_ + 1).as(jwks)

    def verifier: UIO[JwksTokenVerifier] =
      Ref.make(0).flatMap(verifierCounting)
  }
