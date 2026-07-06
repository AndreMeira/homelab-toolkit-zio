package homelab.incubator.auth.v2


import zio.json.*
import zio.test.*

import java.security.KeyPairGenerator
import java.security.interfaces.EdECPublicKey
import java.util.Base64


object JsonWebKeySpec extends ZIOSpecDefault:

  private val rsaJwk = """{"use":"sig","kty":"RSA","kid":"k1","alg":"RS256","n":"abc","e":"AQAB"}"""
  private val okpJwk = """{"use":"sig","kty":"OKP","crv":"Ed25519","kid":"reg-1","alg":"EdDSA","x":"xyz"}"""

  // A real RSA-2048 modulus (from the file's example) so reconstruction actually succeeds.
  private val realModulus =
    "nK0KElRi8SXxBTttFepQyoQaF20ayN_aftDX0NZMG-KeCR4McT_0VtzWD0NVauE41s7N5WGimYxNOPHgTsInQY6XI4YxejRjcZboIyZXzyTL3pW-leqXNUUxN2Vl4ScZs61YWU62Cv4IF-hY3phCwhTOIbXi66mQYui-38F7Fe1QtVMtNHgM7ToNh7IJZrc0XWj0ehviuHEuFDsG_hn6NxsVDNvhTHIf0SIld4JgPQ2ywWbtC9URiGBUljRJavQDRY06QXXNSXe5oiX1nRruJyowSwboaOfr1EeWXy7_xgOzNtXQVegwvnXDOY_zInQ7y0xsv6D7s71HZhJzxLvCWQ"

  private def jwkX(pub: EdECPublicKey): String =
    val be  = Array.fill[Byte](32)(0)
    val src = pub.getPoint.getY.toByteArray.dropWhile(_ == 0.toByte)
    java.lang.System.arraycopy(src, 0, be, 32 - src.length, src.length)
    if pub.getPoint.isXOdd then be(0) = (be(0) | 0x80).toByte
    Base64.getUrlEncoder.withoutPadding.encodeToString(be.reverse)

  def spec = suite("JsonWebKey")(
    suite("decoder")(
      test("dispatches RSA by kty")(
        assertTrue(rsaJwk.fromJson[JsonWebKey] == Right(JsonWebKey.RSA("k1", "sig", "RS256", "abc", "AQAB")))
      ),
      test("dispatches OKP by kty")(
        assertTrue(okpJwk.fromJson[JsonWebKey] == Right(JsonWebKey.OKP("reg-1", "sig", "Ed25519", "EdDSA", "xyz")))
      ),
      test("rejects an unsupported kty")(
        assertTrue("""{"kty":"oct","k":"secret"}""".fromJson[JsonWebKey].swap.exists(_.contains("unsupported JWK kty: oct")))
      ),
    ),
    suite("publicKey")(
      test("RSA (RS256) reconstructs an RSA public key") {
        assertTrue(JsonWebKey.RSA("k1", "sig", "RS256", realModulus, "AQAB").publicKey.exists(_.getAlgorithm == "RSA"))
      },
      test("OKP (EdDSA / Ed25519) round-trips a generated key") {
        val pub = KeyPairGenerator.getInstance("Ed25519").generateKeyPair.getPublic.asInstanceOf[EdECPublicKey]
        assertTrue(JsonWebKey.OKP("reg-1", "sig", "Ed25519", "EdDSA", jwkX(pub)).publicKey == Right(pub))
      },
      test("RSA with an unsupported alg → UnsupportedKey") {
        assertTrue(JsonWebKey.RSA("k1", "sig", "RS512", realModulus, "AQAB").publicKey == Left(JsonWebKey.UnsupportedAlgorithm("RS512")))
      },
      test("OKP with a non-Ed25519 curve → UnsupportedKey") {
        assertTrue(JsonWebKey.OKP("reg-1", "sig", "Ed448", "EdDSA", "xyz").publicKey.isLeft)
      },
    ),
  )
