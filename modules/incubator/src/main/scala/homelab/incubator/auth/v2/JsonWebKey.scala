package homelab.incubator.auth.v2


import homelab.common.error.ApplicationError.DecodingError
import homelab.incubator.auth.v2.JsonWebKey.{ KeyGenerationFailed, UnsupportedAlgorithm }
import zio.json.*
import zio.json.ast.Json

import java.math.BigInteger
import java.security.spec.{ RSAPublicKeySpec, X509EncodedKeySpec }
import java.security.{ KeyFactory, PublicKey }
import java.util.Base64
import scala.util.Try


/*
k8s JWK
{
  "keys": [
    {
      "use": "sig",
      "kty": "RSA",
      "kid": "LK1WcfTeTxX25FPRSv0icquzwcao3M_Xu_6YXneeHHI",
      "alg": "RS256",
      "n": "nK0KElRi8SXxBTttFepQyoQaF20ayN_aftDX0NZMG-KeCR4McT_0VtzWD0NVauE41s7N5WGimYxNOPHgTsInQY6XI4YxejRjcZboIyZXzyTL3pW-leqXNUUxN2Vl4ScZs61YWU62Cv4IF-hY3phCwhTOIbXi66mQYui-38F7Fe1QtVMtNHgM7ToNh7IJZrc0XWj0ehviuHEuFDsG_hn6NxsVDNvhTHIf0SIld4JgPQ2ywWbtC9URiGBUljRJavQDRY06QXXNSXe5oiX1nRruJyowSwboaOfr1EeWXy7_xgOzNtXQVegwvnXDOY_zInQ7y0xsv6D7s71HZhJzxLvCWQ",
      "e": "AQAB"
    }
  ]
}

And the payload of a projected SA token (so you see where the identity comes from):
{
  "iss": "https://kubernetes.default.svc.cluster.local",
  "sub": "system:serviceaccount:homelab:registration",
  "aud": ["https://kubernetes.default.svc"],
  "exp": 1751800000, "iat": 1751796400, "nbf": 1751796400,
  "kubernetes.io": {
    "namespace": "homelab",
    "serviceaccount": { "name": "registration", "uid": "8f3b…" },
    "pod":            { "name": "registration-6c9df-abc12", "uid": "1a2b…" }
  }
}

// -------------------------------------

{
  "use":"sig",
  "kty":"OKP",
  "crv":"Ed25519",
  "kid":"reg-key-1",
  "alg":"EdDSA",
  "x":"11qYAY…32-byte-base64url"
}
 */


sealed trait JsonWebKey:

  /** the used algorithm */
  def algorithm: String = this match
    case okp: JsonWebKey.OKP => okp.alg
    case rsa: JsonWebKey.RSA => rsa.alg


  def keyId: String = this match
    case okp: JsonWebKey.OKP => okp.kid
    case rsa: JsonWebKey.RSA => rsa.kid


  /** Reconstruct the java `PublicKey`, or a [[DecodingError]] if the key's algorithm is unsupported. */
  def publicKey: Either[DecodingError, PublicKey] = this match
    case okp: JsonWebKey.OKP if okp.alg == "EdDSA" & okp.crv == "Ed25519" => ed25519(okp)
    case rsa: JsonWebKey.RSA if rsa.alg == "RS256"                        => rsaKey(rsa)
    case other                                                            => unsupported(other)


  /**
   * OKP → Ed25519 (`EdDSA`): the JWK `x` is the raw 32-byte public key. Wrap it in the fixed Ed25519
   * SubjectPublicKeyInfo DER header and let `KeyFactory` decode the point — no manual y / x-sign parsing.
   */
  private def ed25519(key: JsonWebKey.OKP): Either[DecodingError, PublicKey] = Try {
    val spkiHeader = Array(0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00).map(_.toByte)
    val der        = spkiHeader ++ Base64.getUrlDecoder.decode(key.x)
    KeyFactory.getInstance("Ed25519").generatePublic(X509EncodedKeySpec(der))
  }.toEither.left.map(e => KeyGenerationFailed(e.getMessage))


  /** RSA → `RS256`: `n` (modulus) and `e` (exponent) are base64url big-endian unsigned integers. */
  private def rsaKey(key: JsonWebKey.RSA): Either[DecodingError, PublicKey] = Try {
    val modulus  = BigInteger(1, Base64.getUrlDecoder.decode(key.n))
    val exponent = BigInteger(1, Base64.getUrlDecoder.decode(key.e))
    KeyFactory.getInstance("RSA").generatePublic(RSAPublicKeySpec(modulus, exponent))
  }.toEither.left.map(e => KeyGenerationFailed(e.getMessage))


  /** Unsupported  */
  private def unsupported(key: JsonWebKey): Either[DecodingError, PublicKey] =
    Left(UnsupportedAlgorithm(key.algorithm))


object JsonWebKey:
  case class KeyType(kty: String) derives JsonDecoder
  case class Set(keys: List[JsonWebKey]) derives JsonDecoder


  case class UnsupportedAlgorithm(name: String) extends DecodingError:
    override def message: String = s"unsupported JWK algorithm: $name"


  case class KeyGenerationFailed(name: String) extends DecodingError:
    override def message: String = s"failed to generate public key from JWK: $name"


  case class OKP(kid: String, use: String, crv: String, alg: String, x: String) extends JsonWebKey
  case class RSA(kid: String, use: String, alg: String, n: String, e: String)   extends JsonWebKey

  /**
   *
   */
  given JsonDecoder[OKP] = JsonDecoder.derived

  /**
   *
   */
  given JsonDecoder[RSA] = JsonDecoder.derived


  /**
   * Decode a JWK by first reading its `kty`, then delegating to the matching key-type decoder. Decodes
   * the JSON to the AST once (so `kty` can be peeked and the same node re-decoded), reads the
   * discriminator via [[KeyType]], and dispatches to `OKP` / `RSA`.
   * @todo see if we can improve with native discriminator
   */
  given JsonDecoder[JsonWebKey] = JsonDecoder[Json].mapOrFail: json =>
    JsonDecoder[KeyType].fromJsonAST(json).flatMap {
      case KeyType("OKP") => JsonDecoder[OKP].fromJsonAST(json)
      case KeyType("RSA") => JsonDecoder[RSA].fromJsonAST(json)
      case KeyType(other) => Left(s"unsupported JWK kty: $other")
    }
