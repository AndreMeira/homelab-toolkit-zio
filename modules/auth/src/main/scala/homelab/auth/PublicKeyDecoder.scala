package homelab.auth

import homelab.common.error.ApplicationError

import java.math.BigInteger
import java.security.spec.{ RSAPublicKeySpec, X509EncodedKeySpec }
import java.security.{ KeyFactory, PublicKey }
import java.util.Base64
import scala.util.Try

object PublicKeyDecoder {
  def decode(key: JsonWebKey): Either[Failure, PublicKey] = key match {
    case okp: JsonWebKey.OKP if okp.crv == "Ed25519" => ed25519(okp)
    case rsa: JsonWebKey.RSA if rsa.alg == "RS256"   => rsaKey(rsa)
    case other                                       => Left(Failure.UnsupportedKey(other))
  }

  /**
   * OKP → Ed25519 (`EdDSA`): the JWK `x` is the raw 32-byte public key. Wrap it in the fixed Ed25519
   * SubjectPublicKeyInfo DER header and let `KeyFactory` decode the point — no manual y / x-sign parsing.
   */
  private def ed25519(key: JsonWebKey.OKP): Either[Failure, PublicKey] = Try {
    val spkiHeader = Array(0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00).map(_.toByte)
    val der        = spkiHeader ++ Base64.getUrlDecoder.decode(key.x)
    KeyFactory.getInstance("Ed25519").generatePublic(X509EncodedKeySpec(der))
  }.toEither.left.map(err => Failure.KeyGenerationFailed(err))

  /** 
   * RSA → `RS256`: `n` (modulus) and `e` (exponent) are base64url big-endian unsigned integers. 
   */
  private def rsaKey(key: JsonWebKey.RSA): Either[Failure, PublicKey] = Try {
    val modulus  = BigInteger(1, Base64.getUrlDecoder.decode(key.n))
    val exponent = BigInteger(1, Base64.getUrlDecoder.decode(key.e))
    KeyFactory.getInstance("RSA").generatePublic(RSAPublicKeySpec(modulus, exponent))
  }.toEither.left.map(err => Failure.KeyGenerationFailed(err))

  /**
   *
   */
  enum Failure extends ApplicationError.DecodingError:
    case UnsupportedKey(key: JsonWebKey)
    case KeyGenerationFailed(err: Throwable)

    override def message: String = this match {
      case UnsupportedKey(key)      => s"unsupported JWK algorithm: $key"
      case KeyGenerationFailed(err) => s"key generation failed with ${err.getMessage}"
    }
}
