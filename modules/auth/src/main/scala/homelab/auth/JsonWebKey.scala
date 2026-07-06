package homelab.auth


import zio.json.JsonDecoder
import zio.json.ast.Json


enum JsonWebKey:
  case RSA(kid: String, use: String, alg: String, n: String, e: String)
  case OKP(kid: String, use: String, crv: String, alg: String, x: String)

  def keyId: String = this match
    case JsonWebKey.RSA(kid, _, _, _, _) => kid
    case JsonWebKey.OKP(kid, _, _, _, _) => kid


object JsonWebKey:
  case class KeyType(kty: String) derives JsonDecoder
  case class Set(keys: List[JsonWebKey]) derives JsonDecoder

  given JsonDecoder[JsonWebKey.OKP] = JsonDecoder.derived

  given JsonDecoder[JsonWebKey.RSA] = JsonDecoder.derived

  given JsonDecoder[JsonWebKey] = JsonDecoder[Json].mapOrFail: json =>
    JsonDecoder[KeyType].fromJsonAST(json).flatMap {
      case KeyType("OKP") => JsonDecoder[OKP].fromJsonAST(json)
      case KeyType("RSA") => JsonDecoder[RSA].fromJsonAST(json)
      case KeyType(other) => Left(s"unsupported JWK kty: $other")
    }
