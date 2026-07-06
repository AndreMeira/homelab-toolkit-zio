package homelab.incubator.auth


import homelab.common.auth.Requester.User
import homelab.common.types.{ SignedToken, UserId, UserName }
import homelab.incubator.auth.v1.{ InvalidToken, JwtTokenVerifier, JwtUserAuthenticator }
import pdi.jwt.{ Jwt, JwtAlgorithm, JwtClaim }
import zio.*
import zio.test.*

import java.time.Instant
import java.util.UUID


object JwtTokenVerifierSpec extends ZIOSpecDefault:

  private val secret   = "test-secret-please-change-0123456789"
  private val algo     = JwtAlgorithm.HS256
  private val verifier = new JwtTokenVerifier(secret, algo)
  private val user     = JwtUserAuthenticator(verifier)

  private val userId = UUID.fromString("00000000-0000-0000-0000-000000000009")


  /** Issue a signed token (the "issuer" side of the round-trip). */
  private def sign(secretUsed: String, subject: String, name: String, expiresInSeconds: Long = 3600): SignedToken =
    val claim = JwtClaim(
      content = s"""{"name":"$name"}""",
      subject = Some(subject),
      expiration = Some(Instant.now.plusSeconds(expiresInSeconds).getEpochSecond),
    )
    SignedToken(Jwt.encode(claim, secretUsed, algo))


  def spec = suite("JwtTokenVerifier (round-trip)")(
    test("issued token authenticates back to the user")(
      user
        .authenticate(sign(secret, userId.toString, "alice"))
        .map(u => assertTrue(u == User.Authenticated(UserId(userId), UserName("alice"))))
    ),
    test("garbage token → UnauthorisedError")(
      failsWith(user.authenticate(SignedToken("not-a-jwt")))(_.isInstanceOf[InvalidToken])
    ),
    test("expired token → UnauthorisedError")(
      failsWith(user.authenticate(sign(secret, userId.toString, "alice", expiresInSeconds = -60)))(_.isInstanceOf[InvalidToken])
    ),
    test("wrong secret → UnauthorisedError")(
      failsWith(user.authenticate(sign("a-different-secret-9876543210", userId.toString, "alice")))(_.isInstanceOf[InvalidToken])
    ),
  )


  private def failsWith[E](effect: IO[E, Any])(pred: E => Boolean): UIO[TestResult] =
    effect.either.map {
      case Left(e)  => assertTrue(pred(e))
      case Right(_) => assertTrue(false)
    }
