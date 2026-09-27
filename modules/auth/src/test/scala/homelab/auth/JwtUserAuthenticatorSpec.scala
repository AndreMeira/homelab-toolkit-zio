package homelab.auth


import homelab.common.auth.Requester.User
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ SignedToken, UserId, UserName }
import pdi.jwt.JwtClaim
import zio.*
import zio.test.*

import java.util.UUID


object JwtUserAuthenticatorSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("JwtUserAuthenticator")(
    test("authenticate a valid token → Authenticated") {
      for who <- authenticator(claim()).authenticate(token)
      yield assertTrue(who == User.Authenticated(UserId(userId), UserName("alice")))
    },
    test("authenticate a non-UUID subject → InvalidUserToken") {
      for exit <- authenticator(claim(sub = Some("not-a-uuid"))).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtUserAuthenticator.InvalidUserToken]))
    },
    test("authenticate a token with no name claim → InvalidUserToken") {
      for exit <- authenticator(claim(content = "{}")).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtUserAuthenticator.InvalidUserToken]))
    },
    test("any(None) → Anonymous") {
      for who <- authenticator(claim()).any(None)
      yield assertTrue(who == User.Anonymous)
    },
    test("any with a valid token → Authenticated") {
      for who <- authenticator(claim()).any(Some(token))
      yield assertTrue(who == User.Authenticated(UserId(userId), UserName("alice")))
    },
    test("any with a present-but-invalid token → rejected (UnauthorisedError)") {
      val invalid = JwksTokenVerifier.UntrustedToken("bad signature")
      for exit <- JwtUserAuthenticator(verifierFailing(invalid)).any(Some(token)).either
      yield assertTrue(exit.swap.exists(_ == invalid))
    },
    test("any propagates an AdapterError") {
      for exit <- JwtUserAuthenticator(verifierFailing(BackendDown)).any(Some(token)).either
      yield assertTrue(exit.swap.exists(_ == BackendDown))
    },
  )

  /** The claims these tests authenticate, and verifiers that return or refuse them. */
  private object Support {

    val userId = UUID.fromString("00000000-0000-0000-0000-00000000000a")
    val token  = SignedToken("x") // ignored by the stub verifier

    case object BackendDown extends AdapterError:
      override def message: String = "backend down"

    def claim(content: String = """{"name":"alice"}""", sub: Option[String] = Some(userId.toString)): JwtClaim =
      JwtClaim(content = content, subject = sub)

    def verifierReturning(c: JwtClaim): TokenVerifier = new TokenVerifier:
      def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] = ZIO.succeed(c)

    def verifierFailing(error: AdapterError | UnauthorisedError): TokenVerifier = new TokenVerifier:
      def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] = ZIO.fail(error)

    def authenticator(c: JwtClaim) = JwtUserAuthenticator(verifierReturning(c))
  }
