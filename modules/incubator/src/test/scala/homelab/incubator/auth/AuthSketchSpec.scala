package homelab.incubator.auth


import homelab.common.auth.Requester.{ Service, User }
import homelab.common.types.{ ServiceName, SignedToken, UserId, UserName }
import homelab.incubator.auth.v1.{ Claims, InvalidToken, JwtServiceAuthenticator, JwtUserAuthenticator, KeySourceUnavailable, TokenVerifier }
import zio.*
import zio.test.*

import java.util.UUID


object AuthSketchSpec extends ZIOSpecDefault:

  /** In-memory stub verifier: known tokens → claims, unknown → Invalid; `unavailable` simulates infra failure. */
  final private class InMemory(valid: Map[SignedToken, Claims], unavailable: Boolean = false) extends TokenVerifier:

    def verify(token: SignedToken): IO[TokenVerifier.Failure, Claims] =
      if unavailable then ZIO.fail(TokenVerifier.Failure.Unavailable("key source unreachable", new RuntimeException("stub")))
      else ZIO.fromOption(valid.get(token)).orElseFail(TokenVerifier.Failure.Invalid("unknown or expired token"))


  private val userId       = UUID.fromString("00000000-0000-0000-0000-000000000001")
  private val userToken    = SignedToken("user-token")
  private val serviceToken = SignedToken("service-token")
  private val badToken     = SignedToken("nope")


  private val verifier =
    new InMemory(Map(userToken -> Claims(userId.toString, "alice"), serviceToken -> Claims("registration", "registration-service")))


  private val down     = new InMemory(Map.empty, unavailable = true)

  private val service = JwtServiceAuthenticator(verifier)
  private val user    = JwtUserAuthenticator(verifier)


  /** Assert the effect fails, and its error satisfies `pred`. */
  private def failsWith[E](effect: IO[E, Any])(pred: E => Boolean): UIO[TestResult] =
    effect.either.map {
      case Left(e)  => assertTrue(pred(e))
      case Right(_) => assertTrue(false) // expected a failure
    }


  def spec = suite("auth sketches")(
    suite("ServiceAuthenticator")(
      test("valid token → Service")(
        service.authenticate(serviceToken).map(s => assertTrue(s == Service(ServiceName("registration"))))
      ),
      test("invalid token → UnauthorisedError")(
        failsWith(service.authenticate(badToken))(_.isInstanceOf[InvalidToken])
      ),
      test("key source down → AdapterError")(
        failsWith(JwtServiceAuthenticator(down).authenticate(serviceToken))(_.isInstanceOf[KeySourceUnavailable])
      ),
    ),
    suite("UserAuthenticator.authenticate")(
      test("valid token → Authenticated")(
        user.authenticate(userToken).map(u => assertTrue(u == User.Authenticated(UserId(userId), UserName("alice"))))
      ),
      test("invalid token → UnauthorisedError")(
        failsWith(user.authenticate(badToken))(_.isInstanceOf[InvalidToken])
      ),
    ),
    suite("UserAuthenticator.any")(
      test("no token → Anonymous")(
        user.any(None).map(u => assertTrue(u == User.Anonymous))
      ),
      test("valid token → Authenticated")(
        user.any(Some(userToken)).map(u => assertTrue(u == User.Authenticated(UserId(userId), UserName("alice"))))
      ),
      test("present-but-invalid → Anonymous (downgraded, not rejected)")(
        user.any(Some(badToken)).map(u => assertTrue(u == User.Anonymous))
      ),
      test("key source down → AdapterError escapes")(
        failsWith(JwtUserAuthenticator(down).any(Some(userToken)))(_.isInstanceOf[KeySourceUnavailable])
      ),
    ),
  )
