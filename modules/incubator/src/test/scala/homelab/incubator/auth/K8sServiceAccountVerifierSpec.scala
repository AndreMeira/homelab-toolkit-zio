package homelab.incubator.auth

import homelab.common.auth.Requester.Service
import homelab.common.types.{ServiceName, SignedToken}
import homelab.incubator.auth.v1.{Claims, JwtServiceAuthenticator, K8sServiceAccountVerifier, TokenReviewer, TokenVerifier}
import zio.*
import zio.test.*

object K8sServiceAccountVerifierSpec extends ZIOSpecDefault:

  private val saName   = "system:serviceaccount:homelab:registration"
  private val anyToken = SignedToken("sa-token")

  private def reviewer(outcome: IO[TokenReviewer.Unavailable, TokenReviewer.Result]): TokenReviewer =
    new TokenReviewer:
      def review(token: SignedToken): IO[TokenReviewer.Unavailable, TokenReviewer.Result] = outcome

  private def verifier(outcome: IO[TokenReviewer.Unavailable, TokenReviewer.Result]): K8sServiceAccountVerifier =
    new K8sServiceAccountVerifier(reviewer(outcome))

  def spec = suite("K8sServiceAccountVerifier")(
    test("authenticated SA token → Claims with the canonical name") {
      verifier(ZIO.succeed(TokenReviewer.Result(authenticated = true, Some(saName))))
        .verify(anyToken)
        .map(claims => assertTrue(claims == Claims(saName, "registration")))
    },
    test("end-to-end via ServiceAuthenticator → Service") {
      JwtServiceAuthenticator(verifier(ZIO.succeed(TokenReviewer.Result(authenticated = true, Some(saName)))))
        .authenticate(anyToken)
        .map(who => assertTrue(who == Service(ServiceName(saName))))
    },
    test("unauthenticated → Invalid") {
      verifier(ZIO.succeed(TokenReviewer.Result(authenticated = false, None)))
        .verify(anyToken)
        .either
        .map(exit => assertTrue(exit.swap.exists(_.isInstanceOf[TokenVerifier.Failure.Invalid])))
    },
    test("API server unreachable → Unavailable") {
      verifier(ZIO.fail(TokenReviewer.Unavailable("api down", new RuntimeException("boom"))))
        .verify(anyToken)
        .either
        .map(exit => assertTrue(exit.swap.exists(_.isInstanceOf[TokenVerifier.Failure.Unavailable])))
    },
  )
