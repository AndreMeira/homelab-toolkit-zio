package homelab.auth


import homelab.common.auth.Requester.Service
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ ServiceName, SignedToken }
import pdi.jwt.JwtClaim
import zio.*
import zio.test.*


object JwtServiceAuthenticatorSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("JwtServiceAuthenticator")(
    test("a matching audience and issuer → the calling Service") {
      val verifier = verifierReturning(claim(Set(audience), issuer))
      for who <- JwtServiceAuthenticator(verifier, expectations).authenticate(token)
      yield assertTrue(who == Service(ServiceName(subject)))
    },
    test("an audience that doesn't include ours → InvalidServiceToken") {
      val verifier = verifierReturning(claim(Set("someone-else"), issuer))
      for exit <- JwtServiceAuthenticator(verifier, expectations).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("the wrong issuer → InvalidServiceToken") {
      val verifier = verifierReturning(claim(Set(audience), "https://evil.example"))
      for exit <- JwtServiceAuthenticator(verifier, expectations).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("no subject → InvalidServiceToken") {
      val verifier = verifierReturning(claim(Set(audience), issuer, sub = None))
      for exit <- JwtServiceAuthenticator(verifier, expectations).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("a verifier failure passes through unchanged") {
      val verifier = verifierFailing(JwksTokenVerifier.UntrustedToken("bad signature"))
      for exit <- JwtServiceAuthenticator(verifier, expectations).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.UntrustedToken]))
    },
  )

  /** What these tests expect of a token, the claims they hand it, and verifiers that return or refuse them. */
  private object Support {

    val audience     = "homelab"
    val issuer       = "https://kubernetes.default.svc"
    val subject      = "system:serviceaccount:inmemory:demo"
    val expectations = JwtServiceAuthenticator.Expectations(audience, issuer)
    val token        = SignedToken("x") // ignored by the stub verifier

    def claim(aud: Set[String], iss: String, sub: Option[String] = Some(subject)): JwtClaim =
      JwtClaim(subject = sub, audience = Some(aud), issuer = Some(iss))

    def verifierReturning(claim: JwtClaim): TokenVerifier = new TokenVerifier:
      def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] = ZIO.succeed(claim)

    def verifierFailing(error: AdapterError | UnauthorisedError): TokenVerifier = new TokenVerifier:
      def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] = ZIO.fail(error)
  }
