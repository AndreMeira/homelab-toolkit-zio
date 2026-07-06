package homelab.auth


import homelab.common.auth.Requester.Service
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ ServiceName, SignedToken }
import pdi.jwt.JwtClaim
import zio.*
import zio.test.*


object JwtServiceAuthenticatorSpec extends ZIOSpecDefault:

  private val audience     = "homelab"
  private val issuer       = "https://kubernetes.default.svc"
  private val subject      = "system:serviceaccount:default:demo"
  private val expectations = JwtServiceAuthenticator.Expectations(audience, issuer)
  private val token        = SignedToken("x") // ignored by the stub verifier


  private def claim(aud: Set[String], iss: String, sub: Option[String] = Some(subject)): JwtClaim =
    JwtClaim(subject = sub, audience = Some(aud), issuer = Some(iss))


  private def verifierReturning(c: JwtClaim): TokenVerifier = new TokenVerifier:
    def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] = ZIO.succeed(c)


  private def authenticate(c: JwtClaim) =
    JwtServiceAuthenticator(verifierReturning(c), expectations).authenticate(token)


  def spec = suite("JwtServiceAuthenticator")(
    test("a matching audience and issuer → the calling Service") {
      for who <- authenticate(claim(Set(audience), issuer))
      yield assertTrue(who == Service(ServiceName(subject)))
    },
    test("an audience that doesn't include ours → InvalidServiceToken") {
      for exit <- authenticate(claim(Set("someone-else"), issuer)).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("the wrong issuer → InvalidServiceToken") {
      for exit <- authenticate(claim(Set(audience), "https://evil.example")).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("no subject → InvalidServiceToken") {
      for exit <- authenticate(claim(Set(audience), issuer, sub = None)).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwtServiceAuthenticator.InvalidServiceToken]))
    },
    test("a verifier failure passes through unchanged") {
      val failing = new TokenVerifier:
        def verify(t: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] =
          ZIO.fail(JwksTokenVerifier.UntrustedToken("bad signature"))
      for exit <- JwtServiceAuthenticator(failing, expectations).authenticate(token).either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[JwksTokenVerifier.UntrustedToken]))
    },
  )
