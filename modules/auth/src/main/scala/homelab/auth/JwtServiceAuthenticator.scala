package homelab.auth


import homelab.common.auth.Requester.Service
import homelab.common.auth.ServiceAuthenticator
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ ServiceName, SignedToken }
import homelab.auth.JwtServiceAuthenticator.*
import pdi.jwt.JwtClaim
import zio.*


/**
 * A [[ServiceAuthenticator]] over a [[TokenVerifier]]: verifies the service token, checks it was minted for
 * us (`aud`) by the trusted issuer (`iss`), and maps its subject to the calling [[Service]].
 *
 * The verifier handles signature + expiry; this adds the audience/issuer binding — the check that stops a
 * validly-signed token minted for *another* audience (e.g. a Kubernetes service-account token for a
 * different service) from authenticating here — plus the claim → identity mapping.
 *
 * @param verifier     verifies the token's signature and expiry
 * @param expectations the audience and issuer the token must carry
 */
final class JwtServiceAuthenticator(verifier: TokenVerifier, expectations: Expectations) extends ServiceAuthenticator:

  /**
   * Authenticate a calling service from its bearer token.
   *
   * @param token the signed service token presented on the call
   * @return the calling service; fails with [[InvalidServiceToken]] if the audience or issuer don't match
   *         or the subject is missing, or forwards the verifier's `AdapterError | UnauthorisedError`
   */
  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, Service] =
    for
      claim   <- verifier.verify(token)
      _       <- checkAudience(claim)
      _       <- checkIssuer(claim)
      service <- serviceOf(claim)
    yield service

  /**
   * Check the token was minted for us — its `aud` includes the expected audience.
   *
   * @param claim the verified claims
   * @return unit; fails with [[InvalidServiceToken]] if the audience doesn't include the expected one
   */
  private def checkAudience(claim: JwtClaim): IO[UnauthorisedError, Unit] =
    if claim.audience.exists(_.contains(expectations.audience)) then ZIO.unit
    else ZIO.fail(InvalidServiceToken(s"audience ${claim.audience.getOrElse(Set.empty)} does not include '${expectations.audience}'"))

  /**
   * Check the token came from the trusted issuer — its `iss` equals the expected issuer.
   *
   * @param claim the verified claims
   * @return unit; fails with [[InvalidServiceToken]] if the issuer doesn't match
   */
  private def checkIssuer(claim: JwtClaim): IO[UnauthorisedError, Unit] =
    if claim.issuer.contains(expectations.issuer) then ZIO.unit
    else ZIO.fail(InvalidServiceToken(s"issuer ${claim.issuer.getOrElse("<none>")} is not '${expectations.issuer}'"))

  /**
   * Map the token's subject to the calling service.
   *
   * @param claim the verified claims
   * @return the service; fails with [[InvalidServiceToken]] if the subject is missing
   */
  private def serviceOf(claim: JwtClaim): IO[UnauthorisedError, Service] =
    ZIO.fromOption(claim.subject).mapBoth(_ => InvalidServiceToken("token has no subject"), sub => Service(ServiceName(sub)))


object JwtServiceAuthenticator:

  /**
   * What a service token must carry to be accepted.
   *
   * @param audience the audience the token must be minted for (its `aud` must include this)
   * @param issuer   the issuer the token must come from (its `iss` must equal this)
   */
  final case class Expectations(audience: String, issuer: String)

  /** The token verified cryptographically but isn't acceptable — wrong audience/issuer, or no subject. */
  final case class InvalidServiceToken(reason: String) extends UnauthorisedError:
    override def message: String = s"invalid service token: $reason"
