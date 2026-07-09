package homelab.auth


import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import pdi.jwt.JwtClaim
import zio.*
import zio.test.*


object CachedTokenVerifierSpec extends ZIOSpecDefault:

  private val subject = "system:serviceaccount:default:demo"
  private val token   = SignedToken("token-abc")

  // TestClock starts at the epoch, so token `exp` values are absolute epoch-seconds on that timeline.
  private def claim(expEpochSecond: Long): JwtClaim =
    JwtClaim(subject = Some(subject), expiration = Some(expEpochSecond))

  private val farClaim  = claim(3600) // ~1h out — beyond any ttl used here, so ttl governs freshness
  private val soonClaim = claim(10)   // 10s out — inside the ttl, so the token's exp caps the entry

  private case object Boom extends AdapterError:
    override def message: String = "boom"

  /** A [[TokenVerifier]] that counts how often it's consulted and always yields `result`. */
  private def counting(counter: Ref[Int], result: IO[AdapterError | UnauthorisedError, JwtClaim]): TokenVerifier =
    new TokenVerifier:
      def verify(t: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] =
        counter.update(_ + 1) *> result

  def spec = suite("CachedTokenVerifier")(
    test("returns the inner claims on a miss") {
      for
        counter <- Ref.make(0)
        cached  <- CachedTokenVerifier.make(counting(counter, ZIO.succeed(farClaim)), 1.minute)
        result  <- cached.verify(token)
      yield assertTrue(result.subject.contains(subject))
    },
    test("reuses a fresh result: inner is consulted once across repeated verifies") {
      for
        counter <- Ref.make(0)
        cached  <- CachedTokenVerifier.make(counting(counter, ZIO.succeed(farClaim)), 1.minute)
        _       <- cached.verify(token)
        _       <- cached.verify(token)
        hits    <- counter.get
      yield assertTrue(hits == 1)
    },
    test("re-verifies once the ttl has elapsed") {
      for
        counter <- Ref.make(0)
        cached  <- CachedTokenVerifier.make(counting(counter, ZIO.succeed(farClaim)), 1.minute)
        _       <- cached.verify(token)
        _       <- TestClock.adjust(90.seconds)
        _       <- cached.verify(token)
        hits    <- counter.get
      yield assertTrue(hits == 2)
    },
    test("an entry never outlives the token's exp (even within the ttl)") {
      for
        counter <- Ref.make(0)
        cached  <- CachedTokenVerifier.make(counting(counter, ZIO.succeed(soonClaim)), 1.minute)
        _       <- cached.verify(token)
        _       <- TestClock.adjust(15.seconds) // past the token's exp (10s), still within ttl (60s)
        _       <- cached.verify(token)
        hits    <- counter.get
      yield assertTrue(hits == 2)
    },
    test("failures are not cached: inner is consulted on every failing call") {
      for
        counter <- Ref.make(0)
        cached  <- CachedTokenVerifier.make(counting(counter, ZIO.fail(Boom)), 1.minute)
        _       <- cached.verify(token).either
        _       <- cached.verify(token).either
        hits    <- counter.get
      yield assertTrue(hits == 2)
    },
  )
