package homelab.incubator.auth

import homelab.incubator.auth.v1.{JwksKeySource, KeySource}
import zio.*
import zio.test.*

import java.security.{KeyPairGenerator, PublicKey}

object KeySourceSpec extends ZIOSpecDefault:

  private def genKey: PublicKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair.getPublic

  def spec = suite("JwksKeySource")(
    test("known key resolves, and repeated lookups are served from cache (one fetch)") {
      val key = genKey
      for
        fetches <- Ref.make(0)
        fetch    = fetches.update(_ + 1).as(Map("k1" -> key))
        source  <- JwksKeySource.make(fetch)
        first   <- source.publicKey("k1")
        second  <- source.publicKey("k1")
        count   <- fetches.get
      yield assertTrue(first == key, second == key, count == 1)
    },
    test("a new key id triggers a refetch and resolves") {
      val oldKey = genKey
      val newKey = genKey
      for
        calls  <- Ref.make(0)
        fetch   = calls.updateAndGet(_ + 1).map(n => if n == 1 then Map("old" -> oldKey) else Map("old" -> oldKey, "new" -> newKey))
        source <- JwksKeySource.make(fetch)
        a      <- source.publicKey("old") // miss → fetch #1 {old}
        b      <- source.publicKey("new") // cache miss on "new" → fetch #2 {old,new}
        count  <- calls.get
      yield assertTrue(a == oldKey, b == newKey, count == 2)
    },
    test("unknown key after refetch → UnknownKey") {
      for
        source <- JwksKeySource.make(ZIO.succeed(Map.empty[String, PublicKey]))
        exit   <- source.publicKey("missing").either
      yield assertTrue(exit == Left(KeySource.Failure.UnknownKey("missing")))
    },
    test("failed fetch → Unavailable") {
      val boom = new RuntimeException("jwks endpoint down")
      for
        source <- JwksKeySource.make(ZIO.fail(KeySource.Failure.Unavailable("unreachable", boom)))
        exit   <- source.publicKey("k1").either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[KeySource.Failure.Unavailable]))
    },
  )
