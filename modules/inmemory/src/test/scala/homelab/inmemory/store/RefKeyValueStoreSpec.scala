package homelab.inmemory.store


import zio.*
import zio.test.*


// Correctness spec for the in-memory KeyValueStore adapter, pinning the port's contract: absence is None,
// set upserts, and delete reports whether the key existed. A per-suite timeout turns any hang into a
// failure rather than blocking the run.
object RefKeyValueStoreSpec extends ZIOSpecDefault:

  def spec = suite("InMemoryKeyValueStore")(
    test("get returns None for an absent key") {
      for
        store <- InMemoryKeyValueStore.make[Int, String]
        out   <- store.get(1)
      yield assertTrue(out.isEmpty)
    },
    test("set then get returns the stored value") {
      for
        store <- InMemoryKeyValueStore.make[Int, String]
        _     <- store.set(1, "a")
        out   <- store.get(1)
      yield assertTrue(out == Some("a"))
    },
    test("set overwrites an existing value (upsert)") {
      for
        store <- InMemoryKeyValueStore.make[Int, String]
        _     <- store.set(1, "a")
        _     <- store.set(1, "b")
        out   <- store.get(1)
      yield assertTrue(out == Some("b"))
    },
    test("delete removes an existing key and reports true; the key is then absent") {
      for
        store   <- InMemoryKeyValueStore.make[Int, String]
        _       <- store.set(1, "a")
        removed <- store.delete(1)
        out     <- store.get(1)
      yield assertTrue(removed, out.isEmpty)
    },
    test("delete reports false for an absent key") {
      for
        store   <- InMemoryKeyValueStore.make[Int, String]
        removed <- store.delete(1)
      yield assertTrue(!removed)
    },
    test("fromMap seeds the store with its entries") {
      for
        store <- InMemoryKeyValueStore.fromMap(Map(1 -> "a", 2 -> "b"))
        a     <- store.get(1)
        b     <- store.get(2)
        c     <- store.get(3)
      yield assertTrue(a == Some("a"), b == Some("b"), c.isEmpty)
    },
    test("contramap re-keys the store, reading and writing through the mapping") {
      for
        store    <- InMemoryKeyValueStore.make[Int, String]
        byLength  = store.contramap[String](_.length)
        _        <- byLength.set("abc", "three") // stored under key 3
        viaString <- byLength.get("xyz")         // also length 3
        viaInt   <- store.get(3)
      yield assertTrue(viaString == Some("three"), viaInt == Some("three"))
    },
    test("concurrent sets to distinct keys all land") {
      val n = 1000
      for
        store <- InMemoryKeyValueStore.make[Int, Int]
        _     <- ZIO.foreachParDiscard(1 to n)(i => store.set(i, i * 10))
        outs  <- ZIO.foreach((1 to n).toList)(store.get)
      yield assertTrue(outs == (1 to n).map(i => Some(i * 10)).toList)
    },
  ) @@ TestAspect.timeout(60.seconds)
