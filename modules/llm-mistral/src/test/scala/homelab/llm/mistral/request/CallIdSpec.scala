package homelab.llm.mistral.request


import homelab.llm.Tool
import zio.test.*
import zio.{ Chunk, Scope }


/** The id a call goes out under, for any id a conversation might hold. */
object CallIdSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CallId")(
    test("an id Mistral would take goes out as it is") {
      check(Gen.stringN(9)(Gen.alphaNumericChar)) { id =>
        assertTrue(CallId.from(Tool.Call.Id(id)) == id)
      }
    },
    test("any id goes out as nine letters and digits") {
      check(Gen.string) { id =>
        assertTrue(CallId.from(Tool.Call.Id(id)).matches("[A-Za-z0-9]{9}"))
      }
    },
    test("two different ids go out as two different ids") {
      check(Gen.string, Gen.string) { (one, other) =>
        assertTrue(one == other || CallId.from(Tool.Call.Id(one)) != CallId.from(Tool.Call.Id(other)))
      }
    },
    test("the ids other providers mint, which differ only at the end, still go out apart") {
      val minted = Chunk("call_0fypS1hVXab", "call_0fypS1hVXac", "toolu_01VpEm654HvfMoRcrE3Hgdc6", "toolu_01VpEm654HvfMoRcrE3Hgdc7")
      assertTrue(minted.map(id => CallId.from(Tool.Call.Id(id))).toSet.size == minted.size)
    },
  )
