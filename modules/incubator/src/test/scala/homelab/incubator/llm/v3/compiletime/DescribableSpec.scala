package homelab.incubator.llm.v3.compiletime


import scala.compiletime.testing.typeChecks
import zio.Scope
import zio.test.*


/**
 * What a macro can decide about a tool's arguments before the program runs.
 *
 * The accepting cases prove themselves by compiling — the calls below are macro expansions, and a
 * rejection would fail this file rather than this suite. The refusing cases are asserted with
 * `typeChecks`, which reports whether a snippet survives typing.
 */
object DescribableSpec extends ZIOSpecDefault:

  final case class Search(text: String, limit: Option[Int])

  final case class Node(value: String, children: List[Node])

  enum Colour:
    case Red, Green

  final case class Painted(colour: Colour, at: Node)

  final case class Indexed(byName: Map[String, Int])

  final case class Paired(point: (Int, Int))

  final case class Nothing0(nothing: Unit)

  // Expansions, not assertions: this file would not compile if any of them were refused.
  Describable.check[Search]
  Describable.check[Node]
  Describable.check[Painted]

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Describable")(
    test("a record of describable fields passes") {
      assertTrue(typeChecks("Describable.check[Search]"))
    },
    test("a recursive type passes, and the walk terminates") {
      assertTrue(typeChecks("Describable.check[Node]"))
    },
    test("a map is refused, wherever it sits") {
      assertTrue(!typeChecks("Describable.check[Indexed]"))
    },
    test("a tuple is refused") {
      assertTrue(!typeChecks("Describable.check[Paired]"))
    },
    test("Unit is refused") {
      assertTrue(!typeChecks("Describable.check[Nothing0]"))
    },
    test("a map nested inside another record is still refused") {
      assertTrue(!typeChecks("Describable.check[List[Indexed]]"))
    },
  )
