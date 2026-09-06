package homelab.common.error


import zio.{ NonEmptyChunk, Scope }
import zio.test.*


object ValidationErrorSpec extends ZIOSpecDefault:

  private enum Problem extends ValidationError.InvalidInput:
    case Missing
    case TooLong(limit: Int)

    def message: String = this match
      case Missing        => "a name is required"
      case TooLong(limit) => s"at most $limit characters"

  private class Handwritten extends ValidationError.InvalidInput:
    def message: String = "hand written"

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ValidationError")(
    test("the aggregate reads as prose, because it is what a caller is told") {
      // Not the problems' toString: this reaches a caller as a status description or a response body, and
      // `{kind: …, message: …}` is a log line, not an explanation.
      val error = ValidationError(NonEmptyChunk(Problem.Missing, Problem.TooLong(3)))
      assertTrue(error.message == "a name is required; at most 3 characters")
    },
    test("a singleton enum case still has a kind") {
      // The shape this contract invites: `getClass.getSimpleName` on a Scala 3 enum's parameterless case is
      // the empty string, because the case is an anonymous class.
      assertTrue(Problem.Missing.kind == "Missing")
    },
    test("a parameterised case and a plain class keep their names") {
      assertTrue(Problem.TooLong(3).kind == "TooLong", new Handwritten().kind == "Handwritten")
    },
  )
