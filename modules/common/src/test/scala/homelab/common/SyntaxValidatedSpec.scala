package homelab.common


import homelab.common.error.{ ApplicationError, ValidationError }
import zio.*
import zio.prelude.Validation
import zio.test.*


object SyntaxValidatedSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Validated")(
    test("a success passes the value through") {
      for value <- Validation.succeed(42).orFail
      yield assertTrue(value == 42)
    },
    test("every problem is reported, not just the first") {
      // The property the applicative exists for: two failing checks yield two problems, where an error
      // channel would have stopped at the first.
      val both: Validated[Int] =
        Validation.validateWith(
          Validation.fail(TooShort("name")),
          Validation.fail(TooShort("email")),
        )((_: Int, _: Int) => 0)
      for failure <- both.orFail.flip
      yield assertTrue(failure.errors.size == 2, failure.errors.map(_.message).contains("email is too short"))
    },
    test("orFailWith builds a service's own error from the problems") {
      val invalid: Validated[Int] = Validation.fail(TooShort("name"))
      for failure <- invalid.orFailWith(Refused.apply).flip
      yield assertTrue(failure.isInstanceOf[Refused], failure.message == "name is too short")
    },
  )

  /** A problem a field can have, and the refusal that collects them. */
  private object Support {

    case class TooShort(field: String) extends ValidationError.InvalidInput:
      def message: String = s"$field is too short"

    case class Refused(problems: NonEmptyChunk[ValidationError.InvalidInput]) extends ApplicationError.DomainError:
      def message: String = problems.map(_.message).mkString("; ")
  }
