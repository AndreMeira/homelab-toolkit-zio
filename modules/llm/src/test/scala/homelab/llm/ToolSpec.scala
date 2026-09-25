package homelab.llm


import homelab.common.error.ApplicationError
import zio.schema.{ Schema, derived }
import zio.test.*
import zio.{ IO, Ref, Scope, ZIO }


/** What a tool is on its own: what it answers with, what it reads, and what it refuses to be described as. */
object ToolSpec extends ZIOSpecDefault:

  final private case class Where(city: String) derives Schema

  final private case class Reading(degrees: Double) derives Schema

  final private case class Rig(arm: String, tenant: String)

  final private case class Refused(message: String) extends ApplicationError.AdapterError

  private val weather: Tool[String, Where, Reading] =
    Tool.Definition("weather", "Report the temperature.") { (_: String) => (input: Where) =>
      ZIO.succeed(Tool.Result.success(Reading(if input.city == "Hamburg" then 12.0 else 20.0)))
    }

  private def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

  private def call(arguments: String): Tool.Call.Raw = Tool.Call.Raw(id("c1"), "weather", arguments)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Tool")(
    suite("result")(
      test("a success renders as its value, written by its own schema") {
        assertTrue(Tool.Result.success(Reading(12.0)).render == """{"degrees":12.0}""")
      },
      test("a failure renders as the envelope a provider without an error flag needs") {
        assertTrue(
          Tool.Result.failure[Reading]("no such city").render == """{"isError":true,"reason":"no such city"}"""
        )
      },
      test("only render envelopes, so a reason stays prose until it becomes something a model reads") {
        assertTrue(
          Tool.Result.failure[Reading]("no such city") == Tool.Result.Failed[Reading]("no such city"),
          Tool.Result.failure[Reading]("no such city").failed,
          !Tool.Result.success(Reading(12.0)).failed,
        )
      },
    ),
    suite("decoding")(
      test("reads a call's arguments as the type the tool takes") {
        assertTrue(weather.decoded("""{"city":"Hamburg"}""") == Right(Where("Hamburg")))
      },
      test("says the arguments were the problem when they do not parse") {
        assertTrue(weather.decoded("""{"city":42}""").left.exists(_.startsWith("arguments did not parse")))
      },
      test("refuses arguments that parse as JSON but not as the type") {
        assertTrue(weather.decoded("""{"town":"Hamburg"}""").isLeft)
      },
    ),
    suite("permits")(
      test("a tool is available by default") {
        weather.permits("anyone").map(allowed => assertTrue(allowed))
      },
      test("one that cannot say refuses rather than assuming either way") {
        val guarded = new Tool.Definition[String, Where, Reading]("weather", "…") {
          override def permits(context: String): IO[ApplicationError, Boolean] = ZIO.fail(Refused("no idea"))
          override def handle(context: String, input: Where): IO[ApplicationError, Tool.Result[Reading]] =
            ZIO.succeed(Tool.Result.success(Reading(0.0)))
        }
        guarded.permits("anyone").flip.map(error => assertTrue(error == Refused("no idea")))
      },
    ),
    suite("contramapCtx")(
      test("narrows the context a tool is run against, and its permission with it") {
        for
          seen    <- Ref.make(List.empty[String])
          narrow   = new Tool.Definition[String, Where, Reading]("weather", "…") {
                       override def permits(context: String): IO[ApplicationError, Boolean] =
                         seen.update(_ :+ s"permits:$context").as(true)
                       override def handle(
                         context: String,
                         input: Where,
                       ): IO[ApplicationError, Tool.Result[Reading]] =
                         seen.update(_ :+ s"handle:$context").as(Tool.Result.success(Reading(1.0)))
                     }
          wider    = narrow.contramapCtx[Rig](rig => rig.tenant)
          _       <- wider.permits(Rig("left", "acme"))
          _       <- wider.handle(Rig("left", "acme"), Where("Hamburg"))
          reached <- seen.get
        yield assertTrue(reached == List("permits:acme", "handle:acme"))
      },
      test("keeps the name and the description it was given") {
        val wider = weather.contramapCtx[Rig](rig => rig.tenant)
        assertTrue(wider.name == "weather", wider.description == "Report the temperature.")
      },
    ),
    suite("validateInput")(
      test("accepts a record, because arguments are named") {
        assertTrue(Tool.validateInput[Where].isRight)
      },
      test("accepts a recursive record, whose root is a reference to an object") {
        final case class Tree(value: String, children: List[Tree]) derives Schema
        assertTrue(Tool.validateInput[Tree].isRight)
      },
      test("refuses a bare string, which has nowhere to put a named argument") {
        assertTrue(
          Tool.validateInput[String].left.map(_.message) == Left("arguments must describe an object, not a string")
        )
      },
      test("refuses a sum type, which renders as a union rather than an object") {
        @zio.schema.annotation.discriminatorName("kind")
        enum Ending derives Schema:
          case Done(summary: String)
          case GiveUp(reason: String)
        assertTrue(Tool.validateInput[Ending].left.exists(_.message.contains("not a union")))
      },
      test("refuses a type outside the describable subset, saying it could not be described") {
        final case class Readings(byHour: Map[String, Int]) derives Schema
        assertTrue(
          Tool.validateInput[Readings].left.exists(_.message.startsWith("arguments cannot be described"))
        )
      },
    ),
    suite("call")(
      test("an id reads as the text the provider sent") {
        assertTrue(id("c1") == "c1", call("{}").id == id("c1"))
      },
      test("a raw call and a decoded one answer the same two questions") {
        val raw     = call("""{"city":"Hamburg"}""")
        val decoded = Tool.Call.Decoded(id("c1"), "weather", Where("Hamburg"))
        assertTrue(raw.id == decoded.id, raw.name == decoded.name)
      },
    ),
  )
