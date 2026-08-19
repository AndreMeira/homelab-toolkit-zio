package homelab.incubator.llm.v2


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v2.Tool.{ Call, Rejected }
import zio.*
import zio.json.*
import zio.schema.{ Schema, derived }
import zio.test.*


/** Registration is where a tool meets the wire; dispatch is where the caller meets the model's arguments. */
object ToolSpec extends ZIOSpecDefault:

  private final case class Caller(userId: String)

  private final case class Search(text: String) derives Schema

  private val allOrders = List("alice" -> "a-1", "alice" -> "a-2", "bob" -> "b-1")

  private val orders = new Tool[Caller, Search, List[String]]:
    override def name        = "orders"
    override def description = "Find the caller's orders"
    override def handle(context: Caller, input: Search): IO[ApplicationError, List[String]] =
      ZIO.succeed(allOrders.collect { case (owner, id) if owner == context.userId => id })

  /** A map, described as an association list — the case where schema and codec used to disagree. */
  private given Schema[Map[String, Int]] = Schemas.mapAsEntries

  private final case class Report(title: String, counts: Map[String, Int]) derives Schema

  private val report = new Tool[Caller, Report, String]:
    override def name                                                                 = "report"
    override def description                                                          = "Summarise counts"
    override def handle(context: Caller, input: Report): IO[ApplicationError, String] =
      ZIO.succeed(s"${input.title}: ${input.counts.toList.sorted.map((k, v) => s"$k=$v").mkString(",")}")

  /** Arguments the model cannot name — a bare string has nowhere to go in `parameters`. */
  private val unnamed = new Tool[Caller, String, String]:
    override def name                                                                 = "unnamed"
    override def description                                                          = "takes a bare string"
    override def handle(context: Caller, input: String): IO[ApplicationError, String] = ZIO.succeed(input)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Tool")(
    test("advertises a registered tool in the shape a provider expects") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
      yield assertTrue(
        session.advertised.map(_.toJson) == List(
          """{"type":"function","function":{"name":"orders","description":"Find the caller's orders",""" +
            """"parameters":{"type":"object","properties":{"text":{"type":"string"}},""" +
            """"required":["text"],"additionalProperties":false}}}"""
        )
      )
    },
    test("joins the caller's namespace with the model's arguments") {
      // Identical arguments, different sessions, different data — nothing the model writes changes the scope.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        alice    <- registry.forSession(Caller("alice"))
        bob      <- registry.forSession(Caller("bob"))
        first    <- alice.dispatch(Call("c1", "orders", """{"text":"everything"}"""))
        second   <- bob.dispatch(Call("c1", "orders", """{"text":"everything"}"""))
      yield assertTrue(first.content == """["a-1","a-2"]""", second.content == """["b-1"]""")
    },
    test("hands malformed arguments back as text instead of failing") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "orders", """{"nope":1}"""))
      yield assertTrue(outcome.content.startsWith("error: arguments did not parse"))
    },
    test("answers every call in a turn, failures included") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcomes <- session.dispatchAll(
                      List(Call("c1", "orders", """{"text":"x"}"""), Call("c2", "missing", "{}"))
                    )
      yield assertTrue(
        outcomes.map(_.callId) == List("c1", "c2"),
        outcomes(1).content == "error: no tool 'missing' is available",
      )
    },
    test("decodes exactly what it advertised, for a shape the two derivations used to disagree on") {
      // The schema says an array of {key,value}; an independently derived zio-json decoder would have
      // demanded a JSON object here. Both now come from the same `Schema`, so the model's reply parses.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(report)
        session  <- registry.forSession(Caller("alice"))
        advert    = session.advertised.map(_.toJson).mkString
        outcome  <- session.dispatch(
                      Call("c1", "report", """{"title":"t","counts":[{"key":"a","value":1},{"key":"b","value":2}]}""")
                    )
      yield assertTrue(
        advert.contains(""""items":{"type":"object","properties":{"key":{"type":"string"}"""),
        outcome.content == "\"t: a=1,b=2\"",
      )
    },
    test("refuses at registration a tool whose arguments are not an object") {
      for
        registry <- Tool.Registry.make[Caller]
        outcome  <- registry.add(unnamed).either
      yield assertTrue(
        outcome.left.map(_.tool) == Left("unnamed"),
        outcome.left.map(_.message.contains("must be an object")) == Left(true),
      )
    },
  )
