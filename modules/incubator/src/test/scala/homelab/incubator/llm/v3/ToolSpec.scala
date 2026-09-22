package homelab.incubator.llm.v3


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v2.Schemas
import homelab.incubator.llm.v3.Tool.{ Call, Outcome, Result }
import zio.*
import zio.json.*
import zio.schema.{ Schema, derived }
import zio.test.*


/**
 * Registration is where a tool meets the wire; dispatch is where the caller meets the model's arguments.
 * v3 adds what a result leaves outstanding, so the fold over a turn is here too.
 */
object ToolSpec extends ZIOSpecDefault:

  final private case class Caller(userId: String)

  final private case class Search(text: String) derives Schema

  private val allOrders = List("alice" -> "a-1", "alice" -> "a-2", "bob" -> "b-1")

  private val orders = new Tool[Caller, Search, List[String]]:
    override def name                                                                               = "orders"
    override def description                                                                        = "Find the caller's orders"
    override def handle(context: Caller, input: Search): IO[ApplicationError, Result[List[String]]] =
      ZIO.succeed(Result.answered(allOrders.collect { case (owner, id) if owner == context.userId => id }))

  /** A map, described as an association list — the case where schema and codec used to disagree. */
  private given Schema[Map[String, Int]] = Schemas.mapAsEntries

  final private case class Report(title: String, counts: Map[String, Int]) derives Schema

  private val report = new Tool[Caller, Report, String]:
    override def name                                                                         = "report"
    override def description                                                                  = "Summarise counts"
    override def handle(context: Caller, input: Report): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.answered(s"${input.title}: ${input.counts.toList.sorted.map((k, v) => s"$k=$v").mkString(",")}"))

  /** Arguments the model cannot name — a bare string has nowhere to go in `parameters`. */
  private val unnamed = new Tool[Caller, String, String]:
    override def name                                                                         = "unnamed"
    override def description                                                                  = "takes a bare string"
    override def handle(context: Caller, input: String): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.answered(input))

  final private case class Task(goal: String) derives Schema

  final private case class Started(handle: String) derives Schema

  /** Starts work that outlives the call, and says so. */
  private val launch = new Tool[Caller, Task, Started]:
    override def name                                                                        = "launch"
    override def description                                                                 = "Start a subagent"
    override def handle(context: Caller, input: Task): IO[ApplicationError, Result[Started]] =
      val handle = s"${context.userId}/${input.goal}"
      ZIO.succeed(Result(Started(handle), Result.Standing.Promised(NonEmptyChunk(handle))))

  final private case class Await(handle: String) derives Schema

  /** Returns the result of work promised earlier. */
  private val collect = new Tool[Caller, Await, String]:
    override def name                                                                        = "collect"
    override def description                                                                 = "Take a subagent's answer"
    override def handle(context: Caller, input: Await): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result(s"done: ${input.handle}", Result.Standing.Delivered(NonEmptyChunk(input.handle))))

  /** What a conversation still owes: promised, less delivered. */
  private def outstanding(outcomes: List[Outcome]): Set[String] =
    outcomes.foldLeft(Set.empty[String]): (open, outcome) =>
      outcome.result.standing match
        case Result.Standing.Answered            => open
        case Result.Standing.Promised(handles)   => open ++ handles
        case Result.Standing.Delivered(handles)  => open -- handles

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
      yield assertTrue(first.result.value == """["a-1","a-2"]""", second.result.value == """["b-1"]""")
    },
    test("hands malformed arguments back as text instead of failing") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "orders", """{"nope":1}"""))
      yield assertTrue(outcome.result.value.startsWith("error: arguments did not parse"))
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
        outcomes(1).result.value == "error: no tool 'missing' is available",
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
        outcome.result.value == "\"t: a=1,b=2\"",
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
    test("an ordinary tool's result owes nothing") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "orders", """{"text":"x"}"""))
      yield assertTrue(outcome.result.standing == Result.Standing.Answered)
    },
    test("a promise survives rendering, and the standing is not the codec's to change") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(launch)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "launch", """{"goal":"nutrition"}"""))
      yield assertTrue(
        outcome.result.value == """{"handle":"alice/nutrition"}""",
        outcome.result.standing == Result.Standing.Promised(NonEmptyChunk("alice/nutrition")),
      )
    },
    test("a failing tool promises nothing") {
      // Everything the model can react to is text, and text that reports an error opened no work.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(launch)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "launch", """{"nope":1}"""))
      yield assertTrue(
        outcome.result.value.startsWith("error:"),
        outcome.result.standing == Result.Standing.Answered,
      )
    },
    test("what a turn still owes is promised less delivered, and no tool is named to work it out") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(launch)
        _        <- registry.add(collect)
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        opened   <- session.dispatchAll(
                      List(
                        Call("c1", "launch", """{"goal":"nutrition"}"""),
                        Call("c2", "launch", """{"goal":"pricing"}"""),
                        Call("c3", "orders", """{"text":"x"}"""),
                      )
                    )
        closed   <- session.dispatchAll(List(Call("c4", "collect", """{"handle":"alice/pricing"}""")))
      yield assertTrue(
        outstanding(opened) == Set("alice/nutrition", "alice/pricing"),
        outstanding(opened ++ closed) == Set("alice/nutrition"),
      )
    },
    test("a delivery of something never promised leaves nothing outstanding") {
      // A set difference ignores what it does not hold, which is the same reading the queue gives an id
      // it does not own: ignored rather than refused.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(collect)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "collect", """{"handle":"nobody/asked"}"""))
      yield assertTrue(outstanding(List(outcome)).isEmpty)
    },
  )
