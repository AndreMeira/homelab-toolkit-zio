package homelab.incubator.llm.v3


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v2.Schemas
import homelab.incubator.llm.v3.Tool.{ Call, Outcome, Result }
import zio.*
import zio.json.*
import zio.schema.{ Schema, derived }
import zio.schema.annotation.discriminatorName
import zio.test.*


/**
 * Registration is where a tool meets the wire; dispatch is where the caller meets the model's arguments.
 * v3 adds what a result leaves outstanding, so the fold over a turn is here too.
 */
object ToolSpec extends ZIOSpecDefault:

  /** A map, described as an association list — the case where schema and codec used to disagree. */
  private given Schema[Map[String, Int]] = Schemas.mapAsEntries

  final private case class Caller(userId: String)
  final private case class Search(text: String) derives Schema
  final private case class Report(title: String, counts: Map[String, Int]) derives Schema
  final private case class Task(goal: String) derives Schema
  final private case class Started(handle: String) derives Schema
  final private case class Await(handle: String) derives Schema

  /** What a dependency's encodeFailure looks like: written for an operator, not for a model. */
  final private case class Leaky(message: String) extends ApplicationError.AdapterError

  private val allOrders = List("alice" -> "a-1", "alice" -> "a-2", "bob" -> "b-1")

  private val orders = new Tool[Caller, Search, List[String]] {
    override def name        = "orders"
    override def description = "Find the caller's orders"

    override def handle(context: Caller, input: Search): IO[ApplicationError, Result[List[String]]] =
      ZIO.succeed(Result.success(allOrders.collect { case (owner, id) if owner == context.userId => id }))
  }

  private val report = new Tool[Caller, Report, String] {
    override def name        = "report"
    override def description = "Summarise counts"

    override def handle(context: Caller, input: Report): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.success(s"${input.title}: ${input.counts.toList.sorted.map((k, v) => s"$k=$v").mkString(",")}"))
  }

  /** Arguments the model cannot name — a bare string has nowhere to go in `parameters`. */
  private val unnamed = new Tool[Caller, String, String] {
    override def name        = "unnamed"
    override def description = "takes a bare string"

    override def handle(context: Caller, input: String): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.success(input))
  }

  /** A tool that aborts rather than returning a encodeFailure it chose. */
  private val fragile = new Tool[Caller, Search, String] {
    override def name        = "fragile"
    override def description = "Aborts"

    override def handle(context: Caller, input: Search): IO[ApplicationError, Result[String]] =
      ZIO.fail(Leaky("""relation "billing.customer_card" does not exist; dsn=postgres://ops:hunter2@db"""))
  }

  /** Starts work that outlives the call, and says so. */
  private val launch = new Tool[Caller, Task, Started] {
    override def name        = "launch"
    override def description = "Start a subagent"

    override def handle(context: Caller, input: Task): IO[ApplicationError, Result[Started]] =
      val handle = s"${context.userId}/${input.goal}"
      ZIO.succeed(Result.Succeeded(Started(handle), Result.Standing.Promised(NonEmptyChunk(handle))))
  }

  /** Returns the result of work promised earlier. */
  private val collect = new Tool[Caller, Await, String] {
    override def name        = "collect"
    override def description = "Take a subagent's answer"

    override def handle(context: Caller, input: Await): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.Succeeded(s"done: ${input.handle}", Result.Standing.Delivered(NonEmptyChunk(input.handle))))
  }

  /** How a run may end, as one tool both takes and returns it. */
  @discriminatorName("kind")
  private enum Ending derives Schema:
    case Done(summary: String)
    case GiveUp(reason: String)

  /** The request carrying an ending: an object, because `parameters` must be one — a sum type is refused. */
  final private case class Terminate(ending: Ending) derives Schema

  /** A tool whose produced value is what the loop acts on. */
  private val terminate = new Tool[Caller, Terminate, Ending] {
    override def name        = "terminate"
    override def description = "End the run"

    override def handle(context: Caller, input: Terminate): IO[ApplicationError, Result[Ending]] =
      ZIO.succeed(Result.success(input.ending))
  }

  /** Only root may use it, and the lookup is an effect like any other. */
  private val privileged = new Tool[Caller, Search, String] {
    override def name        = "privileged"
    override def description = "Root only"

    override def permits(context: Caller): IO[ApplicationError, Boolean] =
      ZIO.succeed(context.userId == "root")

    override def handle(context: Caller, input: Search): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.success("granted"))
  }

  /** Its permission cannot be established, which is neither a yes nor a no. */
  private val unanswerable = new Tool[Caller, Search, String] {
    override def name        = "unanswerable"
    override def description = "Cannot say"

    override def permits(context: Caller): IO[ApplicationError, Boolean] =
      ZIO.fail(Leaky("the grant service did not answer"))

    override def handle(context: Caller, input: Search): IO[ApplicationError, Result[String]] =
      ZIO.succeed(Result.success("unreachable"))
  }

  /** What a conversation still owes: promised, less delivered. A encodeFailure started nothing. */
  private def outstanding(outcomes: List[Outcome]): Set[String] =
    outcomes.foldLeft(Set.empty[String]): (opened, outcome) =>
      outcome.result match
        case Result.Succeeded(_, Result.Standing.Promised(handles))  => opened ++ handles
        case Result.Succeeded(_, Result.Standing.Delivered(handles)) => opened -- handles
        case _                                                       => opened

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
      yield assertTrue(first.result.render.text == """["a-1","a-2"]""", second.result.render.text == """["b-1"]""")
    },
    test("hands malformed arguments back as a failed result instead of aborting") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "orders", """{"nope":1}"""))
      yield assertTrue(outcome.result.failed, outcome.result.render.text.contains("arguments did not parse"))
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
        outcomes(1).result.failed,
        outcomes(1).result.render.text == """{"isError":true,"reason":"no tool 'missing' is available"}""",
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
        outcome.result.render.text == "\"t: a=1,b=2\"",
      )
    },
    test("a tool that refuses this caller is never advertised, and cannot be called") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(orders)
        _        <- registry.add(privileged)
        alice    <- registry.forSession(Caller("alice"))
        root     <- registry.forSession(Caller("root"))
        refused  <- alice.dispatch(Call("c1", "privileged", "{}"))
      yield assertTrue(
        alice.advertised.map(_.toJson).mkString.contains("privileged") == false,
        root.advertised.map(_.toJson).mkString.contains("privileged"),
        refused.result.failed,
      )
    },
    test("a permission the registry cannot establish refuses the session") {
      // Neither allowed nor denied: a session that cannot be built is better than one built on a guess.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(unanswerable)
        session  <- registry.forSession(Caller("alice")).either
      yield assertTrue(session.isLeft)
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
      // Unrendered, so the value is the one the tool made rather than its text.
      yield assertTrue(outcome.result == Result.Succeeded(List("a-1", "a-2"), Result.Standing.Answered))
    },
    test("a promise survives rendering, and the standing is not the codec's to change") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(launch)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "launch", """{"goal":"nutrition"}"""))
      yield assertTrue(
        // The value is the tool's own until something renders it; the standing is untouched either way.
        outcome.result == Result.Succeeded(
          Started("alice/nutrition"),
          Result.Standing.Promised(NonEmptyChunk("alice/nutrition")),
        ),
        outcome.result.render.text == """{"handle":"alice/nutrition"}""",
      )
    },
    test("a failing tool says so in the type, not in a prefix") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(launch)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "launch", """{"nope":1}"""))
      yield assertTrue(
        outcome.result.failed,
        outcome.result.render.text.contains("arguments did not parse"),
        outstanding(List(outcome)).isEmpty,
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
    test("an abort tells the model it failed and nothing else") {
      // The message on an ApplicationError is written for an operator and can name a table, a host or a
      // credential. It reaches a log; what reaches the conversation is that the call did not complete.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(fragile)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "fragile", """{"text":"x"}"""))
      yield assertTrue(
        outcome.result.failed,
        outcome.result.render.text.contains(Tool.Registry.Withheld),
        !outcome.result.render.text.contains("billing.customer_card"),
        !outcome.result.render.text.contains("hunter2"),
      )
    },
    test("arguments the model got wrong are told to the model, since they are its to fix") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(fragile)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "fragile", """{"nope":1}"""))
      yield assertTrue(
        outcome.result.failed,
        outcome.result.render.text.contains("arguments did not parse"),
        !outcome.result.render.text.contains(Tool.Registry.Withheld),
      )
    },
    test("a produced value survives dispatch and is matched by type, not read back from text") {
      // The static types were erased at registration; the loop tests the case at runtime and gets the
      // typed value, which is what lets a tool's answer decide the loop without a second arguments.
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(terminate)
        _        <- registry.add(orders)
        session  <- registry.forSession(Caller("alice"))
        outcomes <- session.dispatchAll(
                      List(
                        Call("c1", "orders", """{"text":"x"}"""),
                        Call("c2", "terminate", """{"ending":{"kind":"GiveUp","reason":"stuck"}}"""),
                      )
                    )
        ending    = outcomes.collectFirst { case Outcome(_, Result.Succeeded(e: Ending, _)) => e }
      yield assertTrue(
        ending == Some(Ending.GiveUp("stuck")),
        outcomes(1).result.render.text == """{"kind":"GiveUp","reason":"stuck"}""",
      )
    },
    test("a failed call leaves the loop nothing to match") {
      for
        registry <- Tool.Registry.make[Caller]
        _        <- registry.add(terminate)
        session  <- registry.forSession(Caller("alice"))
        outcome  <- session.dispatch(Call("c1", "terminate", """{"ending":{"kind":"Nope"}}"""))
        ending    = List(outcome).collectFirst { case Outcome(_, Result.Succeeded(e: Ending, _)) => e }
      yield assertTrue(outcome.result.failed, ending.isEmpty)
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
