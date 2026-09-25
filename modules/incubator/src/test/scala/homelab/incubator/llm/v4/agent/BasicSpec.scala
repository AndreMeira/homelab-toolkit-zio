package homelab.incubator.llm.v4.agent


import homelab.common.error.ApplicationError
import homelab.common.store.KeyValueStore
import homelab.incubator.llm.v4.playground.agent.Basic
import homelab.incubator.llm.v4.{Message, Model, Registry, Tool}
import zio.schema.{Schema, derived}
import zio.test.*
import zio.{Chunk, IO, Ref, Scope, UIO, ZIO}


/** The loop the simplest agent runs, driven by nothing but the conversation it has so far. */
object BasicSpec extends ZIOSpecDefault:

  final case class Where(city: String) derives Schema

  final case class Reading(degrees: Double) derives Schema

  private val weather: Tool[Unit, Where, Reading] =
    Tool.Definition("weather", "Report the temperature.") { (_: Unit) => (input: Where) =>
      ZIO.succeed(Tool.Result.success(Reading(if input.city == "Hamburg" then 12.0 else 20.0)))
    }

  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

  private val usage = Model.Usage(0, 0, None)

  private def said(value: String): Model.Completion =
    Model.Completion(text(value), Chunk.empty, Model.FinishReason.Stop, usage)

  private def asks(callId: String, arguments: String): Model.Completion =
    Model.Completion(
      Chunk.empty,
      Chunk(Tool.Call.Raw(Tool.Call.Id(callId), "weather", arguments)),
      Model.FinishReason.ToolCalls,
      usage,
    )

  /** A model that reads its answers off a script, and keeps every request it was handed. */
  private final class Scripted(script: Ref[List[Model.Completion]], seen: Ref[Chunk[Model.Request]])
      extends Model[Nothing]:

    override def complete(model: Model.Name, request: Model.Request): IO[Nothing, Model.Completion] =
      seen.update(_ :+ request) *> script.modify(take)

    private def take(remaining: List[Model.Completion]): (Model.Completion, List[Model.Completion]) =
      remaining match
        case head :: tail => (head, tail)
        case Nil          => (said("nothing left to say"), Nil)

  private final class Weatherman(
    override val model: Model.Fixed[ApplicationError.AdapterError],
    override val budget: Int = 16,
  ) extends Basic[Unit]:
    override val name: String          = "weatherman"
    override val systemPrompt: String  = "Answer with the temperature."
    override val tools: Registry[Unit] = Registry.add(weather)
    override val context: Unit         = ()

  private def weatherman(model: Scripted, budget: Int = 16): Weatherman =
    Weatherman(model.fixed(Model.Name("canned")), budget)

  private def scripted(completions: Model.Completion*): UIO[(Scripted, Ref[Chunk[Model.Request]])] =
    for
      script <- Ref.make(completions.toList)
      seen   <- Ref.make(Chunk.empty[Model.Request])
    yield (Scripted(script, seen), seen)

  private def toolAnswers(request: Model.Request): Chunk[Message.Content] =
    request.messages.collect { case Message.ToolResult(_, content) => content }.flatten

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Basic")(
    test("calls a tool the model asked for, then answers with what the model said next") {
      for
        (model, seen) <- scripted(asks("c1", """{"city":"Hamburg"}"""), said("12 degrees in Hamburg"))
        answer        <- weatherman(model).run("what is the weather in Hamburg?")
        requests      <- seen.get
      yield assertTrue(answer == text("12 degrees in Hamburg"), requests.size == 2)
    },
    test("the tool's answer reaches the model as the text of a tool message") {
      for
        (model, seen) <- scripted(asks("c1", """{"city":"Hamburg"}"""), said("done"))
        _             <- weatherman(model).run("what is the weather in Hamburg?")
        requests      <- seen.get
      yield assertTrue(
        toolAnswers(requests.head).isEmpty,                        // nothing has run yet
        toolAnswers(requests(1)) == text("""{"degrees":12.0}"""),  // the tool's value, written by its schema
        requests(1).messages.head == Message.System(text("Answer with the temperature.")),
      )
    },
    test("a model that only ever calls tools is stopped by the budget") {
      for
        (model, _) <- scripted(Seq.fill(10)(asks("c1", """{"city":"Hamburg"}"""))*)
        failure    <- weatherman(model, budget = 3).run("what is the weather?").flip
      yield assertTrue(failure == Basic.Exhausted("weatherman", 3))
    },
    test("a run resumed from a finished conversation answers without asking the model") {
      for
        (model, seen) <- scripted(said("should never be reached"))
        store         <- KeyValueStore.inmemory[String, Chunk[Message]]
        finished       = Chunk(Message.User(text("hello")), Message.Assistant(text("hi"), Chunk.empty))
        _             <- store.set("ask", finished)
        answer        <- weatherman(model).persisted(store).run("ask")
        requests      <- seen.get
        slot          <- store.get("ask")
      yield assertTrue(answer == text("hi"), requests.isEmpty, slot.isEmpty)
    },
  )
