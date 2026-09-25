package homelab.llm


import homelab.llm.Message.Content
import zio.*
import zio.test.*


/** What a conversation is waiting for is read from its messages, and from nothing else. */
object ProgressSpec extends ZIOSpecDefault:

  private def text(value: String): Chunk[Content] = Chunk(Content.Text(value))

  private def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

  private def call(value: String): Tool.Call.Raw = Tool.Call.Raw(id(value), "search", """{"text":"x"}""")

  private val asked  = Message.user(text("what do I eat tonight"))
  private val said   = Message.assistant(text("pasta"))
  private val called = Message.assistant(Chunk.empty, Chunk(call("c1"), call("c2")))

  private def answering(callId: String): Message.ToolResult =
    Message.ToolResult(id(callId), text("done"))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Progress")(
    test("nothing said is nothing to send") {
      assertTrue(Progress.from(Chunk.empty) == Progress.Empty)
    },
    test("a question nobody has answered is the model's move") {
      assertTrue(Progress.from(Chunk(asked)) == Progress.AwaitingModel)
    },
    test("an answer with nothing after it is where the conversation stopped") {
      assertTrue(Progress.from(Chunk(asked, said)) == Progress.Finished(said))
    },
    test("a question asked after an answer is the model's move again") {
      // The turn that answered is no longer the last word, so the conversation has not stopped.
      val messages = Chunk(asked, said, Message.User(text("and tomorrow")))
      assertTrue(Progress.from(messages) == Progress.AwaitingModel)
    },
    test("calls with no results yet are owed to the tools") {
      val messages = Chunk(asked, called)
      assertTrue(Progress.from(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c1"), call("c2"))))
    },
    test("a turn answered in part is still waiting, and names only what is missing") {
      // The case a suspended `wait` leaves behind: one call resolved, one still running.
      val messages = Chunk(asked, called, answering("c1"))
      assertTrue(Progress.from(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c2"))))
    },
    test("a turn answered in full is the model's move") {
      val messages = Chunk(
        asked,
        called,
        answering("c1"),
        answering("c2"),
      )
      assertTrue(Progress.from(messages) == Progress.AwaitingModel)
    },
    test("a result answering a call that was never made answers for nothing") {
      val messages = Chunk(asked, called, answering("nobody-asked"))
      assertTrue(Progress.from(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c1"), call("c2"))))
    },
    test("only the last turn decides, whatever earlier turns did") {
      // An earlier turn's calls were answered and are not owed again.
      val messages = Chunk(
        asked,
        called,
        answering("c1"),
        answering("c2"),
        said,
      )
      assertTrue(Progress.from(messages) == Progress.Finished(said))
    },
  )
