package homelab.incubator.llm.v3


import homelab.incubator.llm.v3.Model.{ Content, Message }
import homelab.incubator.llm.v3.Transcript.Progress
import zio.*
import zio.test.*


/** What a conversation is waiting for is read from its messages, and from nothing else. */
object TranscriptSpec extends ZIOSpecDefault:

  private def text(value: String): Chunk[Content] = Chunk(Content.Text(value))

  private def call(id: String): Tool.Call = Tool.Call(id, "search", """{"text":"x"}""")

  private val asked  = Message.User(text("what do I eat tonight"))
  private val said   = Message.Assistant(text("pasta"), Chunk.empty)
  private val called = Message.Assistant(Chunk.empty, Chunk(call("c1"), call("c2")))

  private def promising(callId: String, handle: String): Message.ToolResult =
    Message.ToolResult(callId, text("launched"), Tool.Result.Standing.Promised(NonEmptyChunk(handle)))

  private def delivering(callId: String, handle: String): Message.ToolResult =
    Message.ToolResult(callId, text("here it is"), Tool.Result.Standing.Delivered(NonEmptyChunk(handle)))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Transcript")(
    test("nothing said is nothing to send") {
      assertTrue(Transcript.progress(Chunk.empty) == Progress.Empty)
    },
    test("a question nobody has answered is the model's move") {
      assertTrue(Transcript.progress(Chunk(asked)) == Progress.AwaitingModel)
    },
    test("an answer with nothing after it is where the conversation stopped") {
      assertTrue(Transcript.progress(Chunk(asked, said)) == Progress.Finished(text("pasta")))
    },
    test("a question asked after an answer is the model's move again") {
      // The turn that answered is no longer the last word, so the conversation has not stopped.
      val messages = Chunk(asked, said, Message.User(text("and tomorrow")))
      assertTrue(Transcript.progress(messages) == Progress.AwaitingModel)
    },
    test("calls with no results yet are owed to the tools") {
      val messages = Chunk(asked, called)
      assertTrue(Transcript.progress(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c1"), call("c2"))))
    },
    test("a turn answered in part is still waiting, and names only what is missing") {
      // The case a suspended `wait` leaves behind: one call resolved, one still running.
      val messages = Chunk(asked, called, Message.ToolResult("c1", text("done")))
      assertTrue(Transcript.progress(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c2"))))
    },
    test("a turn answered in full is the model's move") {
      val messages = Chunk(
        asked,
        called,
        Message.ToolResult("c1", text("done")),
        Message.ToolResult("c2", text("done")),
      )
      assertTrue(Transcript.progress(messages) == Progress.AwaitingModel)
    },
    test("a result answering a call that was never made answers for nothing") {
      val messages = Chunk(asked, called, Message.ToolResult("nobody-asked", text("done")))
      assertTrue(Transcript.progress(messages) == Progress.AwaitingTools(NonEmptyChunk(call("c1"), call("c2"))))
    },
    test("a conversation that started nothing owes nothing") {
      assertTrue(!Transcript.outstanding(Chunk(asked, said)).pending)
    },
    test("work promised and not delivered is still owed") {
      val messages = Chunk(asked, called, promising("c1", "nutrition"), promising("c2", "pricing"))
      assertTrue(
        Transcript.outstanding(messages).handles == Set("nutrition", "pricing"),
        Transcript.outstanding(messages).pending,
      )
    },
    test("a delivery settles what it names and leaves the rest") {
      val messages = Chunk(asked, called, promising("c1", "nutrition"), promising("c2", "pricing"))
      val later    = messages :+ delivering("c3", "pricing")
      assertTrue(Transcript.outstanding(later).handles == Set("nutrition"))
    },
    test("delivering something never promised answers for nothing") {
      assertTrue(!Transcript.outstanding(Chunk(asked, delivering("c1", "never-asked"))).pending)
    },
    test("a conversation can have stopped and still owe results") {
      // The subagent nobody waited for: the turn answered, the work is still running.
      val messages = Chunk(asked, called, promising("c1", "nutrition"), said)
      assertTrue(
        Transcript.progress(messages) == Progress.Finished(text("pasta")),
        Transcript.outstanding(messages).pending,
      )
    },
    test("only the last turn decides, whatever earlier turns did") {
      // An earlier turn's calls were answered and are not owed again.
      val messages = Chunk(
        asked,
        called,
        Message.ToolResult("c1", text("done")),
        Message.ToolResult("c2", text("done")),
        said,
      )
      assertTrue(Transcript.progress(messages) == Progress.Finished(text("pasta")))
    },
  )
