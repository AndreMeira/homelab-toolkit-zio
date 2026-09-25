package homelab.incubator.llm.v4


import homelab.incubator.llm.v4.Message.Content
import zio.test.*
import zio.{ Chunk, Scope }


/** What an adapter may join before sending, and what it may not. */
object MessageSpec extends ZIOSpecDefault:

  private def text(value: String): Chunk[Content] = Chunk(Content.Text(value))

  private def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

  private def call(value: String): Tool.Call.Raw = Tool.Call.Raw(id(value), "search", "{}")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Message")(
    test("adjacent user messages become one, keeping their parts in order") {
      val messages = Chunk(Message.User(text("what is the weather")), Message.User(text("in Hamburg")))
      assertTrue(
        Message.merged(messages) ==
          Chunk(Message.User(text("what is the weather") ++ text("in Hamburg")))
      )
    },
    test("adjacent system messages become one") {
      val messages = Chunk(Message.System(text("be brief")), Message.System(text("be kind")))
      assertTrue(Message.merged(messages).size == 1)
    },
    test("two assistant turns stay two, because each carries its own calls") {
      val messages = Chunk(
        Message.Assistant(Chunk.empty, Chunk(call("c1"))),
        Message.Assistant(text("done"), Chunk.empty),
      )
      assertTrue(Message.merged(messages) == messages)
    },
    test("tool results stay apart, because each answers one call") {
      val messages = Chunk(
        Message.ToolResult(id("c1"), text("one")),
        Message.ToolResult(id("c2"), text("two")),
      )
      assertTrue(Message.merged(messages) == messages)
    },
    test("a conversation that already alternates is left alone") {
      val messages = Chunk(
        Message.System(text("be brief")),
        Message.User(text("hello")),
        Message.Assistant(text("hi"), Chunk.empty),
        Message.User(text("again")),
      )
      assertTrue(Message.merged(messages) == messages)
    },
  )
