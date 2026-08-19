package homelab.incubator.llm


import homelab.common.data.Codec.{ Decoder, Encoder }
import homelab.common.error.ApplicationError
import homelab.incubator.llm.v1.ToolSketch.*
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets.UTF_8


/**
 * What the `Ctx` parameter is worth, demonstrated rather than asserted: the same model-supplied arguments
 * reach different data for different callers, a tool a caller may not use is never advertised *and* refused
 * if named anyway, and a tool needing no context shares the registry with tools that do.
 */
object ToolSketchSpec extends ZIOSpecDefault:

  /** The trusted half of a tool's arguments: who is calling. Never serialised, never in a schema. */
  private final case class Caller(userId: String, admin: Boolean)

  /** The untrusted half: what the model chose. */
  private final case class Search(text: String)

  private final case class Order(id: String, owner: String)

  private given Decoder[Search] with
    override def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, Search] =
      Right(Search(new String(value, UTF_8)))

  private given Encoder[String] with
    override def encode(value: String): Array[Byte] = value.getBytes(UTF_8)

  /** Every order in the system — the thing a leak would expose. */
  private val allOrders = List(Order("a-1", "alice"), Order("a-2", "alice"), Order("b-1", "bob"))

  /** Context-bound: the namespace comes from the caller, never from the model. */
  private val orders: Tool[Caller, Search, String] = new Tool[Caller, Search, String]:
    override def name    = "orders"
    override def schema  = """{"type":"object","properties":{"text":{"type":"string"}}}"""
    override def decoder = summon[Decoder[Search]]
    override def encoder = summon[Encoder[String]]
    override def handle(context: Caller, args: Search): IO[ApplicationError, String] =
      ZIO.succeed(allOrders.filter(_.owner == context.userId).map(_.id).mkString(","))

  /** Admin-only: absent from a non-admin session entirely. */
  private val audit: Tool[Caller, Search, String] = new Tool[Caller, Search, String]:
    override def name                          = "audit"
    override def schema                        = """{"type":"object","properties":{}}"""
    override def decoder                       = summon[Decoder[Search]]
    override def encoder                       = summon[Encoder[String]]
    override def permits(context: Caller)      = context.admin
    override def handle(context: Caller, args: Search): IO[ApplicationError, String] =
      ZIO.succeed(allOrders.map(_.id).mkString(","))

  /** Context-free: `Tool[Any, …]` in a `Registry[Caller]`, which is what the contravariance buys. */
  private val echo: Tool[Any, Search, String] = new Tool[Any, Search, String]:
    override def name    = "echo"
    override def schema  = """{"type":"object","properties":{"text":{"type":"string"}}}"""
    override def decoder = summon[Decoder[Search]]
    override def encoder = summon[Encoder[String]]
    override def handle(context: Any, args: Search): IO[ApplicationError, String] = ZIO.succeed(args.text)

  private val registry: Registry[Caller] = Registry.of[Caller](orders, audit, echo)

  private def call(tool: String, arguments: String) = Call(s"call-$tool", tool, arguments)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ToolSketch")(
    test("confines a tool to the caller, from identical model arguments") {
      // The model sends the same thing both times; only the session differs.
      for
        alice <- registry.forSession(Caller("alice", admin = false)).dispatch(call("orders", "everything"))
        bob   <- registry.forSession(Caller("bob", admin = false)).dispatch(call("orders", "everything"))
      yield assertTrue(alice.content == "a-1,a-2", bob.content == "b-1")
    },
    test("never advertises a tool the caller may not use") {
      val user  = registry.forSession(Caller("alice", admin = false))
      val admin = registry.forSession(Caller("root", admin = true))
      assertTrue(user.schemas.size == 2, admin.schemas.size == 3)
    },
    test("refuses a forbidden tool even when the model names it anyway") {
      // Hiding stops it being tried; this check is the actual control — a resumed conversation can carry a
      // call the caller has since lost access to.
      for outcome <- registry.forSession(Caller("alice", admin = false)).dispatch(call("audit", "{}"))
      yield assertTrue(outcome.content == "error: no tool 'audit' is available", !outcome.content.contains("a-1"))
    },
    test("runs a context-free tool from the same registry") {
      for outcome <- registry.forSession(Caller("alice", admin = false)).dispatch(call("echo", "hello"))
      yield assertTrue(outcome.content == "hello")
    },
    test("keeps every outcome when several calls run in one turn") {
      for outcomes <- registry
                        .forSession(Caller("alice", admin = false))
                        .dispatchAll(List(call("orders", "x"), call("audit", "x"), call("echo", "hi")))
      yield assertTrue(
        outcomes.map(_.callId) == List("call-orders", "call-audit", "call-echo"),
        outcomes.head.content == "a-1,a-2",
        outcomes(1).content.startsWith("error:"),
      )
    },
  )
