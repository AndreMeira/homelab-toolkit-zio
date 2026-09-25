package homelab.incubator.llm.v3.compiletime


import homelab.llm.schema.{ Node, Shape }
import zio.Scope
import zio.test.*

import scala.compiletime.testing.typeChecks


/** Describability decided by implicit search: what has no instance has no schema, and does not compile. */
object GeneratedSpec extends ZIOSpecDefault:

  final case class Weather(city: String, days: Option[Int], tags: List[String])

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Generated")(
    test("a describable record derives, optionality included") {
      assertTrue(
        summon[Generated[Weather]].node == Node.obj(
          Shape.Obj.Field("city", Node.text),
          Shape.Obj.Field("days", Node.nullable(Node.integer), required = false),
          Shape.Obj.Field("tags", Node.array(Node.text)),
        )
      )
    },
    test("a map is refused while compiling, because it has no instance") {
      assertTrue(
        !typeChecks("""
          final case class Readings(station: String, byHour: Map[String, Int])
          summon[Generated[Readings]]
        """)
      )
    },
    test("a recursive type is refused too — the cost of deciding this by implicit search") {
      assertTrue(
        !typeChecks("""
          final case class Tree(value: String, children: List[Tree])
          summon[Generated[Tree]]
        """)
      )
    },
  )
