package homelab.incubator.llm.v4


import zio.json.ast.Json
import zio.schema.{ Schema, derived }
import zio.test.*
import zio.{ Scope, ZIO }


/** A registry is a value: what a holder added is what its sessions offer. */
object RegistrySpec extends ZIOSpecDefault:

  final case class Where(city: String) derives Schema

  final case class Reading(degrees: Double) derives Schema

  /** Arguments no model can be asked to fill, so the tool taking them cannot be registered. */
  final case class Readings(byHour: Map[String, Int]) derives Schema

  private val weather: Tool[Unit, Where, Reading] =
    Tool.Definition("weather", "Report the temperature.") { (_: Unit) => (_: Where) =>
      ZIO.succeed(Tool.Result.success(Reading(12.0)))
    }

  private val forecast: Tool[Unit, Where, Reading] =
    Tool.Definition("forecast", "Report tomorrow's temperature.") { (_: Unit) => (_: Where) =>
      ZIO.succeed(Tool.Result.success(Reading(9.0)))
    }

  private val undescribable: Tool[Unit, Readings, Reading] =
    Tool.Definition("readings", "Report a whole day.") { (_: Unit) => (_: Readings) =>
      ZIO.succeed(Tool.Result.success(Reading(0.0)))
    }

  private val alsoUndescribable: Tool[Unit, Readings, Reading] =
    Tool.Definition("more_readings", "Report another whole day.") { (_: Unit) => (_: Readings) =>
      ZIO.succeed(Tool.Result.success(Reading(0.0)))
    }

  private val bareString: Tool[Unit, String, Reading] =
    Tool.Definition("echo", "Echo a city name.") { (_: Unit) => (_: String) =>
      ZIO.succeed(Tool.Result.success(Reading(0.0)))
    }

  /** The registry as a plain value — no effect, no builder, no build step. */
  private val registry: Registry[Unit] = Registry.empty[Unit].add(weather)

  private def rendered(session: Session[Unit]): String = session.advertised.map(asString).mkString(",")

  private def asString(advertised: Json): String = advertised.toString

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Registry")(
    test("adding a tool leaves the registry it was added to alone") {
      val extended = registry.add(forecast)
      for
        original <- registry.forSession(())
        wider    <- extended.forSession(())
      yield assertTrue(
        rendered(original).contains(""""name":"weather""""),
        !rendered(original).contains("forecast"),
        rendered(wider).contains(""""name":"weather""""),
        rendered(wider).contains(""""name":"forecast""""),
      )
    },
    test("a tool whose arguments cannot be described is set aside, not registered") {
      val attempted = registry.add(undescribable)
      assertTrue(
        attempted.rejections.map(_.tool) == zio.Chunk("readings"),
        registry.rejections.isEmpty,
      )
    },
    test("every rejection is reported, not the first") {
      val attempted = registry.add(undescribable).add(alsoUndescribable)
      for failure <- attempted.forSession(()).flip
      yield assertTrue(
        failure.message.contains("readings"),
        failure.message.contains("more_readings"),
      )
    },
    test("a rejection says which of the two things went wrong") {
      val undescribed = registry.add(undescribable).rejections.map(_.cause.message).mkString
      val notAnObject = Registry.empty[Unit].add(bareString).rejections.map(_.cause.message).mkString
      assertTrue(
        undescribed.startsWith("arguments cannot be described:"),
        notAnObject == "arguments must describe an object, not a string",
      )
    },
    test("a chain of tools builds the same registry as repeated adds") {
      val chained: Registry[Unit] = weather + forecast
      val added: Registry[Unit]   = Registry.empty[Unit].add(weather).add(forecast)
      for
        fromChain <- chained.forSession(())
        fromAdds  <- added.forSession(())
      yield assertTrue(rendered(fromChain) == rendered(fromAdds))
    },
    test("a rejection in a chain is carried, not raised") {
      val chained = weather + undescribable + alsoUndescribable
      assertTrue(chained.rejections.map(_.tool) == zio.Chunk("readings", "more_readings"))
    },
    test("a session dispatches a registered tool and refuses a name it does not hold") {
      for
        session <- registry.forSession(())
        ran     <- session.dispatch(Tool.Call(Tool.Call.Id("c1"), "weather", """{"city":"Hamburg"}"""))
        missing <- session.dispatch(Tool.Call(Tool.Call.Id("c2"), "nope", "{}"))
      yield assertTrue(
        ran.result.render == """{"degrees":12.0}""",
        !ran.result.failed,
        missing.result.failed,
        missing.result.render.contains("no tool 'nope' is available"),
      )
    },
  )
