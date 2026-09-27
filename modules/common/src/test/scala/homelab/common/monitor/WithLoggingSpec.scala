package homelab.common.monitor


import zio.*
import zio.test.*


object WithLoggingSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("WithLogging")(
    test("a success is not logged, because that is what made it unusable in production") {
      for
        _      <- logging.measure("op")(ZIO.succeed(1)).exit
        output <- ZTestLogger.logOutput
      yield assertTrue(output.isEmpty)
    },
    test("a typed failure is logged") {
      for
        _      <- logging.measure("op")(ZIO.fail("nope")).exit
        output <- ZTestLogger.logOutput
      yield assertTrue(output.size == 1, output.headOption.exists(_.message().contains("Operation op failed")))
    },
    test("a defect is logged, with its cause rather than its toString") {
      // The failure most worth a line, and the one `tapError` used to miss.
      for
        _      <- logging.measure("op")(ZIO.dieMessage("kaboom")).exit
        output <- ZTestLogger.logOutput
      yield assertTrue(
        output.size == 1,
        output.headOption.exists(_.cause.dieOption.exists(_.getMessage.contains("kaboom"))),
      )
    },
    test("a cancelled fiber writes nothing") {
      // Interruption is not a failure: a consumer whose call was cancelled is not an incident.
      for
        fiber  <- logging.measure("op")(ZIO.never).fork
        _      <- fiber.interrupt
        output <- ZTestLogger.logOutput
      yield assertTrue(output.isEmpty)
    },
  )

  /** The monitor under test, wrapping one that does nothing of its own. */
  private object Support {

    val logging = Monitor.WithLogging(Monitor.Noop)
  }
