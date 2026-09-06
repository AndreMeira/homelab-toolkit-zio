package homelab.common.monitor


import zio.*
import zio.test.*


object WithLoggingSpec extends ZIOSpecDefault:

  private val logging = Monitor.WithLogging(Monitor.Noop)

  private def logged(effect: ZIO[Any, Any, Any]): UIO[Chunk[String]] =
    logging.measure("op")(effect).exit *> ZTestLogger.logOutput.map(_.map(_.message()))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("WithLogging")(
    test("a success is not logged, because that is what made it unusable in production") {
      for lines <- logged(ZIO.succeed(1))
      yield assertTrue(lines.isEmpty)
    },
    test("a typed failure is logged") {
      for lines <- logged(ZIO.fail("nope"))
      yield assertTrue(lines.size == 1, lines.head.contains("Operation op failed"))
    },
    test("a defect is logged, with its cause rather than its toString") {
      // The failure most worth a line, and the one `tapError` used to miss.
      for
        _      <- logging.measure("op")(ZIO.dieMessage("kaboom")).exit
        output <- ZTestLogger.logOutput
      yield assertTrue(
        output.size == 1,
        output.head.cause.dieOption.exists(_.getMessage.contains("kaboom")),
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
