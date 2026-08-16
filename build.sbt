// homelab-toolkit-zio — shared ZIO building blocks for homelab services.
// DDD + hexagonal at the module level: `common` holds data + ports, plus the in-process adapters that
// need no dependency of their own (`store/inmemory`, `messaging/inmemory`). A separate module exists to
// quarantine a third-party dependency — magnum/Hikari, OTel, the NATS client — never merely to mark a
// port/adapter boundary.
// ZIO counterpart of ../homelab-toolkit (Kyo).
// Stack rationale: ../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md
//
// Status: scaffold — only `common` DATA is populated (errors, value objects, Requester). Ports and
// adapters (magnum/inmemory/auth) come next.

val scala3Version         = "3.8.3"
val zioVersion            = "2.1.23" // keep in sync with the zio-core that zio-prelude/zio-http pull, else zio-test layer macros break
val zioPreludeVersion     = "1.0.0-RC47"
val zioLoggingVersion     = "2.5.0"
val jwtVersion            = "11.0.4"
val zioHttpVersion        = "3.0.1"
val magnumVersion         = "1.3.1"
val flywayVersion         = "12.9.0"
val postgresqlVersion     = "42.7.11"
val hikariVersion         = "7.1.0"
val zioOtelVersion        = "3.1.13"
val otelVersion           = "1.57.0"
val fabric8Version        = "6.0.0"
val testcontainersVersion = "1.20.6"
val sttpVersion           = "4.0.9"

ThisBuild / scalaVersion := scala3Version
ThisBuild / organization := "com.andremeira.homelab"
ThisBuild / version      := "0.1.0-SNAPSHOT"


ThisBuild / scalacOptions ++= Seq(
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
)


lazy val common = project
  .in(file("modules/common"))
  .settings(
    name := "homelab-common",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"                       % zioVersion,
      "dev.zio" %% "zio-prelude"               % zioPreludeVersion,
      "dev.zio" %% "zio-logging"               % zioLoggingVersion,
      "dev.zio" %% "zio-logging-slf4j2-bridge" % zioLoggingVersion,
      "dev.zio" %% "zio-test"                  % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt"              % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


// Persistence adapter — Postgres via Magnum (effect-agnostic; blocking JDBC lifted with ZIO.attemptBlocking,
// no cats-effect). Flyway for migrations. See docs/sessions for the Magnum-vs-doobie rationale.
lazy val postgres = project
  .in(file("modules/postgres"))
  .dependsOn(common)
  .settings(
    name := "homelab-postgres",
    libraryDependencies ++= Seq(
      "com.augustnagro"   %% "magnum"                     % magnumVersion,
      "com.augustnagro"   %% "magnumpg"                   % magnumVersion,
      "org.postgresql"     % "postgresql"                 % postgresqlVersion,
      "com.zaxxer"         % "HikariCP"                   % hikariVersion,
      "org.flywaydb"       % "flyway-core"                % flywayVersion,
      "org.flywaydb"       % "flyway-database-postgresql" % flywayVersion,
      "dev.zio"           %% "zio-test"                   % zioVersion            % Test,
      "dev.zio"           %% "zio-test-sbt"               % zioVersion            % Test,
      "org.testcontainers" % "postgresql"                 % testcontainersVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


// Telemetry adapter — OpenTelemetry implementation of the common `Monitor` port (spans + metrics), via
// zio-telemetry. The app wires the Tracing/Meter layers; the toolkit provides the OtelMonitor class.
lazy val telemetry = project
  .in(file("modules/telemetry"))
  .dependsOn(common)
  .settings(
    name := "homelab-telemetry",
    libraryDependencies ++= Seq(
      "dev.zio"         %% "zio-opentelemetry" % zioOtelVersion,
      "io.opentelemetry" % "opentelemetry-api" % otelVersion,
      "dev.zio"         %% "zio-test"          % zioVersion % Test,
      "dev.zio"         %% "zio-test-sbt"      % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


lazy val auth = project
  .in(file("modules/auth"))
  .dependsOn(common)
  .settings(
    name := "homelab-auth",
    libraryDependencies ++= Seq(
      "io.fabric8"            % "kubernetes-client" % fabric8Version,
      "com.github.jwt-scala" %% "jwt-zio-json"      % jwtVersion, // brings zio-json transitively; JDK HttpClient for transport (no zio-http)
      "dev.zio"              %% "zio-test"          % zioVersion % Test,
      "dev.zio"              %% "zio-test-sbt"      % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


// Messaging adapter — NATS (Core NATS ephemeral pub/sub + JetStream durable delivery), promoted from the
// llm's messaging/nats sketches. Implements the common `messaging` ports; ZStream is an internal
// bridge detail (never surfaced). Integration tests via Testcontainers (a JetStream-enabled nats server).
lazy val nats = project
  .in(file("modules/nats"))
  .dependsOn(common)
  .settings(
    name := "homelab-nats",
    libraryDependencies ++= Seq(
      "dev.zio"           %% "zio-streams"    % zioVersion,
      "io.nats"            % "jnats"          % "2.20.5",
      "dev.zio"           %% "zio-test"       % zioVersion            % Test,
      "dev.zio"           %% "zio-test-sbt"   % zioVersion            % Test,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


// Incubator — throwaway sketches / experiments (the ZIO answer to Kyo's playground). Not published.
lazy val incubator = project
  .in(file("modules/incubator"))
  .dependsOn(common)
  .settings(
    name           := "homelab-incubator",
    publish / skip := true,
    scalacOptions  := Nil, // experiment area — Java-interop heavy; skip the strict prod flags
    libraryDependencies ++= Seq(
      "com.github.jwt-scala"          %% "jwt-zio-json"   % jwtVersion,
      "dev.zio"                       %% "zio-http"       % zioHttpVersion,
      "com.softwaremill.sttp.client4" %% "zio"            % sttpVersion, // llm sketch: sttp4 ZIO backend + SSE (core/model/shared-zio transitively)
      "dev.zio"                       %% "zio-streams"    % zioVersion, // adapter-internal only (NATS callback bridge); never surfaced
      "io.nats"                        % "jnats"          % "2.20.5", // NATS exploration sketch (messaging/nats)
      "dev.zio"                       %% "zio-test"       % zioVersion            % Test,
      "dev.zio"                       %% "zio-test-sbt"   % zioVersion            % Test,
      "org.testcontainers"             % "testcontainers" % testcontainersVersion % Test, // NATS via GenericContainer
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    // Sketches are not gated by CI. They still *compile* — that is what catches an experiment rotting
    // against a change in `common` — but running them is on demand (`incubator/testOnly …`), because they
    // spin up brokers, race on timing, and are abandoned in place rather than maintained.
    Test / test := {
      val _ = (Test / compile).value
      streams.value.log.info("incubator: sketches compiled, not run — use `incubator/testOnly <spec>`")
    },
  )


lazy val root = project
  .in(file("."))
  .aggregate(common, postgres, telemetry, auth, incubator, nats)
  .settings(
    name           := "homelab-toolkit-zio",
    publish / skip := true,
  )
