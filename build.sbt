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
val zioSchemaVersion      = "1.8.6"
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
// The release workflow sets RELEASE_VERSION from the tag (`v0.1.0` -> `0.1.0`). Everywhere else this is a
// snapshot, so a local build cannot accidentally claim a release number.
ThisBuild / version      := sys.env.getOrElse("RELEASE_VERSION", "0.1.0-SNAPSHOT")


ThisBuild / scalacOptions ++= Seq(
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
)


// Publishing — this repo's own GitHub Packages Maven registry.
//
// Releases are cut by CI when a GitHub Release is published (.github/workflows/release.yml), never from a
// laptop, so a published version always corresponds to a commit CI built and tested. Publishing needs no secret: Actions' built-in
// GITHUB_TOKEN can write to its own repo's registry.
//
// *Consumers* do need a classic PAT with `read:packages` — GitHub Packages serves Maven to authenticated
// callers only, public repo or not (the container registry is the sole anonymous one). The consumer-side
// recipe is docs/learning-material/using-modules-as-a-dependency.md.
ThisBuild / publishMavenStyle := true
ThisBuild / publishTo         := Some(
  "GitHub Packages" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"
)
ThisBuild / credentials ++= githubCredentials

// POM metadata, which is also what makes the package page on GitHub link back to this repo.
ThisBuild / homepage := Some(url("https://github.com/AndreMeira/homelab-toolkit-zio"))
ThisBuild / scmInfo  := Some(
  ScmInfo(
    url("https://github.com/AndreMeira/homelab-toolkit-zio"),
    "scm:git:git@github.com:AndreMeira/homelab-toolkit-zio.git",
  )
)

// From the environment in CI, from ~/.sbt/1.0/credentials on a laptop, and never from a file in the repo.
// The realm is fixed by GitHub — "GitHub Package Registry" — and sbt matches credentials on (realm, host),
// so a wrong realm fails as a 401 that reads like a bad token.
def githubCredentials: Seq[Credentials] =
  (sys.env.get("GITHUB_ACTOR"), sys.env.get("GITHUB_TOKEN")) match {
    case (Some(actor), Some(token)) =>
      Seq(Credentials("GitHub Package Registry", "maven.pkg.github.com", actor, token))
    case _ =>
      val local = Path.userHome / ".sbt" / "1.0" / "credentials"
      if (local.exists) Seq(Credentials(local)) else Nil
  }


lazy val common = project
  .in(file("modules/common"))
  .settings(
    name := "homelab-common",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"                       % zioVersion,
      "dev.zio" %% "zio-prelude"               % zioPreludeVersion,
      "dev.zio" %% "zio-logging"               % zioLoggingVersion,
      "dev.zio" %% "zio-logging-slf4j2-bridge" % zioLoggingVersion,
      // A reified description of a type, from which both a codec and an advertised schema derive — one
      // source instead of two hand-written artefacts that drift. In `common` beside `data/Codec` rather than
      // quarantined in a module: it describes our own data, it reaches no external system.
      "dev.zio" %% "zio-schema"                % zioSchemaVersion,
      "dev.zio" %% "zio-schema-derivation"     % zioSchemaVersion, // the `derives Schema` macro
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
    // zio-schema-json wants zio-json 0.10, the jwt-scala sketches pin 0.7. Sketches are not published and
    // their tests are not gated by CI, so take the newer rather than hold the experiment back — if a jwt
    // sketch breaks on it, that is a signal to prune it (it is already on the list) rather than to downgrade.
    libraryDependencySchemes += "dev.zio" %% "zio-json" % VersionScheme.Always,
    scalacOptions  := Nil, // experiment area — Java-interop heavy; skip the strict prod flags
    libraryDependencies ++= Seq(
      "com.github.jwt-scala"          %% "jwt-zio-json"   % jwtVersion,
      "dev.zio"                       %% "zio-http"       % zioHttpVersion,
      "com.softwaremill.sttp.client4" %% "zio"            % sttpVersion, // llm sketch: sttp4 ZIO backend + SSE (core/model/shared-zio transitively)
      "dev.zio"                       %% "zio-streams"    % zioVersion, // adapter-internal only (NATS callback bridge); never surfaced
      "io.nats"                        % "jnats"          % "2.20.5", // NATS exploration sketch (messaging/nats)
      // Codecs derived from the *same* zio-schema the advertised JSON Schema comes from — one description,
      // so what a model is told to send is what the decoder reads. See docs/sessions/2026-08-17.
      "dev.zio"                       %% "zio-schema-json" % zioSchemaVersion,
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
