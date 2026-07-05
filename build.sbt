// homelab-toolkit-zio — shared ZIO building blocks for homelab services.
// DDD + hexagonal at the module level: `common` holds data + ports; each adapter is its own module.
// ZIO counterpart of ../homelab-toolkit (Kyo).
// Stack rationale: ../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md
//
// Status: scaffold — only `common` DATA is populated (errors, value objects, Requester). Ports and
// adapters (magnum/inmemory/auth) come next.

val scala3Version     = "3.3.4"
val zioVersion        = "2.1.14"
val zioPreludeVersion = "1.0.0-RC47"
val jwtVersion        = "11.0.4"

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
      "dev.zio" %% "zio"          % zioVersion,
      "dev.zio" %% "zio-prelude"  % zioPreludeVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

// Adapter modules — declared to mirror the structure; empty until we implement the ports.
// NOTE: `magnum` is a placeholder name for the persistence adapter. Magnum is effect-agnostic (usable
// with ZIO), but the ZIO reference used doobie — pick the lib when we implement (may rename).
lazy val magnum = project
  .in(file("modules/magnum"))
  .dependsOn(common)
  .settings(name := "homelab-magnum")

lazy val inmemory = project
  .in(file("modules/inmemory"))
  .dependsOn(common)
  .settings(name := "homelab-inmemory")

lazy val auth = project
  .in(file("modules/auth"))
  .dependsOn(common)
  .settings(name := "homelab-auth")

// Incubator — throwaway sketches / experiments (the ZIO answer to Kyo's playground). Not published.
lazy val incubator = project
  .in(file("modules/incubator"))
  .dependsOn(common)
  .settings(
    name           := "homelab-incubator",
    publish / skip := true,
    scalacOptions  := Nil, // experiment area — Java-interop heavy; skip the strict prod flags
    libraryDependencies ++= Seq(
      "com.github.jwt-scala" %% "jwt-zio-json" % jwtVersion,
      "dev.zio"              %% "zio-test"     % zioVersion % Test,
      "dev.zio"              %% "zio-test-sbt" % zioVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

lazy val root = project
  .in(file("."))
  .aggregate(common, magnum, inmemory, auth, incubator)
  .settings(
    name           := "homelab-toolkit-zio",
    publish / skip := true,
  )
