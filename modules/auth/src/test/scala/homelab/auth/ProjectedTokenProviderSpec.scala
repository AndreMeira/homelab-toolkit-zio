package homelab.auth


import homelab.common.types.SignedToken
import zio.*
import zio.test.*

import java.nio.file.{ Files, Path }


object ProjectedTokenProviderSpec extends ZIOSpecDefault:

  private def tempTokenFile(contents: String): ZIO[Scope, Throwable, Path] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val path = Files.createTempFile("sa-token", ".jwt")
        Files.writeString(path, contents)
        path
      }
    )(path => ZIO.attempt(Files.deleteIfExists(path)).ignore)


  def spec = suite("ProjectedTokenProvider")(
    test("reads the token, trimming the trailing newline") {
      ZIO.scoped {
        for
          path  <- tempTokenFile("header.payload.signature\n")
          token <- new ProjectedTokenProvider(path).get
        yield assertTrue(token == SignedToken("header.payload.signature"))
      }
    },
    test("re-reads on each call, so it picks up rotation") {
      ZIO.scoped {
        for
          path    <- tempTokenFile("first-token")
          provider = new ProjectedTokenProvider(path)
          before  <- provider.get
          _       <- ZIO.attempt(Files.writeString(path, "rotated-token"))
          after   <- provider.get
        yield assertTrue(before == SignedToken("first-token"), after == SignedToken("rotated-token"))
      }
    },
    test("missing file → TokenUnavailable") {
      for exit <- new ProjectedTokenProvider(Path.of("/nonexistent/sa/token")).get.either
      yield assertTrue(exit.swap.exists(_.isInstanceOf[ProjectedTokenProvider.TokenUnavailable]))
    },
  )
