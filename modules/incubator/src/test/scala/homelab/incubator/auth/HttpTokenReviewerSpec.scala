package homelab.incubator.auth


import com.sun.net.httpserver.HttpServer
import homelab.common.auth.Requester.Service
import homelab.common.types.{ ServiceName, SignedToken }
import homelab.incubator.auth.v1.{ HttpTokenReviewer, JwtServiceAuthenticator, K8sServiceAccountVerifier, TokenReviewer }
import zio.*
import zio.test.*

import java.net.http.HttpClient
import java.net.{ InetSocketAddress, URI }
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicReference


object HttpTokenReviewerSpec extends ZIOSpecDefault:

  private val saName = "system:serviceaccount:homelab:registration"


  /** A stub API server that answers the TokenReview endpoint with a fixed status + body. */
  private def stubApiServer(status: Int, body: String): ZIO[Scope, Throwable, HttpServer] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
        server.createContext(
          "/apis/authentication.k8s.io/v1/tokenreviews",
          exchange => {
            val bytes = body.getBytes(UTF_8)
            exchange.sendResponseHeaders(status, bytes.length.toLong)
            val os    = exchange.getResponseBody
            os.write(bytes)
            os.close()
          },
        )
        server.start()
        server
      }
    )(server => ZIO.succeed(server.stop(0)))


  private def reviewerFor(server: HttpServer): HttpTokenReviewer =
    new HttpTokenReviewer(
      HttpTokenReviewer.Config(URI.create(s"http://localhost:${server.getAddress.getPort}"), authToken = "test-auth-token"),
      HttpClient.newHttpClient(),
    )


  /** Like the stub, but records the incoming Authorization header + request body into `sink`. */
  private def capturingServer(sink: AtomicReference[(String, String)]): ZIO[Scope, Throwable, HttpServer] =
    ZIO.acquireRelease(
      ZIO.attempt {
        val server = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
        server.createContext(
          "/apis/authentication.k8s.io/v1/tokenreviews",
          exchange => {
            val auth = exchange.getRequestHeaders.getFirst("Authorization")
            val body = new String(exchange.getRequestBody.readAllBytes(), UTF_8)
            sink.set((auth, body))
            val resp = s"""{"status":{"authenticated":true,"user":{"username":"$saName"}}}""".getBytes(UTF_8)
            exchange.sendResponseHeaders(200, resp.length.toLong)
            val os   = exchange.getResponseBody
            os.write(resp)
            os.close()
          },
        )
        server.start()
        server
      }
    )(server => ZIO.succeed(server.stop(0)))


  def spec = suite("HttpTokenReviewer")(
    test("reviews B's token in the body, using A's own token as the request credential") {
      val sink = new AtomicReference[(String, String)](("", ""))
      ZIO.scoped {
        for
          server                   <- capturingServer(sink)
          reviewer                  = new HttpTokenReviewer(
                                        HttpTokenReviewer.Config(URI.create(s"http://localhost:${server.getAddress.getPort}"), authToken = "SERVICE-A-OWN-TOKEN"),
                                        HttpClient.newHttpClient(),
                                      )
          _                        <- reviewer.review(SignedToken("SERVICE-B-TOKEN-TO-VERIFY"))
          (authHeader, requestBody) = sink.get
        yield assertTrue(
          authHeader == "Bearer SERVICE-A-OWN-TOKEN",        // A's own token authenticates the request
          requestBody.contains("SERVICE-B-TOKEN-TO-VERIFY"), // B's token is the subject under review
          !requestBody.contains("SERVICE-A-OWN-TOKEN"), // A's token is NOT what gets reviewed
        )
      }
    },
    test("authenticated review → Result(true, username)") {
      ZIO.scoped {
        for
          server <- stubApiServer(200, s"""{"status":{"authenticated":true,"user":{"username":"$saName"}}}""")
          result <- reviewerFor(server).review(SignedToken("sa-token"))
        yield assertTrue(result == TokenReviewer.Result(true, Some(saName)))
      }
    },
    test("rejected review → Result(false, None)") {
      ZIO.scoped {
        for
          server <- stubApiServer(201, """{"status":{"authenticated":false}}""")
          result <- reviewerFor(server).review(SignedToken("sa-token"))
        yield assertTrue(result == TokenReviewer.Result(false, None))
      }
    },
    test("API error status → Unavailable") {
      ZIO.scoped {
        for
          server <- stubApiServer(500, "boom")
          exit   <- reviewerFor(server).review(SignedToken("sa-token")).either
        yield assertTrue(exit.swap.exists(_.isInstanceOf[TokenReviewer.Unavailable]))
      }
    },
    test("end-to-end: verifier + ServiceAuthenticator → Service") {
      ZIO.scoped {
        for
          server  <- stubApiServer(200, s"""{"status":{"authenticated":true,"user":{"username":"$saName"}}}""")
          verifier = new K8sServiceAccountVerifier(reviewerFor(server))
          who     <- JwtServiceAuthenticator(verifier).authenticate(SignedToken("sa-token"))
        yield assertTrue(who == Service(ServiceName(saName)))
      }
    },
  )
