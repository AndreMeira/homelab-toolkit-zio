package homelab.incubator.messaging.mailbox.nats


import homelab.incubator.messaging.mailbox.Serde
import homelab.incubator.messaging.nats.v5.NatsSpecLayers
import io.nats.client.{ Connection, Message }
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets
import java.time.Duration as JavaDuration


/**
 * Prototype integration tests for [[SharedInboxNatsMailbox]] — the single-wildcard-subscription variant. Same
 * scenarios as [[NatsMailboxSpec]] (ask round-trip + full request-reply + timeout), so the two adapters can be
 * compared directly; here `expect` opens no per-request subscription — the reply loop set up at `make`
 * correlates by the subject's trailing UUID.
 */
object SharedInboxNatsMailboxSpec extends ZIOSpecDefault:

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  def spec = suite("SharedInboxNatsMailbox (integration)")(
    test("ask round-trip: expect (no network), deliver to the reply subject, await") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- SharedInboxNatsMailbox.make(connection)
          receipt    <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          _          <- mailbox.deliver(receipt.address, "pong")(using Serde.utf8)
          reply      <- receipt.await
        yield assertTrue(reply == Some("pong"))
    },
    test("request-reply: the reply address rides in the request; a responder delivers back") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- SharedInboxNatsMailbox.make(connection)
          responder  <- ZIO.acquireRelease(
                          ZIO.attemptBlocking(connection.createDispatcher((_: Message) => ()))
                        )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
          _          <- ZIO.attemptBlocking:
                          responder.subscribe(
                            "greet",
                            (request: Message) => connection.publish(new String(request.getData, StandardCharsets.UTF_8), utf8("hello back")),
                          )
                          connection.flush(JavaDuration.ofSeconds(5))
          receipt    <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          _          <- ZIO.attemptBlocking(connection.publish("greet", utf8(receipt.address)))
          reply      <- receipt.await
        yield assertTrue(reply == Some("hello back"))
    },
    test("concurrent expectations are correlated to the right reply by subject") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- SharedInboxNatsMailbox.make(connection)
          a          <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          b          <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          _          <- mailbox.deliver(b.address, "to-b")(using Serde.utf8)
          _          <- mailbox.deliver(a.address, "to-a")(using Serde.utf8)
          ra         <- a.await
          rb         <- b.await
        yield assertTrue(ra == Some("to-a"), rb == Some("to-b"))
    },
    test("await yields None when no reply arrives before the timeout") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- SharedInboxNatsMailbox.make(connection)
          receipt    <- mailbox.expect[String](300.millis)(using Serde.utf8)
          reply      <- receipt.await
        yield assertTrue(reply == None)
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds) @@ TestAspect.sequential
