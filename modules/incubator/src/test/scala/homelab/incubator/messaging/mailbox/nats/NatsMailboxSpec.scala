package homelab.incubator.messaging.mailbox.nats


import homelab.incubator.messaging.mailbox.Serde
import homelab.incubator.messaging.nats.v5.NatsSpecLayers
import io.nats.client.{ Connection, Message }
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets
import java.time.Duration as JavaDuration


/**
 * Prototype integration tests for the NATS [[NatsMailbox]] against a real broker (Testcontainers, reusing the
 * v5 connection layer). Shows the ask round-trip through the adapter's own `deliver`, a full request-reply
 * where the reply inbox address travels in the request, and a timeout that reaps the inbox and yields `None`.
 */
object NatsMailboxSpec extends ZIOSpecDefault:

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  def spec = suite("NatsMailbox (integration)")(
    test("ask round-trip: expect an inbox, deliver to it, await the reply") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- NatsMailbox.make(connection)
          receipt    <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          _          <- mailbox.deliver(receipt.address, "pong")(using Serde.utf8)
          reply      <- receipt.await
        yield assertTrue(reply == Some("pong"))
    },
    test("request-reply: the reply address rides in the request; a responder delivers back") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- NatsMailbox.make(connection)
          // a responder on "greet" replies to whatever inbox subject arrives as the payload
          responder  <- ZIO.acquireRelease(
                          ZIO.attemptBlocking(connection.createDispatcher((_: Message) => ()))
                        )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
          _          <- ZIO.attemptBlocking:
                          responder.subscribe(
                            "greet",
                            (request: Message) => connection.publish(new String(request.getData, StandardCharsets.UTF_8), utf8("hello back")),
                          )
                          connection.flush(JavaDuration.ofSeconds(5)) // responder SUB live before the request is sent
          receipt    <- mailbox.expect[String](5.seconds)(using Serde.utf8)
          _          <- ZIO.attemptBlocking(connection.publish("greet", utf8(receipt.address)))
          reply      <- receipt.await
        yield assertTrue(reply == Some("hello back"))
    },
    test("await yields None when no reply arrives before the timeout") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- NatsMailbox.make(connection)
          receipt    <- mailbox.expect[String](300.millis)(using Serde.utf8)
          reply      <- receipt.await // nobody replies to this inbox
        yield assertTrue(reply == None)
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds) @@ TestAspect.sequential
