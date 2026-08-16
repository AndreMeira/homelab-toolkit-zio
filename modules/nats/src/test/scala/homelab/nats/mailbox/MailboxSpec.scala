package homelab.nats.mailbox


import homelab.common.data.Codec.{ Decoder, Encoder }
import homelab.common.error.ApplicationError
import homelab.common.processing.Mailbox as MailboxContract
import homelab.nats.NatsSpecLayers
import io.nats.client.Connection
import zio.*
import zio.test.*


/**
 * Integration check for the NATS mailbox sketch against a real broker: an expectation minted here is
 * resolved by an ordinary publish to its droppoff subject, and anything arriving under the root that no
 * expectation claims reaches `forward`. Also settles the question the design rests on — that a subject root
 * like `__MAILBOX__` is legal, subscribable, and publishable.
 *
 * Core NATS subscribes lazily and drops messages with no live subscriber, so both tests republish on a
 * schedule until the thing they are waiting for arrives, rather than assuming the first publish lands.
 * Requires a running Docker daemon.
 */
object MailboxSpec extends ZIOSpecDefault:

  private given Encoder[String] with
    override def encode(value: String): Array[Byte] = value.getBytes

  private given Decoder[String] with
    override def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, String] =
      Right(new String(value))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("NATS Mailbox (integration)")(
    test("resolves an expectation published to its droppoff subject") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          mailbox    <- Mailbox.make(connection, s"${Mailbox.defaultPrefix}.roundtrip")
          _          <- mailbox.incoming.run.forkScoped
          receipt    <- mailbox.incoming.expect[String](30.seconds)
          _          <- mailbox.outgoing.send(receipt.address, "pong").repeat(Schedule.spaced(100.millis)).forkScoped
          out        <- receipt.await
        yield assertTrue(out.contains("pong"))
    },
    test("hands a message sent to the published address to forward") {
      // The request seam: nothing expects this address, so it leaves through `forward` rather than vanishing.
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          unclaimed  <- Queue.unbounded[MailboxContract.Message]
          mailbox    <- Mailbox.make(
                          connection,
                          s"${Mailbox.defaultPrefix}.requests",
                          Some(message => unclaimed.offer(message).unit),
                        )
          _          <- mailbox.incoming.run.forkScoped
          address    <- mailbox.location.get
          _          <- mailbox.outgoing.send(address, "hello").repeat(Schedule.spaced(100.millis)).forkScoped
          message    <- unclaimed.take
        yield assertTrue(message.address == address, new String(message.payload) == "hello")
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
