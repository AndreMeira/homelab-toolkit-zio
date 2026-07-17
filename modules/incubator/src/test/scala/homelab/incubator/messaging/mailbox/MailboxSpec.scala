package homelab.incubator.messaging.mailbox


import zio.*
import zio.test.*

import java.util.UUID


/**
 * Prototype tests for the [[Mailbox]] port against the [[InMemoryMailbox]] backend — the ask (request-reply)
 * round-trip, that concurrent receipts don't cross, that a delivery to an unknown address is a harmless no-op,
 * and that `await` yields `None` when the reply doesn't arrive before the timeout. `Serde.utf8` is passed
 * explicitly (not an ambient given), as the toolkit convention prescribes.
 */
object MailboxSpec extends ZIOSpecDefault:

  def spec = suite("InMemoryMailbox")(
    test("ask: expect a reply, hand the address to a handler, await it") {
      for
        mailbox <- InMemoryMailbox.make
        receipt <- mailbox.expect[String](5.seconds)(using Serde.utf8)
        // a "remote handler" that holds only the address delivers the reply
        _       <- mailbox.deliver(receipt.address, "pong")(using Serde.utf8).fork
        reply   <- receipt.await
      yield assertTrue(reply == Some("pong"))
    },
    test("concurrent receipts don't cross-deliver") {
      for
        mailbox <- InMemoryMailbox.make
        a       <- mailbox.expect[String](5.seconds)(using Serde.utf8)
        b       <- mailbox.expect[String](5.seconds)(using Serde.utf8)
        _       <- mailbox.deliver(b.address, "to-b")(using Serde.utf8)
        _       <- mailbox.deliver(a.address, "to-a")(using Serde.utf8)
        ra      <- a.await
        rb      <- b.await
      yield assertTrue(ra == Some("to-a"), rb == Some("to-b"))
    },
    test("deliver to an unknown address is a no-op; the pending receipt still resolves") {
      for
        mailbox <- InMemoryMailbox.make
        receipt <- mailbox.expect[String](5.seconds)(using Serde.utf8)
        _       <- mailbox.deliver(Address(UUID.randomUUID().toString), "nobody-home")(using Serde.utf8) // dropped
        _       <- mailbox.deliver(receipt.address, "pong")(using Serde.utf8)                            // the real reply lands
        reply   <- receipt.await
      yield assertTrue(reply == Some("pong"))
    },
    test("await yields None when no reply arrives before the timeout") {
      for
        mailbox <- InMemoryMailbox.make
        receipt <- mailbox.expect[String](200.millis)(using Serde.utf8)
        reply   <- receipt.await // nothing delivered
      yield assertTrue(reply == None)
    },
    test("an abandoned expectation is swept by a later expect (no fiber)") {
      for
        pending <- Ref.make(Map.empty[UUID, Pending[MailboxError]])
        counter <- Ref.make(0)
        mailbox  = new InMemoryMailbox(pending, counter, sweepEvery = 1) // sweep on every expect, for the test
        _       <- mailbox.expect[String](50.millis)(using Serde.utf8)   // expected, never awaited
        _       <- mailbox.expect[String](50.millis)(using Serde.utf8)   // expected, never awaited
        before  <- pending.get.map(_.size)                               // both still registered
        _       <- ZIO.sleep(80.millis)                                  // both past their deadline
        _       <- mailbox.expect[String](5.seconds)(using Serde.utf8)   // sweeps the two, registers this one
        after   <- pending.get.map(_.size)
      yield assertTrue(before == 2, after == 1)
    },
  ) @@ TestAspect.withLiveClock
