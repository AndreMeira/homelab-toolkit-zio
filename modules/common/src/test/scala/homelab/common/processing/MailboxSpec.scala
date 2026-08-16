package homelab.common.processing


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.{ Pipe, Producer }
import homelab.common.data.Codec.{ Decoder, Encoder }
import homelab.common.processing.Mailbox.*
import zio.*
import zio.test.*

import java.time.Instant


// Correctness spec for the v2 mailbox: an expectation is a serialisable promise, so what matters is that
// exactly one reply resolves it, that it stays resolvable until whoever holds the receipt stops waiting,
// that its lifetime is decided once at `expect` rather than drifting with the await, and that everything
// else the inbox sees is handed on rather than swallowed. Driven with promises, yields, and short live
// durations where time itself is under test; a suite timeout turns a wedged expectation into a failure.
object MailboxSpec extends ZIOSpecDefault:

  final private case class Broken(reason: String) extends ApplicationError.DecodingError:
    override def message: String = reason

  private given Encoder[String] with
    override def encode(value: String): Array[Byte] = value.getBytes

  private given Decoder[String] with
    override def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, String] =
      Right(new String(value))

  private given Decoder[Int] with
    override def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, Int] =
      new String(value).toIntOption.toRight(Broken(s"not a number: ${new String(value)}"))

  /** Everything a test needs: the inbox, the pipe feeding it, and whatever it could not match. */
  final private case class Fixture(
    inbox: Incoming[AdapterError],
    pipe: Pipe[Nothing, Message],
    forwarded: Ref[List[Message]],
  )

  /** Addresses are minted in order, so a test can tell two expectations apart by name. */
  private def location(counter: Ref[Int]): Location[AdapterError] = new Location[AdapterError]:
    override def get: IO[AdapterError, Address]         = ZIO.succeed(Address("inbox"))
    override def droppoff[A]: IO[AdapterError, Address] = counter.updateAndGet(_ + 1).map(n => Address(s"drop-$n"))

  /** A producer that keeps what it is given, in arrival order. */
  private def recorder(seen: Ref[List[Message]]): Producer[Nothing, Message] =
    message => seen.update(_ :+ message)

  private def fixture(sweepInterval: Duration = Incoming.defaultSweepInterval): UIO[Fixture] =
    for
      counter   <- Ref.make(0)
      forwarded <- Ref.make(List.empty[Message])
      queue     <- Queue.unbounded[Message]
      pipe       = Pipe.fromQueue(queue)
      inbox     <- Incoming.make[AdapterError](location(counter), pipe, Some(recorder(forwarded)), sweepInterval)
    yield Fixture(inbox, pipe, forwarded)

  /**
   * Wait until `fiber` has actually parked. `yieldNow` only offers it the chance to run; a test that turns
   * on whether the wait has begun needs to know that it has, or it silently stops discriminating.
   */
  private def parked(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
    (ZIO.yieldNow *> fiber.status).repeatUntil {
      case _: Fiber.Status.Suspended => true
      case _                         => false
    }.unit

  /** The payloads that reached a recorder, decoded as text, in arrival order. */
  private def texts(messages: List[Message]): List[String] = messages.map(message => new String(message.payload))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Mailbox")(
    test("resolves an expectation with the reply sent to its address") {
      for
        f     <- fixture()
        rcpt  <- f.inbox.expect[String](5.seconds)
        _     <- f.inbox.process(Message(rcpt.address, "pong".getBytes))
        got   <- rcpt.await
        table <- f.inbox.pending.get
      yield assertTrue(got.contains("pong"), table.isEmpty)
    },
    test("stays resolvable while the holder is waiting on it") {
      // The regression that matters: retiring the entry when the wait *starts* rather than when it ends
      // leaves nothing for `process` to find, and every reply that arrives mid-wait is lost.
      for
        f       <- fixture()
        rcpt    <- f.inbox.expect[String](5.seconds)
        waiting <- rcpt.await.fork
        _       <- parked(waiting) // the reply must land while the wait is genuinely in progress
        during  <- f.inbox.pending.get
        _       <- f.inbox.process(Message(rcpt.address, "pong".getBytes))
        got     <- waiting.join
      yield assertTrue(during.contains(rcpt.address), got.contains("pong"))
    },
    test("resolves exactly once — a second reply to the same address is forwarded, not delivered") {
      for
        f    <- fixture()
        rcpt <- f.inbox.expect[String](5.seconds)
        _    <- f.inbox.process(Message(rcpt.address, "first".getBytes))
        _    <- f.inbox.process(Message(rcpt.address, "second".getBytes))
        got  <- rcpt.await
        fwd  <- f.forwarded.get
      yield assertTrue(got.contains("first"), texts(fwd) == List("second"))
    },
    test("forwards what no expectation claims, and only that") {
      for
        f    <- fixture()
        rcpt <- f.inbox.expect[String](5.seconds)
        _    <- f.inbox.process(Message(Address("nobody"), "stray".getBytes))
        _    <- f.inbox.process(Message(rcpt.address, "mine".getBytes))
        got  <- rcpt.await
        fwd  <- f.forwarded.get
      yield assertTrue(got.contains("mine"), fwd.map(_.address) == List(Address("nobody")))
    },
    test("gives up at its deadline, leaving nothing behind") {
      for
        f     <- fixture()
        rcpt  <- f.inbox.expect[String](200.millis)
        got   <- rcpt.await
        table <- f.inbox.pending.get
      yield assertTrue(got.isEmpty, table.isEmpty)
    },
    test("dies at the instant set when it was minted, not the instant it was awaited") {
      // Mint a 400ms expectation, dawdle 300ms, then await: roughly 100ms of budget should be left. Were
      // the duration re-applied at await time the wait would run a further 400ms — and the sweeper, which
      // goes by the minted deadline, could reclaim the entry while the holder was still waiting on it.
      for
        f       <- fixture()
        rcpt    <- f.inbox.expect[String](400.millis)
        _       <- ZIO.sleep(300.millis)
        started <- Clock.nanoTime
        got     <- rcpt.await
        ended   <- Clock.nanoTime
      yield assertTrue(got.isEmpty, Duration.fromNanos(ended - started) < 250.millis)
    },
    test("answers a second await from the first outcome instead of waiting again") {
      for
        f       <- fixture()
        rcpt    <- f.inbox.expect[String](300.millis)
        first   <- rcpt.await
        started <- Clock.nanoTime
        second  <- rcpt.await
        ended   <- Clock.nanoTime
      yield assertTrue(first.isEmpty, second.isEmpty, Duration.fromNanos(ended - started) < 100.millis)
    },
    test("hands one outcome to every fiber awaiting the receipt, decoding it once") {
      // Two holders of the same receipt must both see the reply — and the memo means the wait, and the decode
      // it ends with, happen once however many fibers are queued on it.
      val decodes                   = new java.util.concurrent.atomic.AtomicInteger(0)
      val counting: Decoder[String] = value => {
        val _ = decodes.incrementAndGet()
        Right(new String(value))
      }
      for
        f      <- fixture()
        rcpt   <- f.inbox.expect[String](5.seconds)(using counting)
        first  <- rcpt.await.fork
        second <- rcpt.await.fork
        _      <- parked(first) *> parked(second) // one holds the memo, the other queues behind it
        _      <- f.inbox.process(Message(rcpt.address, "pong".getBytes))
        one    <- first.join
        two    <- second.join
        later  <- rcpt.await                      // and a third, long after it settled
        table  <- f.inbox.pending.get
      yield assertTrue(
        one.contains("pong"),
        two.contains("pong"),
        later.contains("pong"),
        decodes.get == 1,
        table.isEmpty,
      )
    },
    test("retires its entry when the holder is interrupted out of the wait") {
      for
        f       <- fixture()
        rcpt    <- f.inbox.expect[String](5.seconds)
        waiting <- rcpt.await.fork
        _       <- parked(waiting)
        _       <- waiting.interrupt
        table   <- f.inbox.pending.get
      yield assertTrue(table.isEmpty)
    },
    test("keeps expectations apart under concurrent replies") {
      // Distinct addresses and no cross-delivery: each holder gets its own payload, whatever the order the
      // replies arrive in.
      val indices = (1 to 20).toList
      for
        f        <- fixture()
        receipts <- ZIO.foreach(indices)(_ => f.inbox.expect[String](10.seconds))
        waiting  <- ZIO.foreachPar(receipts)(_.await).fork
        _        <- ZIO.yieldNow.repeatN(20)
        _        <- ZIO.foreachParDiscard(receipts.zip(indices).reverse) { (rcpt, index) =>
                      f.inbox.process(Message(rcpt.address, s"reply-$index".getBytes))
                    }
        got      <- waiting.join
      yield assertTrue(
        receipts.map(_.address).distinct.size == indices.size,
        got == indices.map(index => Some(s"reply-$index")),
      )
    },
    test("fails the holder when the reply does not decode") {
      for
        f    <- fixture()
        rcpt <- f.inbox.expect[Int](5.seconds)
        _    <- f.inbox.process(Message(rcpt.address, "not-a-number".getBytes))
        exit <- rcpt.await.exit
      yield assertTrue(exit.isFailure)
    },
    test("sweeps abandoned expectations, but no more often than its interval") {
      // Nothing awaits this one, so only the sweeper can reclaim it — the case that motivates having a
      // sweeper at all. It must survive an `expect` inside the interval and be gone after one beyond it.
      for
        f         <- fixture(sweepInterval = 200.millis)
        abandoned <- f.inbox.expect[String](1.milli)
        _         <- ZIO.sleep(50.millis)
        _         <- f.inbox.expect[String](10.seconds) // within the interval: no sweep
        early     <- f.inbox.pending.get
        _         <- ZIO.sleep(250.millis)
        _         <- f.inbox.expect[String](10.seconds) // past the interval: sweeps
        late      <- f.inbox.pending.get
      yield assertTrue(early.contains(abandoned.address), !late.contains(abandoned.address), late.size == 2)
    },
    test("runs as a processor, resolving expectations from its own intake") {
      ZIO.scoped {
        for
          f    <- fixture()
          _    <- f.inbox.run.forkScoped
          // A budget far wider than the work: this test is about the loop being wired to the table, so a
          // slow machine should never turn it into a timeout that reads like a delivery failure.
          rcpt <- f.inbox.expect[String](30.seconds)
          _    <- f.pipe.emit(Message(rcpt.address, "through-the-loop".getBytes))
          got  <- rcpt.await
        yield assertTrue(got.contains("through-the-loop"))
      }
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
