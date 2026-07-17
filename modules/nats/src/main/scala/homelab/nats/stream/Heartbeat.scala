package homelab.nats.stream


import io.nats.client.Message
import zio.*


/**
 * JetStream ack-deadline keepalive. While a handler runs, [[wrap]] pings `inProgress()` on the in-flight
 * message(s) every `interval`, resetting their `ackWait` timers so slow-but-live processing isn't
 * redelivered mid-flight; the ping fiber is interrupted the moment the handler completes. Enabling it lets
 * `ackWait` stay short (fast dead-consumer detection) while legitimately-slow work extends its own deadline —
 * at the cost of one ping per message per interval.
 */
private[nats] object Heartbeat:

  /**
   * Run `effect`, pinging `inProgress()` on `messages` every `interval` for as long as it runs.
   *
   * @param interval how often to ping, or `None` to disable (run `effect` unwrapped)
   * @param messages the in-flight messages to keep alive
   * @param effect   the handler to run
   * @tparam E2 the effect's error
   * @tparam R  the effect's result
   * @return the effect's result; ping failures are ignored (redelivery covers a missed keepalive)
   */
  def wrap[E2, R](interval: Option[Duration], messages: List[Message])(effect: IO[E2, R]): IO[E2, R] =
    interval.fold(effect): interval =>
      ZIO.scoped:
        // first ping after `every`, not at t=0 — the freshly-delivered message's ackWait timer is new
        ping(messages).delay(interval).forever.forkScoped *> effect

  /**
   * Best-effort `inProgress()` on every message.
   *
   * @param messages the messages to signal progress on
   * @return unit; individual ping failures are ignored
   */
  private def ping(messages: List[Message]): UIO[Unit] =
    ZIO.foreachDiscard(messages)(message => ZIO.attemptBlocking(message.inProgress()).ignore)
