package homelab.incubator.messaging.mailbox

import zio.Promise

/**
 * An outstanding expectation held by a map-based mailbox backend: the promise a reply completes, plus the
 * absolute deadline (epoch millis) after which it's expired. The deadline both bounds `await`
 * (`remaining = deadline - now`, so the timeout is anchored to `expect`-time, not `await`-time) and lets a
 * later `expect` sweep abandoned entries — no per-expectation timer fiber.
 *
 * @param promise  the promise the matching `deliver` completes with the encoded reply
 * @param deadline the absolute expiry (epoch millis) computed at `expect` time
 * @tparam E the error the promise (and thus `await`) aborts with
 */
final private[mailbox] case class Pending[E](promise: Promise[E, Array[Byte]], deadline: Long)
