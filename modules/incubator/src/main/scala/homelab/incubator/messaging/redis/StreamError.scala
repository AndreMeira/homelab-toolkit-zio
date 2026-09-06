package homelab.incubator.messaging.redis

import homelab.common.error.ApplicationError


/**
 * What the Redis stream adapter can fail with.
 *
 * Deliberately small: everything Lettuce throws is a substrate problem from here, and a caller's only
 * sensible reaction is to retry or to stop.
 */
enum StreamError extends ApplicationError:

  /** Redis could not be reached, or refused the command. */
  case Unavailable(reason: String) extends StreamError, ApplicationError.AdapterError, ApplicationError.TransientError

  override def message: String = this match
    case Unavailable(reason) => s"the stream is unavailable: $reason"
