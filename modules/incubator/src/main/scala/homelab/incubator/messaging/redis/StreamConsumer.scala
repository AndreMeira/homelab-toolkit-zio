package homelab.incubator.messaging.redis


import homelab.common.messaging.Consumer as ConsumerContract
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.models.stream.ClaimedMessages
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Consumer as Group, StreamMessage, XAutoClaimArgs, XClaimArgs, XGroupCreateArgs, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * A [[ConsumerContract]] over a Redis stream, read through a '''consumer group'''.
 *
 * '''Why a group.''' Redis tracks, per group, which entries have been delivered and not yet acknowledged —
 * the pending entries list. That is the commit boundary this port asks for, provided by the substrate
 * rather than built here: `XREADGROUP` delivers and records, `XACK` commits, and an entry whose consumer
 * died is still in the list, reclaimable by a peer. Redelivery is therefore real, so '''logic must be
 * idempotent'''.
 *
 * '''What it is not.''' A group hands each entry to one consumer, with no ordering between them. There is
 * no per-key serialisation here: two entries for the same customer can be worked at the same time by two
 * consumers. If that matters, this is the wrong tool — see `distributed-keyed-queue`, which exists because
 * a stream cannot answer it.
 *
 * '''It holds its connection.''' `XREADGROUP … BLOCK` parks the connection for the whole wait, exactly as
 * a blocking list pop would. Give this its own connection, and make sure that connection's command timeout
 * exceeds `block`, or Lettuce abandons a read that is doing what it was told to.
 *
 * @param redis the connection to read on — not shared, see above
 * @param stream the stream key
 * @param group the consumer group to read through
 * @param name this consumer's name within the group; stable per process, so its pending entries can be
 *             recognised and reclaimed after a crash
 * @param block how long one `XREADGROUP` waits before returning nothing
 * @param keepalive how often to re-claim the in-flight entry to reset its idle time, or `None` to let a
 *                  long handler risk being reclaimed by a peer
 * @param onFailure what to do with an entry whose logic failed
 */
final class StreamConsumer(
  redis: RedisClusterCommands[String, Array[Byte]],
  stream: String,
  group: String,
  name: String,
  block: Duration,
  keepalive: Option[Duration],
  onFailure: StreamConsumer.OnFailure,
) extends ConsumerContract[StreamError, StreamMessage[String, Array[Byte]]]:

  private val consumer: Group[String] = Group.from(group, name)

  /**
   * Take the next entry, run `logic` on it, and settle by the outcome.
   *
   * A read that finds nothing is a noop, not a failure: the caller's loop simply calls again. That keeps
   * `block` a tuning knob rather than part of the contract.
   *
   * @param logic processes one entry
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the entry is settled; aborts with `E2` when `logic` fails under `Surface`, or with
   *         `StreamError` when Redis does
   */
  def consume[E2 >: StreamError](logic: StreamMessage[String, Array[Byte]] => IO[E2, Unit]): IO[E2, Unit] =
    next.flatMap:
      case None          => ZIO.unit
      case Some(message) => settle(message, logic)

  /**
   * One `XREADGROUP` for a single entry.
   *
   * `StreamOffset.lastConsumed` is the `>` id: entries never delivered to this group. Entries this
   * consumer has already been given and not acknowledged are *not* returned again here — they are in the
   * pending list, and come back through [[StreamConsumer.reclaim]].
   *
   * @return the entry, or `None` when the read expired; aborts with `Unavailable` when Redis does
   */
  private def next: IO[StreamError, Option[StreamMessage[String, Array[Byte]]]] =
    ZIO
      .attemptBlocking:
        redis.xreadgroup(
          consumer,
          XReadArgs.Builder.block(block.toMillis).count(1),
          StreamOffset.lastConsumed(stream),
        )
      .mapBoth(failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption)

  /**
   * Run the logic under the keepalive, then acknowledge or apply the failure policy.
   *
   * @param message the entry being processed
   * @param logic what to run
   * @tparam E2 the widened error
   * @return noop once settled; aborts as [[consume]] describes
   */
  private def settle[E2 >: StreamError](
    message: StreamMessage[String, Array[Byte]],
    logic: StreamMessage[String, Array[Byte]] => IO[E2, Unit],
  ): IO[E2, Unit] = alive(message.getId)(logic(message)).foldZIO(
    failed =>
      onFailure match
        case StreamConsumer.OnFailure.Redeliver => ZIO.unit // leave it pending; a peer reclaims it
        case StreamConsumer.OnFailure.Discard   => ack(message) *> drop(message)
        case StreamConsumer.OnFailure.Surface   => ack(message) *> ZIO.fail(failed),
    _ => ack(message),
  )

  /**
   * Keep an in-flight entry from looking abandoned.
   *
   * Idle time is measured from the last delivery, so a handler slower than the reclaim threshold would be
   * taken away mid-flight and worked twice. Re-claiming the entry for ourselves resets that clock — the
   * stream equivalent of a lease heartbeat.
   *
   * @param id the entry being held
   * @param effect the work to run while holding it
   * @tparam E2 the widened error
   * @tparam A what the work produces
   * @return the work's result, with the keepalive stopped either way
   */
  private def alive[E2 >: StreamError, A](id: String)(effect: IO[E2, A]): IO[E2, A] =
    keepalive match
      case None       => effect
      case Some(tick) =>
        val touch = ZIO
          .attemptBlocking:
            val args = XClaimArgs.Builder.minIdleTime(0L).justid()
            redis.xclaim(stream, consumer, args, id)
          .ignore
        effect.race(touch.delay(tick).forever)

  /**
   * Commit the entry: it leaves the group's pending list and will not be redelivered.
   *
   * @param message the entry to acknowledge
   * @return noop; aborts with `Unavailable` when Redis does
   */
  private def ack(message: StreamMessage[String, Array[Byte]]): IO[StreamError, Unit] =
    ZIO.attemptBlocking(redis.xack(stream, group, message.getId)).mapError(failure).unit

  /**
   * Remove the entry's payload from the stream, for a failure nobody should see again.
   *
   * @param message the entry to delete
   * @return noop; failures are ignored, because the entry is acknowledged either way
   */
  private def drop(message: StreamMessage[String, Array[Byte]]): UIO[Unit] =
    ZIO.attemptBlocking(redis.xdel(stream, message.getId)).ignore.unit

  /**
   * Everything Lettuce throws is a substrate problem from here.
   *
   * @param error what was thrown
   * @return the domain error
   */
  private def failure(error: Throwable): StreamError =
    StreamError.Unavailable(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))


object StreamConsumer:

  /** What to do with an entry whose logic failed. */
  enum OnFailure:

    /** Leave it pending, so a peer reclaims and retries it. The default for work that should not be lost. */
    case Redeliver

    /** Acknowledge and delete it — a payload no retry will fix. */
    case Discard

    /** Acknowledge it and fail the `consume` call, leaving the decision to the caller. */
    case Surface

  /**
   * A consumer, with its group created if it does not exist.
   *
   * `MKSTREAM` so a group can be created before anything has been published, and `BUSYGROUP` is swallowed
   * because "already there" is the normal case on every start after the first.
   *
   * @param redis the connection to read on; give it one of its own
   * @param stream the stream key
   * @param group the consumer group
   * @param name this consumer's name within the group
   * @param block how long one read waits
   * @param keepalive how often to reset the in-flight entry's idle time
   * @param onFailure what to do when logic fails
   * @return the consumer; aborts with `Unavailable` when the group cannot be created
   */
  def make(
    redis: RedisClusterCommands[String, Array[Byte]],
    stream: String,
    group: String,
    name: String,
    block: Duration = 5.seconds,
    keepalive: Option[Duration] = Some(10.seconds),
    onFailure: OnFailure = OnFailure.Redeliver,
  ): IO[StreamError, StreamConsumer] =
    ZIO
      .attemptBlocking:
        redis.xgroupCreate(StreamOffset.from(stream, "0"), group, XGroupCreateArgs.Builder.mkstream())
      .unit
      .catchAll:
        case busy if Option(busy.getMessage).exists(_.contains("BUSYGROUP")) => ZIO.unit
        case other                                                           => ZIO.fail(StreamError.Unavailable(Option(other.getMessage).getOrElse("cannot create the group")))
      .as(StreamConsumer(redis, stream, group, name, block, keepalive, onFailure))

  /**
   * Hand entries whose consumer went silent to `name`, so a crash does not strand them.
   *
   * One pass; a caller schedules it. `XAUTOCLAIM` walks the group's pending list from `cursor`, takes
   * everything idle longer than `after`, and returns the cursor to resume from — `"0-0"` when it has been
   * round once.
   *
   * @param redis a connection to run on; this one does not block, so the shared connection is fine
   * @param stream the stream key
   * @param group the consumer group
   * @param name who to reclaim for
   * @param after how idle an entry must be before it is considered abandoned
   * @param cursor where to resume the walk
   * @return the cursor for the next pass; aborts with `Unavailable` when Redis does
   */
  def reclaim(
    redis: RedisClusterCommands[String, Array[Byte]],
    stream: String,
    group: String,
    name: String,
    after: Duration,
    cursor: String = "0-0",
  ): IO[StreamError, String] =
    ZIO
      .attemptBlocking:
        val args: XAutoClaimArgs[String] =
          XAutoClaimArgs.Builder.xautoclaim(Group.from(group, name), after.asJava, cursor)
        redis.xautoclaim(stream, args)
      .mapBoth(
        error => StreamError.Unavailable(Option(error.getMessage).getOrElse("cannot reclaim")),
        (claimed: ClaimedMessages[String, Array[Byte]]) => claimed.getId,
      )
