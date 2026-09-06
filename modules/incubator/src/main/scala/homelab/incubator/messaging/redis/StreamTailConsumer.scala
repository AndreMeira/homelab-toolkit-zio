package homelab.incubator.messaging.redis


import homelab.common.messaging.Consumer as ConsumerContract
import io.lettuce.core.{ Limit, Range, XReadArgs }
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.StreamMessage
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * A [[ConsumerContract]] that '''tails''' a fixed set of Redis streams: plain `XREAD`, no consumer group.
 *
 * The sibling of [[StreamConsumer]], and the opposite trade. A group hands each entry to one consumer and
 * tracks what has been acknowledged; this hands '''every''' entry to '''every''' reader and tracks nothing
 * server-side. That makes it the wrong tool for work — two readers would do the same job twice — and the
 * right one for '''notification''', where each instance must hear what happened and decide for itself.
 *
 * '''Offsets live here, in memory.''' Each stream is read from the last id this consumer saw, advanced
 * '''after''' the logic has run — so a reader that reconnects resumes rather than skipping, and a failure
 * re-delivers rather than dropping. Delivery is therefore at-least-once, which for a notification is the
 * safe direction: a duplicate costs a wasted look, a lost one costs latency.
 *
 * '''The set of streams is fixed at construction, and that is the point.''' An `XREAD` names the streams it
 * was issued with, so a stream added while a read is blocked cannot be heard until that read returns —
 * which puts `block` on the latency path of every newly added stream, and makes a short block the price of
 * hearing new things promptly. With the set fixed there is nothing to add: every stream is in every read
 * from the first one, delivery is push, and `block` stops being a latency bound at all. What is left for it
 * to bound is a half-open connection going unnoticed, a stopping fiber waiting out a read it cannot cancel,
 * and — in cluster mode — how long a stale topology survives. None of those is a consumer waiting, so it
 * can be seconds rather than milliseconds.
 *
 * Callers that need a changing set make a consumer per set and replace it, which keeps the cost visible
 * instead of hiding it inside a read.
 *
 * '''What can still be missed.''' A stream trims, and `XREAD` does not report having stepped over entries
 * that were trimmed while this consumer was away — it simply hands over what is left. The exposure is a
 * duration, not a count: entries survive `MAXLEN / append rate`, so trimming by age (`MINID`) states what
 * is actually meant. [[positions]] against `XINFO STREAM`'s first entry is how a caller detects it.
 *
 * '''It holds its connection.''' `XREAD … BLOCK` parks it for the whole wait. Give this its own, with a
 * command timeout above `block`, or Lettuce abandons a read that is doing what it was told to.
 *
 * '''One fiber at a time.''' Two concurrent `consume` calls would read overlapping ranges and race to
 * advance the same offsets — harmless, since duplicates are expected, but pure waste. Drive it from one
 * fiber, or wrap it with `serial`.
 *
 * @param redis the connection to read on — not shared, see above
 * @param offsets stream key → the last id delivered from it; the key set never changes
 * @param block how long one `XREAD` waits before returning nothing
 * @param count the most entries one read may return
 */
final class StreamTailConsumer(
  redis: RedisClusterCommands[String, Array[Byte]],
  offsets: Ref[Map[String, String]],
  block: Duration,
  count: Long,
) extends ConsumerContract.Batched[StreamError, StreamMessage[String, Array[Byte]]]:

  /**
   * Read what has arrived on the streams and hand it to `logic`.
   *
   * A read that finds nothing is a noop — the caller's loop simply calls again. Offsets advance only once
   * `logic` has returned, so a failure re-reads the same entries.
   *
   * @param logic processes one batch, in the order Redis returned it
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is processed; aborts with `E2` when `logic` fails, or with `StreamError`
   *         when Redis does
   */
  def consume[E2 >: StreamError](logic: List[StreamMessage[String, Array[Byte]]] => IO[E2, Unit]): IO[E2, Unit] =
    for
      offs <- offsets.get
      _    <- read(offs).flatMap:
                case Nil     => ZIO.unit
                case entries => logic(entries) *> advance(entries)
    yield ()

  /**
   * Where each stream has been read up to.
   *
   * For tests and metrics, and for the caller that wants to know whether it has been trimmed past: compare
   * these against `XINFO STREAM`'s first entry, and an oldest-held id newer than the id here means entries
   * were removed unread.
   *
   * @return stream key → the last id delivered from it
   */
  def positions: UIO[Map[String, String]] = offsets.get

  /**
   * One `XREAD` across every stream.
   *
   * @param from what to read, and from where
   * @return what arrived, oldest first; aborts with `Unavailable` when Redis does
   */
  private def read(from: Map[String, String]): IO[StreamError, List[StreamMessage[String, Array[Byte]]]] =
    val streams = from.map((stream, id) => StreamOffset.from(stream, id)).toArray
    ZIO
      .attemptBlocking(redis.xread(XReadArgs.Builder.block(block.toMillis).count(count), streams*))
      .mapBoth(
        error => StreamError.Unavailable(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)),
        reply => Option(reply).map(_.asScala.toList).getOrElse(Nil),
      )

  /**
   * Move each stream's offset to the last id delivered from it.
   *
   * Entries come back in id order per stream, so the last one seen for a stream is the furthest — no
   * comparison needed. Streams the batch said nothing about keep the id they had.
   *
   * @param entries what was just handed to the logic
   * @return noop
   */
  private def advance(entries: List[StreamMessage[String, Array[Byte]]]): UIO[Unit] =
    val latest = entries.foldLeft(Map.empty[String, String])((seen, entry) => seen.updated(entry.getStream, entry.getId))
    offsets.update(current => current.map((stream, id) => stream -> latest.getOrElse(stream, id)))


object StreamTailConsumer:

  /**
   * A tailing consumer over `streams`, each positioned at its end.
   *
   * '''"From now" is resolved here, to a concrete id''' — each stream's current last entry, or `0-0` when
   * it has none. Storing the literal `$` instead would be a bug: `$` is evaluated by each `XREAD` at the
   * moment that read runs, so a read that timed out empty would leave the offset at `$`, and anything
   * appended between two reads would be stepped over and never delivered. Resolving once means every read
   * asks for "after the last thing I actually saw".
   *
   * Streams are tailed from now rather than from the beginning because an entry that arrived before anyone
   * was listening has nothing to wake — whatever it announced is still there to be found by whoever asks.
   *
   * `NonEmptyChunk` rather than `Set`, because `XREAD` needs at least one stream and a consumer over none
   * could only ever sleep: the emptiness is worth refusing at the type level rather than branching on at
   * every read.
   *
   * @param redis the connection to read on; give it one of its own
   * @param streams what to tail, fixed for this consumer's life
   * @param block how long one read waits — a liveness and shutdown bound, not a latency one, so seconds
   *              rather than milliseconds
   * @param count the most entries one read may return. A cap on one reply, not a batch to fill: a read
   *              returns as soon as one entry exists, so a high count costs nothing in latency and is what
   *              lets a reader that fell behind catch up in one round trip instead of twenty
   * @return the consumer; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(
    redis: RedisClusterCommands[String, Array[Byte]],
    streams: NonEmptyChunk[String],
    block: Duration = 5.seconds,
    count: Long = 1000,
  ): IO[StreamError, StreamTailConsumer] =
    ZIO
      .foreach(streams.toChunk)(stream => position(redis, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))
      .map(StreamTailConsumer(redis, _, block, count))

  /**
   * Where a stream is right now: the id of its last entry, or `0-0` when it is empty or absent.
   *
   * @param redis the connection to ask on
   * @param stream the stream key
   * @return the id to read after; aborts with `Unavailable` when Redis does
   */
  private def position(redis: RedisClusterCommands[String, Array[Byte]], stream: String): IO[StreamError, String] =
    ZIO
      .attemptBlocking(redis.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
      .mapBoth(
        error => StreamError.Unavailable(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)),
        reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0"),
      )
