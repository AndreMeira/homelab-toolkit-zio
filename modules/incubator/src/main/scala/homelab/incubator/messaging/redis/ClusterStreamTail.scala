package homelab.incubator.messaging.redis


import homelab.common.messaging.Consumer as ConsumerContract
import io.lettuce.core.StreamMessage
import io.lettuce.core.cluster.SlotHash
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import zio.*


/**
 * A [[StreamTailConsumer]] that works in Redis Cluster, by being several of them.
 *
 * '''Why one is not enough.''' A multi-key command must have all its keys in one slot, and `XREAD` is
 * multi-key. Streams whose hash tags differ live in different slots, so a single `XREAD` cannot name them
 * — the node answers `CROSSSLOT`. And because a blocking read occupies its connection for the whole wait,
 * the groups cannot take turns on one connection either: a five second block on slot A would be five
 * seconds of deafness on slot B.
 *
 * So there is one tailing consumer, one connection and one fiber '''per slot''', each reading the streams
 * that live there. They all feed one queue, and `consume` takes from it — so a caller sees a single
 * consumer regardless of how the streams are spread.
 *
 * '''The grouping is decided once.''' Streams are fixed for this consumer's life, so which slots exist and
 * which streams belong to each is settled at construction: no reader is opened later, and no read is ever
 * re-issued because the set changed. That is what keeps `block` off the latency path — see the note on
 * [[StreamTailConsumer]] — and it is why this holds a plain map rather than a synchronised one.
 *
 * '''What that changes about delivery.''' A [[StreamTailConsumer]] advances its offsets after the logic
 * has run. Here the reader's "logic" is handing the batch to the queue, so an offset advances once a batch
 * is '''in this process''', not once the caller has dealt with it. A caller whose logic fails does not get
 * the batch again. That is the right trade for notifications — where a retry would only re-announce state
 * the caller can read anyway — and the wrong one for work.
 *
 * The queue is bounded and back-pressures: a caller that stops consuming eventually stops the readers,
 * rather than growing the heap.
 *
 * @param groups slot → the tailing consumer reading that slot's streams
 * @param inbox where every reader hands its batches
 */
final class ClusterStreamTail(
  groups: Map[Int, StreamTailConsumer],
  inbox: Queue[List[StreamMessage[String, Array[Byte]]]],
) extends ConsumerContract.Batched[StreamError, StreamMessage[String, Array[Byte]]]:

  /**
   * Take the next batch any reader has produced and hand it to `logic`.
   *
   * Suspends until one arrives, so a caller loops on it without a sleep of its own.
   *
   * @param logic processes one batch
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is processed; aborts with `E2` when `logic` does
   */
  def consume[E2 >: StreamError](logic: List[StreamMessage[String, Array[Byte]]] => IO[E2, Unit]): IO[E2, Unit] =
    inbox.take.flatMap(logic)

  /**
   * Where each reader has got to, by slot.
   *
   * @return slot → its streams and the last id delivered from each
   */
  def positions: UIO[Map[Int, Map[String, String]]] =
    ZIO.foreach(groups)((slot, reader) => reader.positions.map(slot -> _))


object ClusterStreamTail:

  /**
   * A cluster-wide tailing consumer over `streams`.
   *
   * One reader, connection and fiber per slot the streams occupy, all opened here and torn down with the
   * scope. A slot with no streams is not a case: the grouping is derived from the streams themselves.
   *
   * @param open how to open a connection for a slot — from a `RedisClusterClient`, one connection each, so
   *             that a blocking read on one slot cannot queue behind a blocking read on another
   * @param streams what to tail, fixed for this consumer's life
   * @param block how long one read waits — a liveness and shutdown bound, not a latency one
   * @param count the most entries one read may return
   * @param capacity how many batches may sit unconsumed before the readers are slowed
   * @return the consumer; aborts with `Unavailable` when a connection cannot be opened, or when a stream's
   *         position cannot be read
   */
  def make(
    open: Int => ZIO[Scope, StreamError, RedisClusterCommands[String, Array[Byte]]],
    streams: NonEmptyChunk[String],
    block: Duration = 5.seconds,
    count: Long = 1000,
    capacity: Int = 256,
  ): ZIO[Scope, StreamError, ClusterStreamTail] =
    for
      inbox  <- Queue.bounded[List[StreamMessage[String, Array[Byte]]]](capacity)
      groups <- ZIO.foreach(bySlot(streams)): (slot, keys) =>
                  for
                    redis  <- open(slot)
                    reader <- StreamTailConsumer.make(redis, keys, block, count)
                    _      <- reader.consume(batch => inbox.offer(batch).unit).forever.forkScoped
                  yield slot -> reader
    yield ClusterStreamTail(groups, inbox)

  /**
   * Which slot each stream belongs to — the grouping this consumer is built around.
   *
   * Only the part of a key between the first `{` and the next `}` is hashed, so streams sharing a hash tag
   * share a slot and can be read together. Grouping a non-empty set can only produce non-empty groups, so
   * the `flatMap` discards nothing — it is how that fact is expressed in the type.
   *
   * @param streams the stream keys
   * @return slot → the streams in it
   */
  def bySlot(streams: NonEmptyChunk[String]): Map[Int, NonEmptyChunk[String]] =
    streams.toChunk
      .groupBy(SlotHash.getSlot)
      .flatMap((slot, keys) => NonEmptyChunk.fromChunk(keys).map(slot -> _))
