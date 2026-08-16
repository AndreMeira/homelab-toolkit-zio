package homelab.common.messaging


import homelab.common.flow.KeyedQueue
import zio.{ IO, Queue }


/**
 * A two-ended in-memory conduit: both a [[Producer]] you emit into and a [[Consumer]] you read from, over
 * the same value type `A`. Where a bare [[Producer]] and [[Consumer]] are the two independent halves of a
 * channel, a `Pipe` is the whole channel as a single value — handed to an upstream worker as its output
 * and to a downstream worker as its input.
 *
 * `A` is invariant: the same type is both produced (a [[Consumer]]'s covariant `A`) and consumed (a
 * [[Producer]]'s contravariant `A`), so neither variance survives.
 *
 * @tparam E the error emission or consumption aborts with
 * @tparam A the value carried
 */
trait Pipe[+E, A] extends Consumer[E, A] with Producer[E, A]:

  /**
   * Emit a single value — an alias for [[Producer.emit]] that reads naturally on the write side of a pipe.
   *
   * @param message the value to emit
   * @return noop once emitted; aborts with `E` on failure
   */
  def send(message: A): IO[E, Unit] = emit(message)

  /**
   * Emit each value in order — an alias for [[Producer.emitMany]].
   *
   * @param messages the values to emit, in order
   * @return noop once all are emitted; aborts with `E` on the first failure
   */
  def send(messages: List[A]): IO[E, Unit] = emitMany(messages)


object Pipe:

  /**
   * A [[Pipe]] that holds a key while one of its values is in flight: at most one value per key is out at a
   * time, and a key's values are delivered in the order they were emitted.
   *
   * A marker, not a mechanism — it says what a pipe guarantees, and anything consuming it concurrently relies
   * on that guarantee. [[fromKeyedQueue]] provides it; a pipe over an external queue can claim it if the queue
   * leases keys (a per-key claim in a database, say), which is a deliberate assertion rather than something
   * checked here.
   *
   * @tparam E the error consuming may abort with
   * @tparam A the value carried
   */
  trait KeySafe[+E, A] extends Pipe[E, A]

  /**
   * A [[Pipe]] over an unbounded [[Queue]]: `emit` offers, `consume` takes one value. Emission never fails,
   * so the pipe's error is `Nothing`.
   *
   * @param queue the backing queue
   * @tparam A the value carried
   * @return a queue-backed pipe
   */
  def fromQueue[A](queue: Queue[A]): Pipe[Nothing, A] =
    new Pipe[Nothing, A]:
      def emit(value: A): IO[Nothing, Unit]                              = queue.offer(value).unit
      def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] = queue.take.flatMap(logic)

  /**
   * A **key-safe** [[Pipe]] over a [[KeyedQueue]]: `emit` routes a value to `key(value)`, and `consume`
   * claims one value from a key not already in flight, holding that key until the value is done. So draining
   * this pipe concurrently — e.g. from a [[homelab.common.processing.Worker.Parallel]] — never runs two
   * values for the same key at once: the parallelism is *across* keys, serialized *within* one.
   *
   * @param queue the backing keyed queue
   * @param key   the partition key of a value
   * @tparam K the partition key
   * @tparam A the value carried
   * @return a keyed, per-key-serialized pipe
   */
  def fromKeyedQueue[K, A](queue: KeyedQueue[K, A])(key: A => K): KeySafe[Nothing, A] =
    new KeySafe[Nothing, A]:
      def emit(value: A): IO[Nothing, Unit]                              = queue.offer(key(value), value).unit
      def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] = queue.takeWith((_, value) => logic(value))

  /**
   * A [[Pipe]] whose intake delivers batches: a [[Consumer.Batched]] of `A` on the read side, a single-`A`
   * [[Producer]] on the write side. Values are emitted one or many at a time and consumed a `List[A]` at a
   * time.
   *
   * @tparam E the error emission or consumption aborts with
   * @tparam A the element carried
   */
  trait Batched[+E, A] extends Consumer.Batched[E, A] with Producer[E, A]:
    self =>

    /**
     * Emit a single value — an alias for [[Producer.emit]].
     *
     * @param message the value to emit
     * @return noop once emitted; aborts with `E` on failure
     */
    def send(message: A): IO[E, Unit] = emit(message)

    /**
     * Emit each value in order — an alias for [[Producer.emitMany]].
     *
     * @param messages the values to emit, in order
     * @return noop once all are emitted; aborts with `E` on the first failure
     */
    def send(messages: List[A]): IO[E, Unit] = emitMany(messages)
