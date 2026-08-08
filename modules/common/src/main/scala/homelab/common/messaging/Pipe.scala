package homelab.common.messaging

import zio.IO


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
   * A [[Pipe]] whose intake delivers batches: a [[Consumer.Batched]] of `A` on the read side, a single-`A`
   * [[Producer]] on the write side. Values are emitted one or many at a time and consumed a `List[A]` at a
   * time.
   *
   * @tparam E the error emission or consumption aborts with
   * @tparam A the element carried
   */
  trait Batched[+E, A] extends Consumer.Batched[E, A] with Producer[E, A]:

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
