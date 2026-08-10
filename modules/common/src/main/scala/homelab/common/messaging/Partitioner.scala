package homelab.common.messaging

import zio.{UIO, ZIO}


/**
 * Derives a value's partition key — the routing identity that co-locates related messages.
 *
 * @tparam K the partition key
 * @tparam V the value partitioned
 */
trait Partitioner[K, V] {

  /**
   * The partition key of `value`.
   *
   * @param value the value to key
   * @return its partition key
   */
  def partition(value: V): UIO[K]
}

object Partitioner {

  /**
   * A partitioner from an effectful key function.
   *
   * @param fn computes a value's key
   * @return the partitioner
   */
  def fromFunction[K, V](fn: V => UIO[K]): Partitioner[K, V] = fn(_)

  /**
   * A partitioner from a pure key function.
   *
   * @param fn computes a value's key
   * @return the partitioner
   */
  def pure[K, V](fn: V => K): Partitioner[K, V] = value => ZIO.succeed(fn(value))
  
  /**
   * A type class computing a value's key, with the key type carried as an abstract member so a caller need
   * not name it.
   *
   * @tparam A the value a key is computed from
   */
  trait Key[A]:

    /** The computed key type. */
    type Type

    /**
     * The key of `value`.
     *
     * @param value the value to key
     * @return its key
     */
    def get(value: A): Type
}
