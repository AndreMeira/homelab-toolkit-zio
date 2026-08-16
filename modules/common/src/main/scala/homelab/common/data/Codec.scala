package homelab.common.data

import homelab.common.error.ApplicationError


/**
 * The library's default wire codecs: how a value becomes bytes, and how bytes become a value again.
 *
 * The two halves are separate typeclasses rather than one, because call sites are one-directional — a
 * producer needs only to write, an expectation needs only to read — and asking for both would make every
 * such site demand a capability it does not use. They live under one umbrella because they are one
 * decision: an adapter picks a wire format once and supplies the pair.
 */
object Codec {

  /**
   * Putting a value on a wire: an `A` to bytes.
   *
   * Encoding is total, and deliberately so. A value that exists in this process can always be written; it is
   * only the reading side that faces bytes it did not produce — from an older peer, a newer peer, or no peer
   * at all — which is why [[Decoder]] can fail and this cannot. Adapters that need a fallible encoding should
   * reject the value before it becomes an `A`, not here.
   *
   * @tparam A the value encoded
   */
  trait Encoder[A] {

    /**
     * Write a value out.
     *
     * @param value the value to encode
     * @return its wire representation
     */
    def encode(value: A): Array[Byte]
  }

  object Encoder {

    /**
     * Summon the encoder in scope for `A`.
     *
     * @tparam A the value encoded
     * @return the given [[Encoder]] for `A`
     */
    def apply[A: Encoder]: Encoder[A] = summon
  }

  /**
   * Reading a value off a wire: bytes to an `A`, or the reason they were not one.
   *
   * The result is an `Either` rather than an effect: decoding is a pure function of the bytes it is given, so
   * a caller inside an effect can lift it where it needs to and a caller outside one is not forced into a
   * runtime. Failures are [[ApplicationError.DecodingError]]s, so they compose with the error channel every
   * adapter already carries. See [[Encoder]] for why the writing side has no such case.
   *
   * @tparam A the value decoded
   */
  trait Decoder[A] {

    /**
     * Read a value back.
     *
     * @param value the bytes to decode
     * @return the decoded value, or the reason these bytes are not an `A`
     */
    def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, A]
  }

  object Decoder {

    /**
     * Summon the decoder in scope for `A`.
     *
     * @tparam A the value decoded
     * @return the given [[Decoder]] for `A`
     */
    def apply[A: Decoder]: Decoder[A] = summon
  }
}
