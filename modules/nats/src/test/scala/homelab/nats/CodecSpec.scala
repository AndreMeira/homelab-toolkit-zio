package homelab.nats


import zio.schema.{ Schema, derived }
import zio.test.*

import java.nio.charset.StandardCharsets


/**
 * The codecs built from a zio-schema `Schema`: protobuf by default, JSON on request, and the reason a message
 * does not decode. No broker is involved: a message built by an encoder is handed straight to a decoder.
 */
object CodecSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("Codec built from a Schema")(
    test("a value written as protobuf reads back the same, on the subject it was given") {
      val message = Codec.Encoder.fromSchema[Order](subject).encode(order)
      assertTrue(message.getSubject == "orders.o-1", Codec.Decoder.fromSchema[Order].decode(message) == Right(order))
    },
    test("a value written as JSON reads back the same, and is readable on the wire") {
      val message = Codec.Encoder.json.fromSchema[Order](subject).encode(order)
      val text    = String(message.getData, StandardCharsets.UTF_8)
      assertTrue(text.contains("\"id\":\"o-1\""), Codec.Decoder.json.fromSchema[Order].decode(message) == Right(order))
    },
    test("the formats are not interchangeable: protobuf bytes do not read as JSON") {
      val message = Codec.Encoder.fromSchema[Order](subject).encode(order)
      assertTrue(Codec.Decoder.json.fromSchema[Order].decode(message).isLeft)
    },
    test("bytes that are no message of the schema decode to the codec's reason, not a value") {
      val message = Codec.Encoder.utf8(_ => "orders.garbage").encode("not an order")
      assertTrue(Codec.Decoder.fromSchema[Order].decode(message).left.exists(_.nonEmpty))
    },
  )

  private object Support:

    /**
     * What these tests send.
     *
     * @param id the order's id, which also names its subject
     * @param lines how many lines it has
     */
    final case class Order(id: String, lines: Int) derives Schema

    val order: Order = Order("o-1", 3)

    /**
     * Where an order goes: its own subject, as a keyed producer would send it.
     *
     * @param value the order
     * @return its subject
     */
    def subject(value: Order): String = s"orders.${value.id}"
