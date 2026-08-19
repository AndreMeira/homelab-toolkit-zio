package homelab.incubator.llm.v2


import zio.Chunk
import zio.schema.annotation.description
import zio.schema.{ Schema, TypeId }


/**
 * `zio.schema.Schema` instances for types the describable subset cannot express directly.
 *
 * These are *schemas*, not cases in [[JsonSchemaDerivation]], and that is the whole point: a codec derived
 * from the same `Schema` encodes the same shape the schema advertises, so the description handed to a model
 * and the parser reading its reply cannot disagree. Teaching the deriver about `Map` instead would advertise
 * one shape while the codec expected another — the drift this design exists to prevent, self-inflicted.
 */
object Schemas {

  /**
   * One association in a map rendered as a list.
   *
   * @param key the association's key
   * @param value the associated value
   * @tparam K the key type
   * @tparam V the value type
   */
  final case class Entry[K, V](key: K, value: V)

  object Entry:

    /**
     * The schema of one association.
     *
     * @tparam K the key type, itself describable
     * @tparam V the value type, itself describable
     * @return the entry's schema
     */
    given schema[K, V](using key: Schema[K], value: Schema[V]): Schema[Entry[K, V]] =
      Schema.CaseClass2[K, V, Entry[K, V]](
        TypeId.parse("homelab.incubator.llm.v2.Schemas.Entry"),
        Schema.Field("key", key, get0 = _.key, set0 = (entry, k) => entry.copy(key = k)),
        Schema.Field("value", value, get0 = _.value, set0 = (entry, v) => entry.copy(value = v)),
        Entry.apply,
      )

  /**
   * A map described — and therefore encoded — as a list of key/value objects.
   *
   * JSON's own encoding of a map needs `additionalProperties`, which this subset closes and strict-mode
   * providers forbid, so an association list is not a downgrade from an object: it is the only shape
   * available. It also admits non-string keys, which the object form never could.
   *
   * '''Duplicates are refused rather than collapsed.''' `toMap` would keep the last silently, turning a
   * model's mistake into a quietly wrong answer; failing gives the model a reason it can act on, which is how
   * every other decode failure is handled here.
   *
   * '''Deliberately not a `given`.''' Importing one would silently re-encode *every* `Map` in scope, including
   * those the author never considered — and this changes what goes on the wire. Opt in per key/value pair,
   * where the choice is visible:
   *
   * {{{
   * given Schema[Map[String, Int]] = Schemas.mapAsEntries
   * }}}
   *
   * @tparam K the key type, itself describable
   * @tparam V the value type, itself describable
   * @return a schema for `Map[K, V]` over a list of [[Entry]]
   */
  def mapAsEntries[K, V](using Schema[K], Schema[V]): Schema[Map[K, V]] =
    Schema
      .list[Entry[K, V]]
      .annotate(description("an association list; each key must appear at most once"))
      .transformOrFail(
        entries =>
          val duplicates = entries.map(_.key).groupBy(identity).collect { case (key, occurrences) if occurrences.sizeIs > 1 => key }
          if duplicates.isEmpty then Right(entries.map(entry => entry.key -> entry.value).toMap)
          else Left(s"duplicate keys: ${duplicates.mkString(", ")}"),
        map => Right(map.map((key, value) => Entry(key, value)).toList),
      )
}
