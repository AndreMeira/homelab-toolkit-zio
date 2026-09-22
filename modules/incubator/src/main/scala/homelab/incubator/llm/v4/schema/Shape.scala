package homelab.incubator.llm.v4.schema

import zio.json.ast.Json

import scala.collection.immutable.ListMap


/** What kind of value a node describes — the closed set this subset admits. */
enum Shape {

  /** A string, optionally tagged with a `format` the provider understands (`date-time`, `uri`, …). */
  case Text(format: Option[String] = None)

  /** A JSON number. */
  case Number

  /** A whole number. */
  case Integer

  /** A boolean. */
  case Bool

  /** JSON `null` — on its own, mostly useful as an [[AnyOf]] branch making a value nullable. */
  case Null

  /** A closed set of string values: a Scala `enum` of case objects, or any sum type without payloads. */
  case Enumeration(first: String, rest: List[String])

  /** An object with known properties. `additionalProperties` is always rendered as `false`. */
  case Obj(properties: ListMap[String, Obj.Field])

  /** A homogeneous array. Tuple-typed arrays (`prefixItems`) are deliberately absent — see the note below. */
  case Arr(items: Node)

  /** A union of at least two alternatives — how a sum type with payloads, or a nullable value, is said. */
  case AnyOf(first: Node, second: Node, rest: List[Node])

  /** A reference to a [[JsonSchema]] definition, by name — the only way to describe a recursive type. */
  case Reference(name: String)

  /**
   * The JSON json this shape contributes, before any description is added.
   *
   * @return the rendered json, in the order a reader expects them
   */
  private[schema] def json: Json.Obj = this match
    case Text(None)        => Json.Obj("type" -> Json.Str("string"))
    case Text(Some(fmt))   => Json.Obj("type" -> Json.Str("string"), "format" -> Json.Str(fmt))
    case Number            => Json.Obj("type" -> Json.Str("number"))
    case Integer           => Json.Obj("type" -> Json.Str("integer"))
    case Bool              => Json.Obj("type" -> Json.Str("boolean"))
    case Null              => Json.Obj("type" -> Json.Str("null"))
    case Arr(items)        => Json.Obj("type" -> Json.Str("array"), "items" -> items.json)
    case AnyOf(a, b, rest) => Json.Obj("anyOf" -> Json.Arr((a :: b :: rest).map(_.json)*))
    case Reference(name)   => Json.Obj("$ref" -> Json.Str(s"#/$$defs/$name"))
    case Enumeration(h, t) => Json.Obj("type" -> Json.Str("string"), "enum" -> Json.Arr((h :: t).map(Json.Str(_))*))
    case Obj(properties)   =>
      Json.Obj(
        "type"                 -> Json.Str("object"),
        "properties"           -> Json.Obj(properties.map((name, field) => name -> field.node.json).toSeq*),
        "required"             -> Json.Arr(properties.collect { case (name, f) if f.required => Json.Str(name) }.toSeq*),
        "additionalProperties" -> Json.Bool(false),
      )

}


object Shape:
  object Obj:
    /**
     * One property of a [[Obj]] — its schema and whether it must be present.
     *
     * Holding `required` here, rather than in a list beside the properties, is what stops a schema requiring a
     * property it does not describe.
     *
     * @param node     what the property holds
     * @param required whether the model must supply it
     */
    final case class Field(node: Node, required: Boolean = true)
