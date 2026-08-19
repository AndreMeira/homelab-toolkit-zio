package homelab.incubator.llm.v2


import zio.json.ast.Json
import zio.schema.Schema

import scala.collection.immutable.ListMap


/**
 * A JSON Schema, restricted to what a model can actually be asked to produce.
 *
 * This is not general JSON Schema. It is the subset a tool's arguments can be described in, chosen so that
 * **the inexpressible is unrepresentable**: once a `JsonSchema` value exists, rendering it cannot fail and
 * the result is something a provider will accept. That is what makes the derivation direction
 * (`zio.schema.Schema[A] => Either[Unsupported, JsonSchema]`) the only place that can reject a type — a
 * `case class` with a field the model could never fill fails there, at registry construction, rather than at
 * runtime when a model guesses.
 *
 * Three encodings carry an invariant rather than documenting one:
 *
 *   - **`required` does not exist as a list.** A field carries its own [[Field.required]] flag, so a schema
 *     cannot require a property it does not have. The rendered `required` array is computed.
 *   - **`additionalProperties` is not a field.** It renders as `false`, always. Strict-mode providers demand
 *     it, and offering the choice invites someone to turn it on and silently lose strictness.
 *   - **[[Shape.AnyOf]] takes two alternatives plus a rest**, so a one-branch union cannot be built.
 *
 * The one invariant left dangling is `$ref`: [[Shape.Reference]] names a definition that [[Document]] may not
 * hold. [[Document.unresolved]] reports those rather than pretending they cannot happen.
 *
 * @param shape what kind of value this describes
 * @param description prose shown to the model — prompt engineering as much as typing, so it is not optional
 *                    in practice even though it is here
 */
final case class JsonSchema(
  root: JsonSchema.Node,
  definitions: ListMap[String, JsonSchema.Node] = ListMap.empty,
) {

  /**
   * Reference names with no matching definition — the one invariant this ADT cannot carry in its types.
   *
   * @return every dangling `$ref` name, empty when the schema is closed
   */
  def unresolved: Set[String] = names(root).filterNot(definitions.keySet) ++
    definitions.values.flatMap(names).filterNot(definitions.keySet)

  /**
   * Render the whole schema, inlining `$defs` when there are any.
   *
   * @return the JSON Schema document a provider is handed
   */
  def json: Json =
    if definitions.isEmpty then root.json
    else
      root.json match
        case Json.Obj(fields) => Json.Obj(fields :+ ("$defs" -> jsonDefinitions))
        case other            => other

  private def jsonDefinitions: Json.Obj =
    Json.Obj(definitions.map((name, node) => name -> node.json).toSeq*)

  /**
   * Every reference name reachable from `node`.
   *
   * @param node the node to walk
   * @return the names its subtree refers to
   */
  private def names(node: JsonSchema.Node): Set[String] = node.shape match
    case JsonSchema.Shape.Reference(name)   => Set(name)
    case JsonSchema.Shape.Obj(properties)   => properties.values.flatMap(field => names(field.schema)).toSet
    case JsonSchema.Shape.Arr(items)        => names(items)
    case JsonSchema.Shape.AnyOf(a, b, rest) => (a :: b :: rest).flatMap(names).toSet
    case _                                  => Set.empty
}


object JsonSchema {

  /** The rendered json of one node, in declaration order. */
  private type TypeDescription = Seq[(String, Json)]
  private def TypeDescription(members: (String, Json)*): TypeDescription = members

  /**
   * One node of a schema: what kind of value it is, and what to tell the model about it.
   *
   * Structure and annotation are separate so that `description` is said once, rather than repeated on every
   * shape or wrapped in a case that could nest inside itself.
   *
   * @param shape what kind of value this node describes
   * @param description prose shown to the model — prompt engineering as much as typing
   */
  final case class Node(shape: Shape, description: Option[String] = None) {

    /**
     * Attach prose for the model.
     *
     * @param text the description to render
     * @return this node, described
     */
    def describedAs(text: String): Node = copy(description = Some(text))

    /**
     * Render this node.
     *
     * @return the JSON Schema fragment for it
     */
    def json: Json = Json.Obj(
      // Description first: it is what a reader — human or model — should meet before the mechanics of the
      // shape. Member order carries no meaning to a validator, so it is free to spend on legibility.
      description.fold(shape.json.fields.toList) { text =>
        ("description" -> Json.Str(text)) +: shape.json.fields.toList
      }*
    )
  }

  /**
   * One property of an [[Shape.Obj]] — its schema and whether it must be present.
   *
   * Holding `required` here, rather than in a list beside the properties, is what stops a schema requiring a
   * property it does not describedAs.
   *
   * @param schema   what the property holds
   * @param required whether the model must supply it
   */
  final case class Field(schema: Node, required: Boolean = true)

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
    case Obj(properties: ListMap[String, Field])

    /** A homogeneous array. Tuple-typed arrays (`prefixItems`) are deliberately absent — see the note below. */
    case Arr(items: Node)

    /** A union of at least two alternatives — how a sum type with payloads, or a nullable value, is said. */
    case AnyOf(first: Node, second: Node, rest: List[Node])

    /** A reference to a [[Document]] definition, by name — the only way to describedAs a recursive type. */
    case Reference(name: String)

    /**
     * The JSON json this shape contributes, before any description is added.
     *
     * @return the rendered json, in the order a reader expects them
     */
    private[JsonSchema] def json: Json.Obj = this match
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
          "properties"           -> Json.Obj(properties.map((name, field) => name -> field.schema.json).toSeq*),
          "required"             -> Json.Arr(properties.collect { case (name, f) if f.required => Json.Str(name) }.toSeq*),
          "additionalProperties" -> Json.Bool(false),
        )

  }

  val text: Node    = Node(Shape.Text())
  val number: Node  = Node(Shape.Number)
  val integer: Node = Node(Shape.Integer)
  val boolean: Node = Node(Shape.Bool)
  val nothing: Node = Node(Shape.Null)

  /**
   * A string in a known format (`date-time`, `uri`, `uuid`, …).
   *
   * @param format the format tag
   * @return the schema
   */
  def formatted(format: String): Node = Node(Shape.Text(Some(format)))

  /**
   * A closed set of string values.
   *
   * @param first the first admissible value — present so the set cannot be empty
   * @param rest the remaining values
   * @return the schema
   */
  def enumeration(first: String, rest: String*): Node = Node(Shape.Enumeration(first, rest.toList))

  /**
   * An object. Properties are required unless their [[Field]] says otherwise.
   *
   * @param properties the properties, in the order the model should read them
   * @return the schema
   */
  def obj(properties: (String, Field)*): Node = Node(Shape.Obj(ListMap.from(properties)))

  /**
   * An array of `items`.
   *
   * @param items what every element holds
   * @return the schema
   */
  def array(items: Node): Node = Node(Shape.Arr(items))

  /**
   * A union of at least two alternatives.
   *
   * @param first the first alternative
   * @param second the second — required, so a one-branch union cannot be built
   * @param rest any further alternatives
   * @return the schema
   */
  def anyOf(first: Node, second: Node, rest: Node*): Node =
    Node(Shape.AnyOf(first, second, rest.toList))

  /**
   * `schema` or null — how an optional value is said to a strict-mode provider, which requires every property
   * to be listed as required.
   *
   * @param Node the value when present
   * @return the nullable schema
   */
  def nullable(node: Node): Node = anyOf(node, nothing)

  /**
   * A reference to a named definition in the enclosing [[Document]].
   *
   * @param name the definition's name
   * @return the schema
   */
  def ref(name: String): Node = Node(Shape.Reference(name))

  /**
   * How a type is described to a model: a whole [[Document]], not a bare node, so a recursive type can carry
   * the definitions its references point at.
   *
   * @tparam A the type described
   */
  trait Encoder[A]:

    /**
     * The document describing `A`, or why it cannot be described.
     *
     * The failure is in the value rather than in a throw, because it cannot be found any earlier: a
     * `zio.schema.Schema` is an object built at runtime, so nothing about `A` is available to the typer.
     * Returning it means a registry assembling its tools handles every rejection the same way it handles any
     * other data — collected, reported together, at the one point where the answer is actionable.
     *
     * @return the schema document, definitions included, or why this type is outside the describable subset
     */
    def get: Either[Unsupported, JsonSchema]

  object Encoder:
    
    def apply[A : Encoder]: Encoder[A] = summon

    /**
     * Describe `A` from its `zio.schema.Schema`. Total: every type has an encoder, and one that cannot be
     * described says so when asked.
     *
     * @tparam A the type to describedAs
     * @return the encoder
     */
    given derived[A](using Schema[A]): Encoder[A] = new Encoder[A]:
      override def get: Either[Unsupported, JsonSchema] = JsonSchemaDerivation.derive[A]

  export JsonSchemaDerivation.{ derive, Unsupported }
}
