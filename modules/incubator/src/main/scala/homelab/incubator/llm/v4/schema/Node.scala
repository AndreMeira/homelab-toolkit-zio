package homelab.incubator.llm.v4.schema


import zio.json.ast.Json
import zio.schema.StandardType

import scala.collection.immutable.ListMap


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
    (description match {
      case None       => shape.json.fields.toList
      case Some(text) => ("description" -> Json.Str(text)) :: shape.json.fields.toList
    })*
  )
}


object Node:

  /** Any string. */
  val text: Node = Node(Shape.Text())

  /** Any JSON number. */
  val number: Node = Node(Shape.Number)

  /** A whole number. */
  val integer: Node = Node(Shape.Integer)

  /** True or false. */
  val boolean: Node = Node(Shape.Bool)

  /** JSON `null`, which is mostly useful as a branch of [[anyOf]]. */
  val nothing: Node = Node(Shape.Null)

  /**
   * The node for a primitive, when this subset can express it.
   *
   * Answers with a node or with nothing; calling absence a *rejection* is the derivation's to do, since it
   * is the one place that knows a type was being described for a model and can say what to change.
   *
   * @param standardType what zio-schema says the value is
   * @return its node, or nothing when no shape here describes it
   */
  def primitive(standardType: StandardType[?]): Option[Node] = standardType match
    case StandardType.StringType     => Some(text)
    case StandardType.CharType       => Some(text)
    case StandardType.BoolType       => Some(boolean)
    case StandardType.ByteType       => Some(integer)
    case StandardType.ShortType      => Some(integer)
    case StandardType.IntType        => Some(integer)
    case StandardType.LongType       => Some(integer)
    case StandardType.BigIntegerType => Some(integer)
    case StandardType.FloatType      => Some(number)
    case StandardType.DoubleType     => Some(number)
    case StandardType.BigDecimalType => Some(number)
    case StandardType.UUIDType       => Some(formatted("uuid"))
    case StandardType.InstantType    => Some(formatted("date-time"))
    case StandardType.LocalDateType  => Some(formatted("date"))
    case StandardType.LocalTimeType  => Some(formatted("time"))
    case _                           => None

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
   * @param rest  the remaining values
   * @return the schema
   */
  def enumeration(first: String, rest: String*): Node = Node(Shape.Enumeration(first, rest.toList))

  /**
   * An object. Fields are required unless their [[Shape.Obj.Field]] says otherwise.
   *
   * @param fields the properties, in the order the model should read them
   * @return the schema
   */
  def obj(fields: Shape.Obj.Field*): Node = Node(Shape.Obj(fields.toList))

  /**
   * An object whose properties are all required — the common case, written as pairs.
   *
   * The `using DummyImplicit` is what lets this share a name with the [[Shape.Obj.Field]] overload: both
   * erase to one vararg parameter, and the extra empty parameter list is what tells them apart. Reach for
   * the other one when a property is optional or carries its own description.
   *
   * @param fields the properties, in the order the model should read them
   * @return the schema
   */
  def obj(fields: (String, Node)*)(using DummyImplicit): Node =
    Node(Shape.Obj(fields.toList.map(Shape.Obj.Field(_, _))))

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
   * @param first  the first alternative
   * @param second the second — required, so a one-branch union cannot be built
   * @param rest   any further alternatives
   * @return the schema
   */
  def anyOf(first: Node, second: Node, rest: Node*): Node =
    Node(Shape.AnyOf(first, second, rest.toList))

  /**
   * `schema` or null — how an optional value is said to a strict-mode provider, which requires every property
   * to be listed as required.
   *
   * @param node the value when present
   * @return the nullable schema
   */
  def nullable(node: Node): Node = anyOf(node, nothing)

  /**
   * A reference to a named definition in the enclosing [[JsonSchema]].
   *
   * @param name the definition's name
   * @return the schema
   */
  def ref(name: String): Node = Node(Shape.Reference(name))
