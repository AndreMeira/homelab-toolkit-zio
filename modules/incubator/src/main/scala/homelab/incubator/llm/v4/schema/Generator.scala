package homelab.incubator.llm.v4.schema


import homelab.common.error.ApplicationError
import zio.Chunk
import zio.schema.annotation.{ caseName, description, discriminatorName }
import zio.schema.{ Schema, StandardType, TypeId }

import scala.annotation.tailrec
import scala.collection.immutable.ListMap


/**
 * Deriving a [[JsonSchema]] from a `zio.schema.Schema`.
 *
 * This is the only place a type can be *rejected*: the ADT admits exactly what a model can be asked to
 * produce, so anything outside that subset — a map, a tuple, `Unit` — fails here, at registry construction,
 * rather than at runtime when a model guesses at a shape it was never able to satisfy.
 *
 * '''Recursion is found before it is rendered.''' A first pass walks the schema tracking the types on the
 * current path; any type that re-enters its own path is recursive. Only those become `$defs` entries reached
 * by `$ref` — everything else is inlined, so an ordinary record does not pay for the machinery a recursive
 * one needs. Without that pass the walk would not terminate: zio-schema ties recursive knots with
 * `Schema.Lazy`, and forcing it yields the very same instance again.
 *
 * '''An `Option` is rendered as a null branch, everywhere.''' `anyOf[inner, null]` is what the codec derived
 * from the same schema actually accepts — it reads a null and writes one — so anything less would advertise a
 * shape our own decoder disagrees with. A field is *additionally* left out of `required`, so the ordinary
 * case still reads as "may be omitted". Flipping that one flag, and leaving the null branch alone, is what a
 * strict-mode provider wants; it is a render-time choice rather than a limit of the ADT.
 */
object Generator {

  /**
   * Why a type cannot be described to a model.
   *
   * An [[ApplicationError.EncodingError]]: this is the outbound direction failing — we cannot render a
   * description of the type for the wire. Nothing at runtime recovers from it; the fix is to change the type,
   * or to give it a schema that is describable (as `Schemas.mapAsEntries` does for maps).
   *
   * @param reason what was met, in terms a developer can act on
   */
  final case class Unsupported(reason: String) extends ApplicationError.EncodingError:
    override def message: String = reason

  /**
   * Derive the document describing `A`.
   *
   * @tparam A the type to describe, with a `zio.schema.Schema` in scope
   * @return the document, or why `A` cannot be described
   */
  def generate[A](using schema: Schema[A]): Either[Unsupported, JsonSchema] =
    val recursive = recursiveTypes(schema, Set.empty, Set.empty)
    Generator.generateNodes(schema, recursive, Defs.empty).map((node, defs) => JsonSchema(node, defs.definitions))

  /**
   * The definitions gathered so far, and the types whose definition is still being built.
   *
   * @param definitions the completed `$defs` entries, in discovery order
   * @param visiting the names currently under construction — meeting one again is the recursive knot
   */
  final private case class Defs(definitions: ListMap[String, Node], visiting: Set[String])

  private object Defs:
    val empty: Defs = Defs(ListMap.empty, Set.empty)

  /**
   * Force a `Schema.Lazy` until a real node appears.
   *
   * @param schema the possibly-deferred schema
   * @return the same schema with its thunks forced
   */
  @tailrec private def force(schema: Schema[?]): Schema[?] = schema match
    case lazySchema: Schema.Lazy[?] => force(lazySchema.schema)
    case other                      => other

  /**
   * The types that refer to themselves, found before rendering so the walk can terminate.
   *
   * @param schema the node to walk
   * @param path the type ids on the current path
   * @param found the recursive ids discovered so far
   * @return every id that re-enters its own path
   */
  private def recursiveTypes(schema: Schema[?], path: Set[TypeId], found: Set[TypeId]): Set[TypeId] =
    force(schema) match
      case record: Schema.Record[?]             =>
        if path.contains(record.id) then found + record.id
        else record.fields.foldLeft(found)((acc, f) => recursiveTypes(f.schema, path + record.id, acc))
      case enumeration: Schema.Enum[?]          =>
        if path.contains(enumeration.id) then found + enumeration.id
        else enumeration.cases.foldLeft(found)((acc, c) => recursiveTypes(c.schema, path + enumeration.id, acc))
      case Schema.Optional(inner, _)            => recursiveTypes(inner, path, found)
      case sequence: Schema.Sequence[?, ?, ?]   => recursiveTypes(sequence.elementSchema, path, found)
      case set: Schema.Set[?]                   => recursiveTypes(set.elementSchema, path, found)
      case Schema.Either(left, right, _)        => recursiveTypes(right, path, recursiveTypes(left, path, found))
      case transform: Schema.Transform[?, ?, ?] => recursiveTypes(transform.schema, path, found)
      case _                                    => found

  /**
   * Render one node, hoisting recursive types into definitions as they are met.
   *
   * @param schema the node to render
   * @param recursive the type ids that must be reached by reference
   * @param defs the definitions gathered so far
   * @return the node and the definitions after it, or why it cannot be described
   */
  private def generateNodes(
    schema: Schema[?],
    recursive: Set[TypeId],
    defs: Defs,
  ): Either[Unsupported, (Node, Defs)] =
    force(schema) match
      case Schema.Primitive(standardType, annotations) =>
        Node
          .primitive(standardType)
          .toRight(unsupported(standardType))
          .map(node => describe(node, annotations) -> defs)

      case Schema.Optional(inner, _) =>
        // Every optional value says so, wherever it sits: inside a list, an either, or a field. A field is
        // additionally left out of `required`, which is what makes the common case read normally.
        generateNodes(inner, recursive, defs).map((node, next) => Node.nullable(node) -> next)

      case sequence: Schema.Sequence[?, ?, ?] =>
        generateNodes(sequence.elementSchema, recursive, defs).map((node, next) => Node.array(node) -> next)

      case set: Schema.Set[?] =>
        generateNodes(set.elementSchema, recursive, defs).map((node, next) => Node.array(node) -> next)

      case Schema.Either(left, right, _) =>
        for
          (leftNode, afterLeft)   <- generateNodes(left, recursive, defs)
          (rightNode, afterRight) <- generateNodes(right, recursive, afterLeft)
        yield Node.anyOf(leftNode, rightNode) -> afterRight

      case transform: Schema.Transform[?, ?, ?] => generateNodes(transform.schema, recursive, defs)
      case record: Schema.Record[?]             => fromRecord(record, recursive, defs)
      case enumeration: Schema.Enum[?]          => fromEnum(enumeration, recursive, defs)

      case Schema.Map(_, _, _)     => Left(Unsupported("a map has open keys; this subset closes every object"))
      case Schema.Tuple2(_, _, _)  => Left(Unsupported("a tuple needs positional array items"))
      case Schema.Fail(message, _) => Left(Unsupported(s"an unsatisfiable schema: $message"))
      case other                   => Left(Unsupported(s"no rendering for ${other.getClass.getSimpleName}"))

  /**
   * Render a record, as a definition when it is recursive and inline when it is not.
   *
   * @param record the record to render
   * @param recursive the type ids that must be reached by reference
   * @param defs the definitions gathered so far
   * @return the node and the definitions after it, or why it cannot be described
   */
  private def fromRecord(
    record: Schema.Record[?],
    recursive: Set[TypeId],
    defs: Defs,
  ): Either[Unsupported, (Node, Defs)] =
    val name = nameOf(record.id)
    if !recursive.contains(record.id) || name.isEmpty
    then properties(record, recursive, defs).map((fields, next) => object0(record, fields) -> next)
    else
      val key = name.get
      if defs.visiting.contains(key) || defs.definitions.contains(key) then Right(Node.ref(key) -> defs)
      else
        properties(record, recursive, defs.copy(visiting = defs.visiting + key)).map { (fields, next) =>
          val definition = object0(record, fields)
          Node.ref(key) -> next.copy(
            definitions = next.definitions + (key -> definition),
            visiting = next.visiting - key,
          )
        }

  /**
   * Render a sum type.
   *
   * Cases carrying no data become a closed set of names — the shape a codec writes as a bare string.
   *
   * Cases carrying data become a union of *discriminated* objects, and only when the type says which tag
   * holds the case name (`@discriminatorName`). Without it the branches would be bare records: ambiguous to a
   * model whenever two cases share a shape and, worse, a shape no codec reads — a decoder needs the tag to
   * know which case it is holding. Refusing is the honest answer; a schema nothing can decode is not.
   *
   * @param enumeration the sum type to render
   * @param recursive the type ids that must be reached by reference
   * @param defs the definitions gathered so far
   * @return the node and the definitions after it, or why it cannot be described
   */
  private def fromEnum(
    enumeration: Schema.Enum[?],
    recursive: Set[TypeId],
    defs: Defs,
  ): Either[Unsupported, (Node, Defs)] =
    val cases = enumeration.cases.toList
    if cases.isEmpty then Left(Unsupported("a sum type with no cases describes no value"))
    else if cases.forall(one => payloadless(one.schema)) then
      val labels = cases.map(label)
      Right(describe(Node.enumeration(labels.head, labels.tail*), enumeration.annotations) -> defs)
    else
      discriminator(enumeration.annotations) match
        case None      =>
          Left(
            Unsupported(
              "a sum type carrying data needs @discriminatorName, so the tag the model writes is the tag the decoder reads"
            )
          )
        case Some(tag) =>
          branches(cases, tag, recursive, defs).map { (nodes, next) =>
            val union = nodes match
              case single :: Nil           => single
              case first :: second :: rest => Node.anyOf(first, second, rest*)
              case Nil                     => Node.nothing
            describe(union, enumeration.annotations) -> next
          }

  /**
   * Render each case as a discriminated object, threading the definitions through.
   *
   * @param cases the sum type's cases
   * @param tag the property carrying the case name
   * @param recursive the type ids that must be reached by reference
   * @param defs the definitions gathered so far
   * @return one node per case, or why a case cannot be described
   */
  private def branches(
    cases: List[Schema.Case[?, ?]],
    tag: String,
    recursive: Set[TypeId],
    defs: Defs,
  ): Either[Unsupported, (List[Node], Defs)] =
    cases.foldLeft[Either[Unsupported, (List[Node], Defs)]](Right(Nil -> defs)) { (acc, next) =>
      acc.flatMap { (nodes, carried) =>
        for
          (node, after) <- generateNodes(next.schema, recursive, carried)
          tagged        <- discriminated(tag, label(next), node)
        yield (nodes :+ tagged) -> after
      }
    }

  /**
   * Add the discriminator to a case's object, as a one-value enum — this subset's way of saying `const`.
   *
   * @param tag the property carrying the case name
   * @param name the case's name, as the codec writes it
   * @param node the case's rendered schema
   * @return the tagged object, or why the case cannot carry a tag
   */
  private def discriminated(tag: String, name: String, node: Node): Either[Unsupported, Node] =
    node.shape match {
      case Shape.Obj(properties) if properties.contains(tag) =>
        Left(Unsupported(s"case '$name' already has a property named '$tag', which the discriminator needs"))

      case Shape.Obj(properties) =>
        Right(Node(Shape.Obj(ListMap(tag -> Shape.Obj.Field(Node.enumeration(name))) ++ properties), node.description))

      case _ =>
        Left(Unsupported(s"case '$name' does not render as an object, so it cannot carry the '$tag' discriminator"))
    }

  /**
   * The tag a sum type's cases are distinguished by, if it names one.
   *
   * @param annotations the sum type's annotations
   * @return the discriminator property name
   */
  private def discriminator(annotations: Chunk[Any]): Option[String] =
    annotations.collectFirst { case named: discriminatorName => named.tag }

  /**
   * A case's name as the codec writes it — `@caseName` when given, the case's own name otherwise.
   *
   * @param enumCase the case
   * @return the name to put in the schema
   */
  private def label(enumCase: Schema.Case[?, ?]): String =
    enumCase.annotations.collectFirst { case renamed: caseName => renamed.name }.getOrElse(enumCase.id)

  /**
   * Render a record's fields, threading the definitions through each in turn.
   *
   * @param record the record whose fields to render
   * @param recursive the type ids that must be reached by reference
   * @param defs the definitions gathered so far
   * @return the fields and the definitions after them, or why one cannot be described
   */
  private def properties(
    record: Schema.Record[?],
    recursive: Set[TypeId],
    defs: Defs,
  ): Either[Unsupported, (List[(String, Shape.Obj.Field)], Defs)] = {
    type Acc = Either[Unsupported, (List[(String, Shape.Obj.Field)], Defs)]
    record.fields.foldLeft[Acc](Right(Nil -> defs)): (acc, field) =>
      acc.flatMap { (fields, carried) =>
        generateNodes(field.schema, recursive, carried).map { (node, after) =>
          val entry = field.name -> Shape.Obj.Field(describe(node, field.annotations), required = !optional(field.schema))
          (fields :+ entry) -> after
        }
      }
  }

  /**
   * Assemble an object node from rendered fields, carrying the type's own description.
   *
   * @param record the record being rendered
   * @param fields its rendered properties
   * @return the object node
   */
  private def object0(record: Schema.Record[?], fields: List[(String, Shape.Obj.Field)]): Node =
    describe(Node.obj(fields*), record.annotations)

  /**
   * Whether a field's schema is optional, and therefore left out of `required`.
   *
   * @param schema the field's schema
   * @return true when the value may be absent
   */
  private def optional(schema: Schema[?]): Boolean = force(schema) match
    case Schema.Optional(_, _) => true
    case _                     => false

  /**
   * Whether an enum case carries no data, making the enum a closed set of names.
   *
   * @param schema the case's schema
   * @return true when the case has no fields
   */
  private def payloadless(schema: Schema[?]): Boolean = force(schema) match
    case record: Schema.Record[?] => record.fields.isEmpty
    case _                        => false

  /**
   * Attach a `@description` annotation, if the type or field carries one.
   *
   * @param node the rendered node
   * @param annotations the annotations found beside it
   * @return the node, described where a description was given
   */
  private def describe(node: Node, annotations: Chunk[Any]): Node =
    annotations.collectFirst {
      case described: description => described.text
    } match {
      case Some(text) => node.describedAs(text)
      case None       => node
    }

  /**
   * The `$defs` name for a type id.
   *
   * Uses the simple type name: it is what a model reads, and a fully-qualified one is noise in a prompt. Two
   * distinct types sharing a simple name would collide — see the note in [[generate]]'s tests.
   *
   * @param id the type's id
   * @return the name, or nothing for a structural (anonymous) type
   */
  private def nameOf(id: TypeId): Option[String] = id match
    case TypeId.Nominal(_, _, typeName) => Some(typeName)
    case TypeId.Structural              => None

  /**
   * Map a zio-schema primitive onto the subset.
   *
   * @param standardType the primitive met
   * @return the node, or why the primitive cannot be described
   */
  /**
   * Why a primitive this subset cannot express was refused.
   *
   * @param standardType what zio-schema said the value was
   * @return the rejection, in terms a developer can act on
   */
  private def unsupported(standardType: StandardType[?]): Unsupported = standardType match
    case StandardType.UnitType => Unsupported("Unit describes no value a model could send")
    case other                 => Unsupported(s"no rendering for the primitive ${other.tag}")
}
