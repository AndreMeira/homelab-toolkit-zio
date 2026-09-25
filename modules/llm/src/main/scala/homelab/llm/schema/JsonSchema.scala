package homelab.llm.schema


import zio.json.ast.Json

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
 *   - **`required` does not exist as a list.** A field carries its own [[Shape.Obj.Field.required]] flag, so a schema
 *     cannot require a property it does not have. The rendered `required` array is computed.
 *   - **`additionalProperties` is not a field.** It renders as `false`, always. Strict-mode providers demand
 *     it, and offering the choice invites someone to turn it on and silently lose strictness.
 *   - **[[Shape.AnyOf]] takes two alternatives plus a rest**, so a one-branch union cannot be built.
 *
 * The one invariant left dangling is `$ref`: [[Shape.Reference]] names a definition this may not hold.
 * [[unresolved]] reports those rather than pretending they cannot happen.
 *
 * @param root what the schema describes at the top level
 * @param definitions the named nodes `$ref` points at — only recursive types need any
 */
final case class JsonSchema(root: Node, definitions: ListMap[String, Node] = ListMap.empty) {

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
  private def names(node: Node): Set[String] = node.shape match
    case Shape.Reference(name)   => Set(name)
    case Shape.Obj(fields)       => fields.flatMap(field => names(field.node)).toSet
    case Shape.Arr(items)        => names(items)
    case Shape.AnyOf(a, b, rest) => (a :: b :: rest).flatMap(names).toSet
    case _                       => Set.empty
}


