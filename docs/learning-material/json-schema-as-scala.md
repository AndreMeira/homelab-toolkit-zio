---
title: "JSON Schema as Scala — what each case renders to, and why the subset stops where it does"
type: learning-material
status: current
updated: 2026-09-22
tags: [json-schema, schema, llm, tool-calling, adt, zio-schema]
---

# JSON Schema as Scala

JSON Schema is a large specification. A tool's `parameters` uses a small corner of it, and
`llm/v4/schema` models exactly that corner as an ADT — small enough that **rendering cannot fail**, because
nothing outside the corner can be built in the first place.

This page is the translation table: what each Scala case becomes on the wire, and what the shape of the
Scala says about the schemas it refuses to express.

## The three types

```scala
final case class JsonSchema(root: Node, definitions: ListMap[String, Node] = ListMap.empty)
final case class Node(shape: Shape, description: Option[String] = None)
enum Shape { case Text, Number, Integer, Bool, Null, Enumeration, Obj, Arr, AnyOf, Reference }
```

They are three levels, not three names for one thing:

- **`JsonSchema` is a document** — one root, plus the `$defs` table references resolve against.
- **`Node` is one described value** — a shape, plus prose for the model.
- **`Shape` is what kind of value it is** — the closed set below.

Structure and annotation are separate on purpose: `description` is said once, on `Node`, rather than
repeated on every shape or wrapped in a case that could nest inside itself.

## The table

| Scala | JSON |
|---|---|
| `Node.text` | `{"type":"string"}` |
| `Node.formatted("date-time")` | `{"type":"string","format":"date-time"}` |
| `Node.number` | `{"type":"number"}` |
| `Node.integer` | `{"type":"integer"}` |
| `Node.boolean` | `{"type":"boolean"}` |
| `Node.nothing` | `{"type":"null"}` |
| `Node.enumeration("red", "green")` | `{"type":"string","enum":["red","green"]}` |
| `Node.array(Node.text)` | `{"type":"array","items":{"type":"string"}}` |
| `Node.anyOf(a, b)` | `{"anyOf":[ a, b ]}` |
| `Node.nullable(Node.text)` | `{"anyOf":[{"type":"string"},{"type":"null"}]}` |
| `Node.ref("Tree")` | `{"$ref":"#/$defs/Tree"}` |
| `node.describedAs("the city")` | adds `"description":"the city"`, rendered **first** |

An object is the one case that renders more than it stores:

```scala
Node.obj(
  "city" -> Shape.Obj.Field(Node.text.describedAs("city name")),
  "unit" -> Shape.Obj.Field(Node.enumeration("c", "f"), required = false),
)
```

```jsonc
{
  "type": "object",
  "properties": {
    "city": { "description": "city name", "type": "string" },
    "unit": { "type": "string", "enum": ["c", "f"] }
  },
  "required": ["city"],                 // computed from the fields, never stored
  "additionalProperties": false         // always, never a choice
}
```

And a document adds its table at the end, only when there is one:

```scala
JsonSchema(
  root        = Node.obj("children" -> Shape.Obj.Field(Node.array(Node.ref("Tree")))),
  definitions = ListMap("Tree" -> Node.obj("value" -> Shape.Obj.Field(Node.text))),
)
```

```jsonc
{
  "type": "object",
  "properties": { "children": { "type": "array", "items": { "$ref": "#/$defs/Tree" } } },
  "required": ["children"],
  "additionalProperties": false,
  "$defs": { "Tree": { "type": "object", … } }
}
```

## What the Scala refuses to say

Four decisions in the types stop a schema being wrong, rather than documenting that it should not be.

**`required` is not a list.** JSON Schema puts required property names in an array beside `properties`,
which lets a document require a property it never describes. Here each property carries its own
`Shape.Obj.Field.required`, and the array is *computed* at render. The mismatch cannot be written down.

**`additionalProperties` is not a field.** It renders as `false`, always. Strict-mode providers demand it,
and offering the choice invites someone to set it true and lose strictness without noticing.

**A union has at least two branches.** `AnyOf(first: Node, second: Node, rest: List[Node])` — JSON Schema
happily accepts `{"anyOf":[x]}`, which means "x" with extra steps. The signature makes the one-branch union
unrepresentable.

**An enumeration has at least one value.** `Enumeration(first: String, rest: List[String])`, so
`{"enum":[]}` — a type with no inhabitants — cannot be built.

The one invariant the types *cannot* carry is `$ref` resolution: `Shape.Reference("Tree")` names a
definition the document may not hold. `JsonSchema.unresolved` reports dangling names rather than pretending
they cannot happen. It is the honest exception, and it is called out in the class doc for that reason.

## What it leaves out, deliberately

Everything else in JSON Schema. Worth knowing *why* for the ones people reach for:

- **`prefixItems`** (tuple-typed arrays) — providers vary on support, and a model filling a positional array
  is a worse prompt than one filling named fields.
- **Numeric and string constraints** (`minimum`, `pattern`, `minLength`) — they are validation, and nothing
  here validates. A model that violates one produces arguments the decoder rejects, which reaches the model
  as a failed tool result. Adding them to the schema would suggest an enforcement that does not exist.
- **`oneOf` / `allOf` / `not`** — `anyOf` is what a sum type needs, and the others invite schemas whose
  failure modes are hard to explain to a model.
- **`$id`, `$anchor`, dynamic scope** — the machinery that makes `$ref` resolution interesting. One flat
  `$defs` table at the root is all a tool's arguments need, and skipping the rest is what lets
  `JsonSchema.unresolved` be four lines.

## Where it comes from

Nothing hand-writes these. `Generator.derive[A]` turns a `zio.schema.Schema[A]` into a `JsonSchema`, or
into `Unsupported` if `A` is outside the subset — a `Map`, a tuple, `Unit`. That is the only place a type
can be rejected, and it happens when a registry is assembled rather than when a model guesses.

Two behaviours are worth knowing when reading a derived schema:

- **Scaladoc becomes `description`.** zio-schema lifts doc comments into the schema, so a stray comment on
  an argument type ships to the model as prompt text.
- **An `Option[A]` renders as `anyOf[A, null]` *and* leaves the field out of `required`.** Both, because the
  codec derived from the same `Schema` reads and writes null — advertising anything less would describe a
  shape our own decoder disagrees with.

## Reading it back

`JsonSchema.json` gives `zio.json.ast.Json`; `.toJson` on that gives the string a provider receives. There
is no parser in the other direction, and none is needed: schemas are produced here and sent, never read
back.
