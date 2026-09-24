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
// Shape.Obj holds List[Shape.Obj.Field], and Field is (name, node, required)
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

```scala
Node.obj(
  Shape.Obj.Field("city", Node.text.describedAs("city name")),
  Shape.Obj.Field("unit", Node.enumeration("c", "f"), required = false),
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
  root        = Node.obj("children" -> Node.array(Node.ref("Tree"))),
  definitions = ListMap("Tree" -> Node.obj(Shape.Obj.Field("value", Node.text))),
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

**Properties are a list, not a map.** A property is a described thing that *has* a name — `Field(name,
node, required)` — rather than a name pointing at a description, and nothing ever looks one up. The
rendering turns that list into the two JSON members it implies. A zero-argument tool is `Obj(Nil)`, which
renders as an object with no properties and nothing required.

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

## From a case class to a schema

Nothing hand-writes these. `Generator.generate[A]` takes the `zio.schema.Schema[A]` the compiler derived
and turns it into a `JsonSchema`, or into `Unsupported` if `A` is outside the subset.

```scala
final case class Weather(city: String, days: Option[Int]) derives Schema

Generator.generate[Weather]    // Either[Unsupported, JsonSchema]
```

### Two passes, and why there are two

```scala
def generate[A](using schema: Schema[A]): Either[Unsupported, JsonSchema] =
  val recursive = recursiveTypes(schema, Set.empty, Set.empty)
  generateNodes(schema, recursive, Defs.empty).map((node, defs) => JsonSchema(node, defs.definitions))
```

**The first pass finds recursion; the second renders.** `recursiveTypes` walks the schema carrying the set
of type ids on the *current path*; a type that re-enters its own path is recursive. Only those become
`$defs` entries reached by `$ref` — everything else is inlined, so an ordinary record pays nothing for
machinery it does not need.

The pass is not an optimisation. zio-schema ties recursive knots with `Schema.Lazy`, and forcing one hands
back the very same instance, so a single-pass walk would not terminate. Knowing the recursive ids up front
is what lets the second pass stop and emit a `$ref` instead of descending forever.

### What the second pass does with each shape

`generateNodes` is one match over zio-schema's ADT. Threaded through it is `Defs` — `definitions`, the
completed `$defs` entries in discovery order, and `visiting`, the names whose definition is still being
built. Meeting a name in `visiting` *is* the recursive knot: the walk emits a `$ref` to it rather than
descending into a definition it is already in the middle of writing.

| zio-schema | becomes |
|---|---|
| `Schema.Primitive(t)` | `Node.primitive(t)`, or `Unsupported` for one this subset has no shape for |
| `Schema.Optional(inner)` | `Node.nullable(inner)` — `anyOf[inner, null]` |
| `Schema.Sequence` / `Schema.Set` | `Node.array(element)` |
| `Schema.Either(l, r)` | `Node.anyOf(l, r)` |
| `Schema.Transform(inner)` | whatever `inner` becomes — the transform is invisible on the wire |
| `Schema.Record` | an object, inline or hoisted into `$defs` |
| `Schema.Enum` | a string enum, or a union of discriminated objects |
| `Schema.Map` | **refused** — a map has open keys; this subset closes every object |
| `Schema.Tuple2` | **refused** — a tuple needs positional array items |
| `Schema.Fail` | **refused** — an unsatisfiable schema describes nothing |

Three more refusals live further in, where a sum type is rendered: a type with no cases at all, a
data-carrying one without `@discriminatorName`, and a case that already has a property named after the
discriminator. Each is shown below.

### A record

```scala
final case class Weather(city: String, days: Option[Int]) derives Schema
```

```jsonc
{
  "type": "object",
  "properties": {
    "city": { "type": "string" },
    "days": { "anyOf": [{ "type": "integer" }, { "type": "null" }] }
  },
  "required": ["city"],
  "additionalProperties": false
}
```

`days` is said to be optional **twice**: a null branch in its own schema, and absence from `required`. Both,
because the codec derived from the same `Schema` reads and writes null — advertising only the missing
`required` entry would describe a shape our own decoder disagrees with.

### A sum type with no payloads

```scala
enum Unit0 derives Schema:
  case Celsius, Fahrenheit
```

```jsonc
{ "type": "string", "enum": ["Celsius", "Fahrenheit"] }
```

That is the shape a codec writes for a payloadless case: a bare string. `@caseName` renames a case, and the
label the schema advertises is the label the decoder reads.

### A sum type carrying data

```scala
@discriminatorName("kind")
enum Ending derives Schema:
  case Done(summary: String)
  case GiveUp(reason: String)
```

```jsonc
{
  "anyOf": [
    { "type": "object",
      "properties": { "kind": { "type": "string", "enum": ["Done"] }, "summary": { "type": "string" } },
      "required": ["kind", "summary"], "additionalProperties": false },
    { "type": "object",
      "properties": { "kind": { "type": "string", "enum": ["GiveUp"] }, "reason": { "type": "string" } },
      "required": ["kind", "reason"], "additionalProperties": false }
  ]
}
```

Each branch gains the discriminator as a one-value enum — this subset's way of writing `const`.

**Without `@discriminatorName` the derivation refuses**, and the message says why: *"a sum type carrying
data needs `@discriminatorName`, so the tag the model writes is the tag the decoder reads."* Bare branches
would be ambiguous to a model whenever two cases share a shape, and — worse — would be a shape no codec
reads, since the decoder needs the tag to know which case it is holding. A schema nothing can decode is not
a useful thing to emit.

The other refusal here is a case that would carry the tag twice: if a case already has a property named
`kind`, it is rejected rather than silently overwritten.

### A recursive type

```scala
final case class Tree(value: String, children: List[Tree]) derives Schema
```

```jsonc
{
  "$ref": "#/$defs/Tree",
  "$defs": {
    "Tree": {
      "type": "object",
      "properties": {
        "value": { "type": "string" },
        "children": { "type": "array", "items": { "$ref": "#/$defs/Tree" } }
      },
      "required": ["value", "children"],
      "additionalProperties": false
    }
  }
}
```

`Tree` re-enters its own path, so the first pass marks it; the second hoists it into `$defs` and refers to
it — at the root and at every recurrence. A record that is *not* recursive is never hoisted, so a schema
only grows `$defs` when something actually needed it.

### Descriptions come from scaladoc

zio-schema lifts doc comments into the schema as `@description` annotations, and `Generator.describe`
renders them onto the node:

```scala
final case class Weather(
  /** The city to report on. */
  city: String
) derives Schema
```

```jsonc
{ "properties": { "city": { "description": "The city to report on.", "type": "string" } }, … }
```

This is the sharpest edge in the whole derivation. **A description is prompt text**, so a comment written
for a developer ships to the model, `/**` markers and all. It is worth reading a derived schema once before
trusting what a type says to a model.

## Reading it back

`JsonSchema.json` gives `zio.json.ast.Json`; `.toJson` on that gives the string a provider receives. There
is no parser in the other direction, and none is needed: schemas are produced here and sent, never read
back.
