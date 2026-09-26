package homelab.llm

import homelab.llm.schema.JsonSchema

/**
 * A tool as a provider is told about it: what it is called, what it is for, and what it takes.
 *
 * Not JSON, because every provider spells this differently — one wraps it in a function object and calls
 * the schema `parameters`, another takes the three fields flat and calls it `input_schema`. Shaping it is
 * the adapter's, and it needs the parts rather than one of the shapes.
 *
 * @param name the name the model calls it by
 * @param description what it is for, in the words the model reads
 * @param arguments what it takes, described in the subset a model can be asked to fill
 */
final case class Advertised(name: String, description: String, arguments: JsonSchema)
