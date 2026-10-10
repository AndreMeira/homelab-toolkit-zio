package homelab.llm.mistral.request


import homelab.llm.schema.JsonSchema
import zio.json.*
import zio.json.ast.Json


/**
 * What shape the answer must take.
 *
 * JSON of any shape also needs the conversation to tell the model to produce JSON, which Mistral requires
 * of that mode. A schema is the one worth reaching for: with `strict` set, decoding is constrained to it.
 *
 * @see [[CompletionRequest]]
 */
@jsonDiscriminator("type")
enum ResponseFormat derives JsonEncoder {

  /** Words, which is what a model does anyway. */
  @jsonHint("text") case Text

  /** Valid JSON of any shape, which says nothing about which shape. */
  @jsonHint("json_object") case JsonObject

  /**
   * JSON conforming to a schema.
   *
   * @param jsonSchema what it must conform to
   */
  @jsonHint("json_schema") @jsonMemberNames(SnakeCase) case Schema(jsonSchema: ResponseFormat.Described)
}


object ResponseFormat:

  /**
   * A schema, as this API asks for one.
   *
   * @param name what to call it, which the API may report in an error
   * @param schema what the answer must conform to
   * @param description what the answer is, in the words the model reads
   * @param strict whether decoding is constrained to the schema rather than merely asked for, which the API
   *               defaults to false
   */
  final case class Described(
    name: String,
    schema: Json,
    description: Option[String] = None,
    strict: Option[Boolean] = None,
  ) derives JsonEncoder

  /**
   * An answer that must conform to a schema, strictly.
   *
   * @param name what to call it
   * @param schema what the answer must conform to
   * @return the format to ask for
   */
  def conforming(name: String, schema: JsonSchema): ResponseFormat =
    Schema(Described(name, schema.json, strict = Some(true)))
