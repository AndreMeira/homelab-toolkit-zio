package homelab.llm.openai.request


import homelab.llm.schema.JsonSchema
import zio.json.*
import zio.json.ast.Json


/**
 * What shape the answer must take.
 *
 * [[ResponseFormat.Schema]] is the one worth reaching for: a provider that supports it constrains decoding
 * to the schema, so what comes back parses by construction rather than by luck. The schema it takes is the
 * toolkit's own, so a type described for a tool can be asked for as an answer without being described
 * twice.
 *
 * @see [[CompletionRequest.responseFormat]]
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
   * A schema, as this protocol asks for one.
   *
   * @param name what to call it, which a provider may report in an error
   * @param schema what the answer must conform to
   * @param strict whether the provider must constrain decoding rather than merely ask
   */
  final case class Described(name: String, schema: Json, strict: Boolean = true) derives JsonEncoder

  /**
   * An answer that must conform to a schema, strictly.
   *
   * @param name what to call it
   * @param schema what the answer must conform to
   * @return the format to ask for
   */
  def conforming(name: String, schema: JsonSchema): ResponseFormat = Schema(Described(name, schema.json))
