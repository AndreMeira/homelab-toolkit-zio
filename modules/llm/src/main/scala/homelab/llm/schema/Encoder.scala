package homelab.llm.schema


import homelab.llm.schema.Generator.Unsupported
import zio.schema.Schema


/**
 * How a type is described to a model: a whole [[JsonSchema]], not a bare node, so a recursive type can carry
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

  def apply[A: Encoder as encoder]: Encoder[A] = encoder

  /**
   * Describe `A` from its `zio.schema.Schema`. Total: every type has an encoder, and one that cannot be
   * described says so when asked.
   *
   * @tparam A the type to describe
   * @return the encoder
   */
  given derived[A](using Schema[A]): Encoder[A] = new Encoder[A]:
    override def get: Either[Unsupported, JsonSchema] = Generator.generate[A]
