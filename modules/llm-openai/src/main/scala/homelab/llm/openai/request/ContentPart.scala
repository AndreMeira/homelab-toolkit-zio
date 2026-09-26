package homelab.llm.openai.request


import homelab.llm.Message
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One piece of a message this adapter writes.
 *
 * A part the caller built is not one of these — it is already JSON, and goes out as written. That is why it
 * is not a case here and why [[ContentPart.parts]] answers in `Json`.
 *
 * @see [[Message.Content]], which this is the wire's spelling of
 */
@jsonDiscriminator("type")
enum ContentPart derives JsonEncoder {

  /**
   * Words.
   *
   * @param text what they are
   */
  @jsonHint("text") case Text(text: String)
}


object ContentPart:

  /**
   * The parts of a message, as the wire carries them.
   *
   * A part the caller built passes through as it was written; a part this adapter models is written by the
   * derived encoder.
   *
   * @param content what the toolkit holds
   * @return the parts to send
   */
  def parts(content: Chunk[Message.Content]): Chunk[Json] = content.map(part)

  /**
   * The words of a message, for the one field that takes a string rather than an array.
   *
   * A part this adapter did not write cannot become text, so it is left out rather than guessed at.
   *
   * @param content the parts
   * @return their words, run together
   */
  def text(content: Chunk[Message.Content]): String =
    content.collect { case Message.Content.Text(said) => said }.mkString

  /**
   * One part.
   *
   * @param content what the toolkit holds
   * @return the part to send
   */
  private def part(content: Message.Content): Json = content match
    case Message.Content.Text(said) => written(Text(said))
    case Message.Content.Raw(json)  => json

  /**
   * One part this adapter wrote, as JSON.
   *
   * The derived encoder answers an `Either`, and the left is unreachable: a text part is one string, and
   * nothing about it can fail to be written.
   *
   * @param part the part
   * @return it, written
   */
  private def written(part: ContentPart): Json = part.toJsonAST.getOrElse(Json.Null)
