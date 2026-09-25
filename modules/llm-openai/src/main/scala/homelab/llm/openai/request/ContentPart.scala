package homelab.llm.openai.request


import homelab.llm.Message
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One piece of a message, as a request carries it.
 *
 * @see [[Message.Content]], which this is the wire's spelling of
 */
enum ContentPart {

  /**
   * Words.
   *
   * @param text what they are
   */
  case Text(text: String)

  /**
   * A part the toolkit does not model, which the caller built and which goes out as it was written.
   *
   * @param json the part
   */
  case Raw(json: Json)
}


object ContentPart:

  /**
   * How a part is written.
   *
   * A [[Raw]] is spliced rather than wrapped — what the caller built is the part, not a field of one — and
   * that is why this is written by hand where everything around it is derived.
   */
  given JsonEncoder[ContentPart] = JsonEncoder[Json].contramap {
    case Text(text) => Json.Obj("type" -> Json.Str("text"), "text" -> Json.Str(text))
    case Raw(json)  => json
  }

  /**
   * The parts of a message, as the wire carries them.
   *
   * @param content what the toolkit holds
   * @return the parts to send
   */
  def from(content: Chunk[Message.Content]): List[ContentPart] = content.map(part).toList

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
  private def part(content: Message.Content): ContentPart = content match
    case Message.Content.Text(text) => Text(text)
    case Message.Content.Raw(json)  => Raw(json)
