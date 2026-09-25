package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import homelab.llm.Tool
import homelab.incubator.llm.v4.playground.chat.Conversation
import zio.schema.{ Schema, derived }
import zio.{ IO, Random, ZIO }


/**
 * The tool that starts work outliving the call that asked for it.
 *
 * What it answers is not the result — it is a receipt. The model reads that the work is running and carries
 * on, and the answer arrives later as a message, put there by whoever the nudge wakes. Nothing in the
 * conversation records that something is owed: the row this writes is the only record, and it is the one a
 * sweep would read.
 *
 * The conversation comes from the caller context, so a call cannot deliver into a conversation other than
 * the one it was made in — the model is never offered the choice.
 *
 * @param store where the work this starts is recorded
 * @param investigator what actually does it
 */
final class Investigate(store: InvestigationStore, investigator: Investigator)
    extends Tool.Definition[Conversation, Investigate.Topic, Investigate.Receipt](
      "investigate",
      "Start a long investigation of a topic. It answers immediately with a receipt; the findings arrive " +
        "later as a message. Ask for one when a question needs more than you can work out here.",
    ) {

  /**
   * Record the work, start it, and hand the model its receipt.
   *
   * The row is written before the work is started, so work that is running is work that is recorded.
   *
   * @param context which conversation this call was made in
   * @param input what the model wants investigated
   * @return the receipt; aborts when the store or the investigator does
   */
  override def handle(
    context: Conversation,
    input: Investigate.Topic,
  ): IO[ApplicationError, Tool.Result[Investigate.Receipt]] =
    for
      id           <- Random.nextUUID.map(uuid => Investigation.Id(uuid.toString))
      investigation = Investigation(id, context, input.topic, answer = None)
      _            <- store.start(investigation)
      _            <- investigator.start(investigation)
    yield Tool.Result.success(Investigate.Receipt(id, Investigate.Working))
}


object Investigate:

  /** What the model is told while it waits, which is the whole of what a receipt means. */
  val Working: String = "running; the findings will arrive as a message"

  /** How an investigation's name is written into a schema, and read back out of one. */
  given Schema[Investigation.Id] = Schema[String].transform(Investigation.Id.apply, identity)

  /**
   * What to investigate.
   *
   * @param topic the question to look into, in the model's own words
   */
  final case class Topic(topic: String) derives Schema

  /**
   * What a started investigation answers with.
   *
   * The id is in the receipt so the model can refer to it, and so a reader of the conversation can pair a
   * delivery with the call that asked for it. It is not a promise the conversation tracks — nothing reads
   * it back to decide anything.
   *
   * @param investigation what the work is called
   * @param status what is happening, in the words the model reads
   */
  final case class Receipt(investigation: Investigation.Id, status: String) derives Schema
