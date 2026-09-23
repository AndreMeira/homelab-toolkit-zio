package homelab.incubator.llm.v4


import zio.Chunk


/**
 * Turning what a model and its tools produced into the messages a conversation carries.
 *
 * Only the appending half lives here; what a conversation is waiting for is read by [[Progress.from]], off
 * the same messages. Nothing either half produces records where a turn has got to — the messages say it, so
 * there is no second record to keep in step.
 */
object Transcript {

  /**
   * What the model said, as the conversation carries it.
   *
   * A turn goes back as it came — words and calls both — so what the model reads next is what it wrote.
   * Why it stopped and what it cost are facts about the call rather than about the turn, and stay behind.
   *
   * @param completion what the model returned
   * @return the assistant message to append
   */
  def said(completion: Model.Completion): Message.Assistant =
    Message.Assistant(completion.content, completion.calls)

  /**
   * What a tool answered, as the conversation carries it.
   *
   * The text is whatever the result renders to, a failure included — everything a model can react to
   * reaches it as words.
   *
   * @param outcome what dispatch produced
   * @return the tool message to append
   */
  def answered(outcome: Outcome): Message.ToolResult =
    Message.ToolResult(outcome.callId, Chunk(Message.Content.Text(outcome.result.render)))

  /**
   * One whole turn: what the model said, and an answer to every call it made.
   *
   * The protocol needs a result for each call id before the next model call, so the pair is built together
   * rather than appended separately. The order is the model's: its turn first, then the outcomes as
   * dispatch returned them.
   *
   * @param completion what the model returned
   * @param outcomes one outcome per call the completion asked for
   * @return the messages to append, in order
   */
  def turn(completion: Model.Completion, outcomes: Chunk[Outcome]): Chunk[Message] =
    said(completion) +: outcomes.map(answered)

}
