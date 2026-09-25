package homelab.llm.openai.response

import zio.json.*

/**
 * What the gateway answers when it will not serve a request.
 *
 * @param error the failure, of which only the message is read
 */
final case class FailureResponse(error: FailureResponse.Detail) derives JsonDecoder


object FailureResponse:

  /**
   * The gateway's own account of a failure.
   *
   * @param message what went wrong, in its words
   */
  final case class Detail(message: String) derives JsonDecoder
