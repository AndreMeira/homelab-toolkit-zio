package homelab.llm.mistral.request

import zio.json.*


/**
 * Whether a request may be served from priority capacity or only from standard capacity.
 *
 * @see [[CompletionRequest]]
 */
enum ServiceTier {

  /** The API decides. */
  case Auto

  /** Standard capacity only. */
  case StandardOnly
}


object ServiceTier:

  /** A tier is written as the word the API uses. */
  given JsonEncoder[ServiceTier] = JsonEncoder[String].contramap(encode)

  /**
   * The word the API uses for a tier.
   *
   * @param tier the tier
   * @return its word
   */
  private def encode(tier: ServiceTier): String = tier match
    case Auto         => "auto"
    case StandardOnly => "standard_only"
