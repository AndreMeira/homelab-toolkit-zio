package homelab.llm.anthropic.request

import zio.json.*


/**
 * What the API records about who a call is for.
 *
 * The id is opaque to the API and should stay opaque to anyone reading its logs: an account's own
 * identifier for a user, not an email or a name.
 *
 * @param userId who this is on behalf of, where a caller wants the API to know
 */
@jsonMemberNames(SnakeCase)
final case class Metadata(userId: Option[String] = None) derives JsonEncoder
