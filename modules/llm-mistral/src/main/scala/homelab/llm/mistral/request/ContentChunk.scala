package homelab.llm.mistral.request


import homelab.llm.Message
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One piece of a message's content, in either direction.
 *
 * Two states: [[ContentChunk.Decoded]] is a chunk of a kind this module models, and [[ContentChunk.Raw]]
 * is one it does not, kept as it arrived. A raw chunk is written back exactly as it was read, so a chunk of
 * a kind added after this module still reaches the next turn, and a caller can send one before this module
 * names it.
 */
enum ContentChunk:

  /**
   * A chunk as it arrived, of a kind this module does not model.
   *
   * @param json the chunk
   */
  case Raw(json: Json)

  /**
   * A chunk read as one of the kinds this module names.
   *
   * @param kind what it turned out to be
   */
  case Decoded(kind: ContentChunk.Kind)


object ContentChunk:

  /**
   * Words, as a chunk.
   *
   * @param text what they are
   * @return the chunk
   */
  def text(text: String): ContentChunk = Decoded(Kind.Text(text))

  /**
   * A conversation's content, as a chunk.
   *
   * A part the toolkit holds raw goes out exactly as it is held, which is how a reasoning chunk read from
   * one answer reaches the next request unaltered.
   *
   * @param content what the toolkit holds
   * @return the chunk to send
   */
  def from(content: Message.Content): ContentChunk = content match
    case Message.Content.Text(said) => text(said)
    case Message.Content.Raw(json)  => Raw(json)

  /**
   * The kinds Mistral's schema names, told apart by the `type` every chunk carries.
   *
   * Which kinds a message may carry depends on its role and on the model, and is the API's to refuse: a
   * system message takes text and thinking only, and a tool message takes no thinking.
   */
  @jsonDiscriminator("type")
  enum Kind derives JsonCodec:

    /**
     * Words.
     *
     * @param text what they are
     */
    @jsonHint("text") case Text(text: String)

    /**
     * An image, by address or inline.
     *
     * @param imageUrl where it is: a URL, or the image itself as a `data:` URI
     */
    @jsonHint("image_url") @jsonMemberNames(SnakeCase) case ImageUrl(imageUrl: Image)

    /**
     * A document, by address or inline.
     *
     * @param documentUrl where it is: a URL, or the document itself as a `data:` URI
     * @param documentName what to call it, where the model is told
     */
    @jsonHint("document_url") @jsonMemberNames(SnakeCase) case DocumentUrl(
      documentUrl: String,
      documentName: Option[String] = None,
    )

    /**
     * A pointer into sources the model was given, which an answer carries to cite them.
     *
     * @param referenceIds which sources, by the ids they were given, each a number or a string
     */
    @jsonHint("reference") @jsonMemberNames(SnakeCase) case Reference(referenceIds: Chunk[Json])

    /**
     * A file uploaded to Mistral beforehand.
     *
     * @param fileId the id the upload was given
     */
    @jsonHint("file") @jsonMemberNames(SnakeCase) case File(fileId: String)

    /**
     * What a model reasoned before it answered.
     *
     * Mistral asks for it to be sent back on the following turn, as it came.
     *
     * @param thinking the reasoning, as chunks of its own: text, and references to tools or sources
     * @param signature what a model needs to recognise its own reasoning when it is replayed, where it sets one
     * @param closed whether the reasoning is finished, which matters only when it ends a prefix
     */
    @jsonHint("thinking") case Thinking(
      thinking: Chunk[ContentChunk],
      signature: Option[String] = None,
      closed: Option[Boolean] = None,
    )

    /**
     * Sound, inline.
     *
     * @param inputAudio the audio, base64-encoded
     */
    @jsonHint("input_audio") @jsonMemberNames(SnakeCase) case InputAudio(inputAudio: String)

    /**
     * A resource, given with its content.
     *
     * @param uri what the resource is called
     * @param content what it holds, as a text or image chunk
     */
    @jsonHint("resource") case Resource(uri: String, content: ContentChunk)

    /**
     * A resource, given by reference only.
     *
     * @param uri what the resource is called
     * @param metadata what is known about it, each value a string, number or flag
     */
    @jsonHint("resource_link") case ResourceLink(uri: String, metadata: Option[Map[String, Json]] = None)

  /**
   * Where an image is, and how closely to look at it.
   *
   * The wire takes either a bare string or an object here. This writes the object, which carries both
   * fields, and reads either.
   *
   * @param url a URL, or the image as a `data:` URI
   * @param detail how closely the model looks, where a caller says
   */
  final case class Image(url: String, detail: Option[Detail] = None)

  object Image:

    /**
     * The object form, as the derived codec writes and reads it.
     *
     * @param url the address
     * @param detail how closely to look
     */
    final private case class Addressed(url: String, detail: Option[Detail] = None) derives JsonCodec

    /** An image is always written as the object, which can say everything the string can. */
    given JsonEncoder[Image] = JsonEncoder[Addressed].contramap(image => Addressed(image.url, image.detail))

    /** An image is read from either form. */
    given JsonDecoder[Image] =
      JsonDecoder[String].map(Image(_)).orElse(JsonDecoder[Addressed].map(read => Image(read.url, read.detail)))

  /**
   * How closely the model looks at an image, which trades detail for what it costs.
   */
  enum Detail:
    case Low
    case Auto
    case High

  object Detail:

    /** A detail is written as the word the API uses. */
    given JsonEncoder[Detail] = JsonEncoder[String].contramap(encode)

    /** A detail is read from that word, and refused when it is none of the three. */
    given JsonDecoder[Detail] = JsonDecoder[String].mapOrFail(named)

    /**
     * The detail a word names.
     *
     * @param word what the API sent
     * @return the detail, or why the word names none
     */
    private def named(word: String): Either[String, Detail] =
      Detail.values.find(encode(_) == word).toRight(s"no image detail is called '$word'")

    /**
     * The word the API uses for a detail.
     *
     * @param detail the detail
     * @return its word
     */
    private def encode(detail: Detail): String = detail match
      case Low  => "low"
      case Auto => "auto"
      case High => "high"

  /** A chunk is written as its kind encodes, or exactly as it was read when it is raw. */
  given JsonEncoder[ContentChunk] = JsonEncoder[Json].contramap(json)

  /** A chunk is read as one of the named kinds where it is one, and kept raw where it is not. */
  given JsonDecoder[ContentChunk] = JsonDecoder[Json].map(transform)

  /**
   * One chunk, as JSON.
   *
   * The derived encoder answers an `Either`, and the left is unreachable: every kind is strings, numbers,
   * flags, JSON and nested chunks, and nothing about them can fail to be written.
   *
   * @param chunk the chunk
   * @return it, written
   */
  def json(chunk: ContentChunk): Json = chunk match
    case Raw(held)     => held
    case Decoded(kind) => kind.toJsonAST.getOrElse(Json.Null)

  /**
   * One chunk, from JSON.
   *
   * @param json the chunk as it arrived
   * @return it as a named kind, or kept whole when it is not one
   */
  private def transform(json: Json): ContentChunk = json.as[Kind].fold(_ => Raw(json), Decoded.apply)
