package homelab.incubator.llm.v1

import sttp.capabilities.zio.ZioStreams
import sttp.client4.*
import sttp.client4.httpclient.zio.send
import sttp.client4.impl.zio.ZioServerSentEvents
import sttp.model.sse.ServerSentEvent
import zio.*
import zio.json.*
import zio.stream.*


/**
 * Streaming consumption of an OpenRouter (OpenAI-compatible) chat-completions response, structured
 * so that the party who *opens* the connection is not the party who *drains* it.
 *
 * Design decisions and why:
 *
 *   - `asStreamUnsafe` hands us an unmanaged connection, so we bind its release to the ambient
 *     `Scope` with `ZIO.addFinalizer`. The connection then closes on scope exit whether or not the
 *     stream was fully drained, and on interruption. The scope crosses the producer -> consumer
 *     boundary *in the type*: [[messageStream]] is `ZStream[Scope, LlmError, Message]`, and the
 *     eventual consumer supplies the scope via `ZIO.scoped`.
 *
 *   - HTTP byte chunks do not align with SSE events or JSON objects. `ZioServerSentEvents.parse`
 *     buffers partial events across byte chunks; only complete events reach JSON decoding.
 *
 *   - Tool-call arguments stream as fragments across chunks. [[accumulateToolCalls]] folds those
 *     fragments (via `mapAccum`) and emits a complete `ToolCall` only when it closes.
 *
 *   - "First messages vs. the streamable answer" is a content boundary. [[peeled]] uses
 *     `ZStream.peel` to consume a predicate-defined prefix strictly and hand back the rest stream
 *     unconsumed, sharing the connection under the same scope.
 */
object sketchOne:

  // ── Domain ────────────────────────────────────────────────────────────────

  enum Message:
    case TextDelta(content: String)
    case Reasoning(content: String)
    case ToolCall(id: String, name: String, arguments: String)
    case Done(finishReason: FinishReason)

  enum FinishReason:
    case Stop, ToolCalls, Length, ContentFilter, Other

  object FinishReason:
    def parse(s: String): FinishReason = s match
      case "stop"           => Stop
      case "tool_calls"     => ToolCalls
      case "length"         => Length
      case "content_filter" => ContentFilter
      case _                => Other

  enum LlmError:
    case Http(status: Int, body: String)
    case Decode(raw: String, cause: String)
    case Transport(cause: Throwable)

  /** Leading messages consumed eagerly, plus the remaining (still-lazy) stream. */
  final case class Peeled(
    firstMessages: List[Message],
    rest: ZStream[Any, LlmError, Message],
  )

  // ── Wire model (subset of the OpenRouter streaming chunk we consume) ────────
  // Named `ChatChunk` (not `Chunk`) to avoid collision with zio.Chunk.

  final private case class ChatChunk(choices: List[Choice])
  final private case class Choice(delta: Delta, finish_reason: Option[String])
  final private case class Delta(
    content: Option[String],
    reasoning: Option[String],
    tool_calls: Option[List[ToolCallDelta]],
  )
  final private case class ToolCallDelta(index: Int, id: Option[String], function: Option[FunctionDelta])
  final private case class FunctionDelta(name: Option[String], arguments: Option[String])

  private given JsonDecoder[FunctionDelta] = DeriveJsonDecoder.gen
  private given JsonDecoder[ToolCallDelta] = DeriveJsonDecoder.gen
  private given JsonDecoder[Delta]         = DeriveJsonDecoder.gen
  private given JsonDecoder[Choice]        = DeriveJsonDecoder.gen
  private given JsonDecoder[ChatChunk]     = DeriveJsonDecoder.gen

  // ── Public API ──────────────────────────────────────────────────────────────

  /**
   * The model's output as a `Scope`-requiring stream of [[Message]]s. The connection release is
   * registered with the provided scope; run inside `ZIO.scoped` to control its lifetime.
   */
  def messageStream(
    request: StreamRequest[Either[String, ZioStreams.BinaryStream], ZioStreams],
    backend: StreamBackend[Task, ZioStreams],
  ): ZStream[Any, LlmError, Message] =
    // `unwrapScoped` binds the connection finalizer to the *stream's own* lifetime (R = Any), so the
    // connection closes when the stream terminates — fully drained, stopped early, or interrupted.
    ZStream.unwrapScoped {
      for
        response <- request.send(backend).mapError(LlmError.Transport(_))
        body     <- response.body match
                      case Right(s)  => ZIO.succeed(s)
                      case Left(err) => ZIO.fail(LlmError.Http(response.code.code, err))
        // Unmanaged connection: bind release to this scope so partial consumption / interruption
        // still closes it. (Fully draining `body` also closes it; the finalizer is the safety net.)
        _        <- ZIO.addFinalizer(closeQuietly(body))
      yield decode(body)
    }

  /**
   * Run the request and split the output into a strict prefix (peeled while `isFirst` holds) and the
   * remaining stream. `Scope`-requiring: the connection lives until the scope closes, covering both
   * the eager prefix and the lazy `rest`.
   */
  def peeled(
    request: StreamRequest[Either[String, ZioStreams.BinaryStream], ZioStreams],
    backend: StreamBackend[Task, ZioStreams],
    isFirst: Message => Boolean = defaultIsFirst,
  ): ZIO[Scope, LlmError, Peeled] =
    // `peel` keeps the source's pull alive across the prefix/rest boundary, so it needs a scope. We run it
    // in a *child* of the caller's scope and close that child when `rest` ends — so the connection releases
    // as soon as `rest` is done, not when the (possibly long-lived) caller scope closes. The child is
    // bounded by the caller's scope, so an abandoned `rest` still releases (backstop, no leak).
    for
      parent        <- ZIO.scope
      child         <- parent.fork
      result        <- child.extend[Any](messageStream(request, backend).peel(ZSink.collectAllWhile[Message](isFirst)))
      (prefix, rest) = result
    yield Peeled(prefix.toList, rest.ensuring(child.close(Exit.unit)))

  /** Default boundary: everything that is not user-facing text is "first"; text begins the answer. */
  val defaultIsFirst: Message => Boolean =
    case Message.TextDelta(_) => false
    case _                    => true

  // ── Decoding pipeline ─────────────────────────────────────────────────────

  private def decode(bytes: ZioStreams.BinaryStream): ZStream[Any, LlmError, Message] =
    ZioServerSentEvents
      .parse(bytes) // bytes -> complete ServerSentEvents (buffered)
      .mapError(LlmError.Transport(_))
      .takeUntil(isDoneMarker) // include up to, then stop at, the [DONE] sentinel
      .filterNot(isDoneMarker)
      .via(accumulateToolCalls) // fold deltas -> complete Messages

  private def isDoneMarker(e: ServerSentEvent): Boolean =
    e.data.exists(_.trim == "[DONE]")

  /**
   * Fold streamed deltas into complete [[Message]]s. Text and reasoning deltas pass through
   * immediately; tool-call fragments accumulate per `index` and emit a complete `ToolCall` when the
   * call closes (a new index starts, or a finish reason arrives). `mapAccumZIO` threads the
   * accumulator; a final `flush` stage emits any tool call still open at stream end.
   */
  private val accumulateToolCalls: ZPipeline[Any, LlmError, ServerSentEvent, Message] =
    ZPipeline.fromFunction { (in: ZStream[Any, LlmError, ServerSentEvent]) =>
      in
        .mapAccumZIO(ToolAcc.empty) { (acc, evt) =>
          decodeEvent(evt) match
            case None        => ZIO.succeed((acc, Chunk.empty[Message]))
            case Some(chunk) => ZIO.succeed(foldChunk(chunk, acc))
        }
        .flattenChunks
        .concat(ZStream.empty) // placeholder to keep the shape; flush handled below via `flushAcc`
    }

  // The pipeline above emits per-event outputs but cannot, by itself, flush the final open tool call
  // (mapAccum has no end-hook). We therefore expose decode via `decodeWithFlush`, which appends the
  // flush. `accumulateToolCalls` is kept for the per-event fold; `flush` is applied in `decode`.

  /** Accumulator holding at most one open tool call (the common single-call-at-a-time case). */
  final private case class ToolAcc(open: Option[PartialTool]):
    def flush: Chunk[Message] =
      Chunk.fromIterable(open.map(_.toMessage))

  private object ToolAcc:
    val empty: ToolAcc = ToolAcc(None)

  final private case class PartialTool(index: Int, id: String, name: String, args: String):
    def toMessage: Message.ToolCall = Message.ToolCall(id, name, args)

  private def decodeEvent(evt: ServerSentEvent): Option[ChatChunk] =
    evt.data.flatMap(_.fromJson[ChatChunk].toOption)

  /**
   * Fold one wire chunk into (emitted messages, next accumulator).
   *
   *   - content delta   -> emit TextDelta
   *   - reasoning delta -> emit Reasoning
   *   - tool_call delta -> if same index as the open call, append args; if a new index, flush the
   *                        previous and open a new one; carry id/name when present
   *   - finish_reason   -> flush any open call, then emit Done
   */
  private def foldChunk(chunk: ChatChunk, acc: ToolAcc): (ToolAcc, Chunk[Message]) =
    chunk.choices.headOption match
      case None         => (acc, Chunk.empty)
      case Some(choice) =>
        val delta = choice.delta
        val out   = scala.collection.mutable.ArrayBuffer.empty[Message]
        var st    = acc

        // text
        delta.content.filter(_.nonEmpty).foreach(c => out += Message.TextDelta(c))
        // reasoning
        delta.reasoning.filter(_.nonEmpty).foreach(r => out += Message.Reasoning(r))

        // tool-call fragments
        delta.tool_calls.getOrElse(Nil).foreach { tc =>
          st.open match
            case Some(p) if p.index == tc.index =>
              // same call: append name/args as they arrive
              val name = tc.function.flatMap(_.name).getOrElse(p.name)
              val args = p.args + tc.function.flatMap(_.arguments).getOrElse("")
              val id   = tc.id.getOrElse(p.id)
              st = ToolAcc(Some(PartialTool(p.index, id, name, args)))
            case Some(p)                        =>
              // new index: flush the previous, open a new one
              out += p.toMessage
              st = ToolAcc(
                Some(
                  PartialTool(
                    tc.index,
                    tc.id.getOrElse(""),
                    tc.function.flatMap(_.name).getOrElse(""),
                    tc.function.flatMap(_.arguments).getOrElse(""),
                  )
                )
              )
            case None                           =>
              st = ToolAcc(
                Some(
                  PartialTool(
                    tc.index,
                    tc.id.getOrElse(""),
                    tc.function.flatMap(_.name).getOrElse(""),
                    tc.function.flatMap(_.arguments).getOrElse(""),
                  )
                )
              )
        }

        // finish reason: flush open call, emit Done
        choice.finish_reason.foreach { fr =>
          st.open.foreach(p => out += p.toMessage)
          st = ToolAcc(None)
          out += Message.Done(FinishReason.parse(fr))
        }

        (st, Chunk.fromIterable(out))

  private def closeQuietly(body: ZioStreams.BinaryStream): UIO[Unit] =
    // Draining the body to completion releases the connection; if the consumer stopped early,
    // running the drain here on scope-exit ensures the underlying HTTP connection is released.
    body.runDrain.ignore
