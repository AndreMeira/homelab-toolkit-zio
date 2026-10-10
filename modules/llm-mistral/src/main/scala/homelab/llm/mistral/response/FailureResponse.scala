package homelab.llm.mistral.response


import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * What the API answers when it will not serve a request.
 *
 * Two shapes arrive. An error object carries a `message` that is either prose or a nested `{"detail":[…]}`,
 * with a `type` and a `code`; a body that failed validation can also come back as the bare `detail` list.
 * Every field is optional so that one decoder reads both.
 *
 * @param message what went wrong: prose, or an object holding the problems found
 * @param kind the API's category for it, which the wire spells `type`
 * @param code the API's identifier for it, a string of digits where one is given
 * @param detail the problems found, when the body is the bare list
 */
final case class FailureResponse(
  message: Option[Json] = None,
  @jsonField("type") kind: Option[String] = None,
  code: Option[Json] = None,
  detail: Option[Chunk[FailureResponse.Problem]] = None,
) derives JsonDecoder {

  /**
   * What the failure says, as one line a log can show.
   *
   * @return the message or the problems, followed by the code where there is one; absent when the body
   *         says neither
   */
  def explanation: Option[String] = said.map(coded)

  /**
   * The words of the failure, followed by its code where there is one.
   *
   * @param text what the failure says
   * @return the text, and the code in brackets after it
   */
  private def coded(text: String): String =
    code.flatMap(FailureResponse.identifier).fold(text)(id => s"$text (code $id)")

  /**
   * The words of the failure, from whichever field carries them.
   *
   * @return the prose message, or the problems from either place a list can sit; absent when there are none
   */
  private def said: Option[String] = message -> detail match
    case Some(Json.Str(text)) -> _   => Some(text)
    case Some(nested: Json.Obj) -> _ => nested.get("detail").flatMap(FailureResponse.problems)
    case _ -> Some(problems)         => Some(FailureResponse.listed(problems))
    case _                           => None
}


object FailureResponse:

  /**
   * One thing validation found wrong with the request.
   *
   * @param loc where it is, as a path of field names and indices from the top of the request
   * @param msg what is wrong with it
   */
  final case class Problem(loc: Chunk[Json], msg: String) derives JsonDecoder

  /**
   * The problems in a nested list, as one line.
   *
   * @param json the list, as the error object nests it
   * @return the problems, or absent when the list does not read as one
   */
  private def problems(json: Json): Option[String] =
    json.as[Chunk[Problem]].toOption.map(listed)

  /**
   * Problems as one line, each led by where it is.
   *
   * @param problems what validation found
   * @return them, separated
   */
  private def listed(problems: Chunk[Problem]): String =
    problems.map(problem => s"${path(problem.loc)}: ${problem.msg}").mkString("; ")

  /**
   * Where a problem is, as a dotted path.
   *
   * @param loc the path as the API gives it
   * @return the path, a field name or index per step
   */
  private def path(loc: Chunk[Json]): String = loc.flatMap(identifier).mkString(".")

  /**
   * One step of a path, or one code, as text.
   *
   * @param json a string or a number
   * @return its text, or absent for anything else
   */
  private def identifier(json: Json): Option[String] = json match
    case Json.Str(text)  => Some(text)
    case Json.Num(value) => Some(value.toString)
    case _               => None
