package homelab.llm.mistral.request


import homelab.llm.Tool
import zio.Chunk
import zio.json.JsonEncoder

import scala.util.hashing.MurmurHash3
import scala.util.matching.Regex


opaque type CallId <: String = String


/**
 * A tool-call id, in the form Mistral takes: nine letters and digits.
 *
 * An id already in that form goes out as it is, which covers every id Mistral minted. Any other id — one
 * another provider minted — goes out as nine letters and digits derived from it, the same for the same id
 * every time, so a call and the tool message answering it still pair. The conversation keeps the id it
 * had; the rewrite exists only in the request.
 */
object CallId:

  /**
   * The id a call goes out under.
   *
   * @param id what the conversation holds
   * @return the id as it is when Mistral would take it, and one derived from it when Mistral would not
   */
  def from(id: Tool.Call.Id): CallId = id match
    case Normalized(id) => id
    case _              => normalize(id)

  /** An id is written as the string it is. */
  given JsonEncoder[CallId] = JsonEncoder[String]

  /** What Mistral takes. */
  private val Normalized: Regex = "([A-Za-z0-9]{9})".r

  /** How many characters an id has. */
  private val Length: Int = 9

  /** How many different characters each one may be. */
  private val Base: BigInt = BigInt(62)

  /** The two seeds whose hashes make up the 64 bits an id is derived from. */
  private val Seeds: (Int, Int) = (0x6d697374, 0x72616c00)

  /**
   * Nine letters and digits, derived from an id Mistral would refuse.
   *
   * @param id the id the conversation holds
   * @return the same nine characters for the same id
   */
  private def normalize(id: String): String =
    Chunk
      .iterate(hashed(id) mod Base.pow(Length), Length)(_ / Base)
      .map(rest => digit((rest mod Base).toInt))
      .mkString

  /**
   * An id's hash, as a number below 2⁶⁴.
   *
   * @param id the id
   * @return two 32-bit hashes of it, side by side
   */
  private def hashed(id: String): BigInt =
    (BigInt(Integer.toUnsignedLong(MurmurHash3.stringHash(id, Seeds._1))) << 32) +
      BigInt(Integer.toUnsignedLong(MurmurHash3.stringHash(id, Seeds._2)))

  /**
   * One character of an id.
   *
   * @param value a number from 0 to 61
   * @return a digit for the first ten, then an upper-case letter, then a lower-case one
   */
  private def digit(value: Int): Char =
    if value < 10 then ('0' + value).toChar
    else if value < 36 then ('A' + value - 10).toChar
    else ('a' + value - 36).toChar
