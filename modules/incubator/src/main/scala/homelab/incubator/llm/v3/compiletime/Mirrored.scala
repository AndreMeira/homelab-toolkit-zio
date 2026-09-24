package homelab.incubator.llm.v3.compiletime


import scala.compiletime.{ constValue, constValueTuple, erasedValue, summonInline }
import scala.deriving.Mirror


/**
 * What the compiler already knows about a type's shape, without any value existing.
 *
 * A second sketch, answering a different question from [[Describable]]: that one asks what a *macro* can
 * see, this one what plain `inline` code can. A `Mirror` carries a product's field labels and field types
 * as *types*, so a derivation can be written with no reflection and no runtime schema at all — which is how
 * most Scala 3 typeclass derivation works.
 */
object Mirrored {

  /**
   * The field names of a product, read from its mirror.
   *
   * @tparam A a product type
   * @return its labels, in declaration order
   */
  inline def labels[A](using mirror: Mirror.ProductOf[A]): List[String] =
    constValueTuple[mirror.MirroredElemLabels].toList.map(_.toString)

  /**
   * Whether a given is available here, decided while compiling.
   *
   * @tparam A the type to look for an instance of
   * @return true when one can be summoned
   */
  inline def hasInstance[A, T[_]]: Boolean =
    scala.compiletime.summonFrom {
      case _: T[A] => true
      case _       => false
    }
}
