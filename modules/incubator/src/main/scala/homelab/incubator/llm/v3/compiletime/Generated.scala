package homelab.incubator.llm.v3.compiletime


import homelab.incubator.llm.v4.schema.{ Node, Shape }

import scala.compiletime.{ constValue, erasedValue, summonInline }
import scala.deriving.Mirror


/** EXPERIMENT: describability as an implicit search, so an undescribable type has no instance at all. */
trait Generated[A] {
  def node: Node
  def required: Boolean = true
}


object Generated {

  given Generated[String]  = new Generated[String]  { def node: Node = Node.text    }
  given Generated[Int]     = new Generated[Int]     { def node: Node = Node.integer }
  given Generated[Long]    = new Generated[Long]    { def node: Node = Node.integer }
  given Generated[Double]  = new Generated[Double]  { def node: Node = Node.number  }
  given Generated[Boolean] = new Generated[Boolean] { def node: Node = Node.boolean }

  given list[A](using inner: Generated[A]): Generated[List[A]] = new Generated[List[A]] {
    def node: Node = Node.array(inner.node)
  }

  given option[A](using inner: Generated[A]): Generated[Option[A]] = new Generated[Option[A]] {
    def node: Node                 = Node.nullable(inner.node)
    override def required: Boolean = false
  }

  inline given derived[A](using mirror: Mirror.ProductOf[A]): Generated[A] =
    val built = Node.obj(fields(labels[mirror.MirroredElemLabels], instances[mirror.MirroredElemTypes])*)
    new Generated[A] { def node: Node = built }

  private inline def labels[T <: Tuple]: List[String] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (h *: t)   => constValue[h].toString :: labels[t]

  private inline def instances[T <: Tuple]: List[Generated[?]] = inline erasedValue[T] match
    case _: EmptyTuple => Nil
    case _: (h *: t)   => summonInline[Generated[h]] :: instances[t]

  private def fields(names: List[String], generated: List[Generated[?]]): List[Shape.Obj.Field] =
    names.zip(generated).map(toField)

  private def toField(pair: (String, Generated[?])): Shape.Obj.Field =
    Shape.Obj.Field(pair._1, pair._2.node, pair._2.required)
}
