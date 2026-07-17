package homelab.incubator.common.data.v6

import homelab.incubator.common.data.v6.Batch.Lineage


case class Batch[+E, +A] private (private val lineage: Lineage, elements: Map[Int, Either[E, A]]) {

  def values: List[A] =
    elements.values.collect { case Right(value) => value }.toList

  def errors: List[E] =
    elements.values.collect { case Left(error) => error }.toList

  def map[B](fn: A => B): Batch[E, B] = Batch(
    lineage,
    elements.map { case (index, either) => index -> either.map(fn) },
  )

  def mapError[E2](fn: E => E2): Batch[E2, A] = Batch(
    lineage,
    elements.map { case (index, either) => index -> either.left.map(fn) },
  )

  def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch[E2, B] = Batch(
    lineage,
    elements.map { case (index, either) => index -> either.flatMap(fn) },
  )

  def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B] = Batch.Partial(
    lineage,
    elements.collect {
      case (index, Right(value)) if fn.isDefinedAt(value) => index -> Right(fn(value))
    },
  )

  def replaceWith[K, B, E2 >: E](others: List[B], defaultError: E2)(keyA: A => K, keyB: B => K): Batch[E2, B] = {
    val indexedB = others.map(b => keyB(b) -> b).toMap
    Batch(
      lineage,
      elements.collect:
        case index -> Left(err)    => index -> Left(err)
        case index -> Right(value) => index -> indexedB.get(keyA(value)).toRight(defaultError),
    )
  }

  def associatedWith[K, B, E2 >: E](others: List[B], defaultError: E2)(keyA: A => K, keyB: B => K): Batch[E2, (A, B)] = {
    val indexedB = others.map(b => keyB(b) -> b).toMap
    Batch(
      lineage,
      elements.collect:
        case index -> Left(err)    => index -> Left(err)
        case index -> Right(value) => index -> indexedB.get(keyA(value)).map(b => (value, b)).toRight(defaultError),
    )
  }

}


object Batch:
  final class Lineage

  case class Partial[+E, +A] private[Batch] (private val lineage: Lineage, elements: Map[Int, Either[E, A]]) {
    def values: List[A] =
      elements.values.collect { case Right(value) => value }.toList

    def errors: List[E] =
      elements.values.collect { case Left(error) => error }.toList

    def map[B](fn: A => B): Batch.Partial[E, B] = Batch.Partial(
      lineage,
      elements.map { case (index, either) => index -> either.map(fn) },
    )

    def mapError[E2](fn: E => E2): Batch.Partial[E2, A] = Batch.Partial(
      lineage,
      elements.map { case (index, either) => index -> either.left.map(fn) },
    )

    def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch.Partial[E2, B] = Batch.Partial(
      lineage,
      elements.map { case (index, either) => index -> either.flatMap(fn) },
    )

    def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B] = Batch.Partial(
      lineage,
      elements.collect {
        case (index, Right(value)) if fn.isDefinedAt(value) => index -> Right(fn(value))
      },
    )

    def replaceWith[K, B, E2 >: E](others: List[B], defaultError: E2)(keyA: A => K, keyB: B => K): Batch.Partial[E2, B] = {
      val indexedB = others.map(b => keyB(b) -> b).toMap
      Batch.Partial(
        lineage,
        elements.collect:
          case index -> Left(err)    => index -> Left(err)
          case index -> Right(value) => index -> indexedB.get(keyA(value)).toRight(defaultError),
      )
    }

    def associatedWith[K, B, E2 >: E](others: List[B], defaultError: E2)(keyA: A => K, keyB: B => K): Batch.Partial[E2, (A, B)] = {
      val indexedB = others.map(b => keyB(b) -> b).toMap
      Batch.Partial(
        lineage,
        elements.collect:
          case index -> Left(err)    => index -> Left(err)
          case index -> Right(value) => index -> indexedB.get(keyA(value)).map(b => (value, b)).toRight(defaultError),
      )
    }
  }
