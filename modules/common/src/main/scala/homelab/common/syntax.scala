package homelab.common


import zio.*
import homelab.common.error.ApplicationError

import scala.util.Try


extension [R, E1, A](zio: ZIO[R, E1, Option[A]]) {
  def ?![Err <: ApplicationError](error: => Err): ZIO[R, E1 | Err, A] = zio.someOrFail(error)
}


extension [A](value: A) {
  def succeed: UIO[A] = ZIO.succeed(value)
}


extension [A](option: Option[A])

  def someOrFail[Err <: ApplicationError](err: => Err): IO[Err, A] = option match
    case Some(value) => ZIO.succeed(value)
    case None        => ZIO.fail(err)


extension [E, A](either: Either[E, A])

  def rightOrFail[Err <: ApplicationError](fn: E => Err): IO[Err, A] = either match
    case Right(value) => ZIO.succeed(value)
    case Left(err)    => ZIO.fail(fn(err))


extension [A](trying: Try[A])

  def successOrFail[Err <: ApplicationError](fn: Throwable => Err): IO[Err, A] = trying match
    case scala.util.Success(value) => ZIO.succeed(value)
    case scala.util.Failure(err)   => ZIO.fail(fn(err))
