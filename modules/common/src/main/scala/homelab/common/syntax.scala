package homelab.common


import zio.*
import homelab.common.error.ApplicationError

import scala.util.Try


extension [R, E1, A](zio: ZIO[R, E1, Option[A]]) {

  /**
   * Require the `Option` this effect yields to be present, failing with `error` when it is empty.
   *
   * @param error the failure to raise when the effect yields `None`
   * @tparam Err the [[ApplicationError]] type raised on absence
   * @return the value, or the effect widened to fail with `E1 | Err`
   */
  def ?![Err <: ApplicationError](error: => Err): ZIO[R, E1 | Err, A] = zio.someOrFail(error)
}


extension [A](value: A) {

  /**
   * Lift this value into an always-succeeding effect.
   *
   * @return a `UIO` yielding this value
   */
  def succeed: UIO[A] = ZIO.succeed(value)
}


extension [A](option: Option[A])

  /**
   * Require this `Option` to be present, failing with `err` when it is empty.
   *
   * @param err the failure to raise when the option is `None`
   * @tparam Err the [[ApplicationError]] type raised on absence
   * @return the value in an effect, or a failure with `err`
   */
  def someOrFail[Err <: ApplicationError](err: => Err): IO[Err, A] = option match
    case Some(value) => ZIO.succeed(value)
    case None        => ZIO.fail(err)


extension [E, A](either: Either[E, A])

  /**
   * Take this `Either`'s `Right`, turning a `Left` into a failure via `fn`.
   *
   * @param fn maps the `Left` value to the [[ApplicationError]] to raise
   * @tparam Err the error type raised for a `Left`
   * @return the `Right` value in an effect, or a failure built from the `Left`
   */
  def rightOrFail[Err <: ApplicationError](fn: E => Err): IO[Err, A] = either match
    case Right(value) => ZIO.succeed(value)
    case Left(err)    => ZIO.fail(fn(err))


extension [A](trying: Try[A])

  /**
   * Take this `Try`'s success, turning its failure into an [[ApplicationError]] via `fn`.
   *
   * @param fn maps the caught `Throwable` to the error to raise
   * @tparam Err the error type raised on failure
   * @return the success value in an effect, or a failure built from the `Throwable`
   */
  def successOrFail[Err <: ApplicationError](fn: Throwable => Err): IO[Err, A] = trying match
    case scala.util.Success(value) => ZIO.succeed(value)
    case scala.util.Failure(err)   => ZIO.fail(fn(err))


/**
 * Retry `effect` under `schedule`, but only while it fails with a [[ApplicationError.TransientError]] — a
 * non-transient error fails fast, without consuming the schedule.
 *
 * @param schedule the retry policy applied to transient failures (e.g. `Schedule.recurs(3)`)
 * @param effect   the effect to run, re-attempted on a transient failure
 * @return the effect's result; fails once the schedule is exhausted or a non-transient error occurs
 */
def retryTransient[R, E <: ApplicationError, A](
  schedule: Schedule[R, Any, Any]
)(
  effect: => ZIO[R, E, A]
): ZIO[R, E, A] = effect.retry(schedule.whileInput[E] {
  case _: ApplicationError.TransientError => true
  case _                                  => false
})
