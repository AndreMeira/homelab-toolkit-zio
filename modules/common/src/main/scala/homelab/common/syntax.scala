package homelab.common


import zio.*
import homelab.common.error.{ ApplicationError, ValidationError }
import zio.prelude.Validation

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


/**
 * A validation in progress: it holds a value, or every problem found on the way to not having one.
 *
 * '''A value rather than an effect.''' Accumulation is what validation is for — a request with two problems
 * must report two — and an error channel cannot do it: `ZIO` short-circuits on the first failure by
 * construction. `Validation` is the applicative that combines failures instead, so validating two checks
 * that both fail yields both.
 *
 * Parameterised by [[ValidationError.InvalidInput]], the contract for a single constraint violation, so a
 * check written anywhere in the homelab composes with any other. A service supplies its own enum of what
 * can be wrong; the shape stays common.
 *
 * '''It stays a value until somebody collapses it.''' That is deliberate: a validator that failed on its
 * caller's behalf could never be composed with another validator, which is the whole point. Collapsing is
 * [[orFail]] or [[orFailWith]], and it belongs to the caller — a use case — rather than to the validators.
 */
type Validated[A] = Validation[ValidationError.InvalidInput, A]


extension [A](validated: Validated[A])

  /**
   * Leave the applicative: give the value, or fail with everything that was wrong.
   *
   * Fails with [[ValidationError]], which aggregates the problems and is a `DomainError`, so a caller that
   * has no error type of its own to map into can stop here.
   *
   * @return the validated value; fails with every problem found, together
   */
  def orFail: IO[ValidationError, A] =
    ZIO.fromEither(validated.toEither).mapError(ValidationError.apply)

  /**
   * Leave the applicative into a service's own error: give the value, or build a failure from everything
   * that was wrong.
   *
   * The moment a validation becomes a refusal, and it belongs to the *caller* rather than to the
   * validators, which have to stay composable. An extension rather than a function because it reads at the
   * call site as the last step of a validation (`validation.parse(request).orFailWith(InvalidRequest.apply)`)
   * instead of wrapping it.
   *
   * @param fn builds the failure from the problems found, in the order they were found
   * @tparam Err the error type raised when anything was wrong
   * @return the validated value; fails with `fn` applied to every problem found
   */
  def orFailWith[Err <: ApplicationError](fn: NonEmptyChunk[ValidationError.InvalidInput] => Err): IO[Err, A] =
    ZIO.fromEither(validated.toEither).mapError(fn)
