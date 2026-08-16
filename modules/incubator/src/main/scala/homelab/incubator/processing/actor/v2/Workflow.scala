package homelab.incubator.processing.actor.v2


import homelab.common.error.ApplicationError.{ AdapterError, DecodingError }
import homelab.common.flow.Loop.Next.*
import homelab.common.flow.{ KeyLock, Loop }
import homelab.common.store.KeyValueStore
import zio.*


trait Workflow[-R, +E, S, +O]:

  def name: String

  /**
   * The transition: given the current state, continue with a new state or finish with the output.
   * Implemented as a case-lambda over `S`.
   *
   * @return the transition function; its effect continues with a new `S` or finishes with an `O`, and
   *         fails with `E` if the step fails
   */
  def next: S => ZIO[R, E, Loop.Next[S, O]]

  /**
   * Run the stepper in memory from `initial` until [[next]] finishes, without persistence. Stack-safe (it
   * delegates to [[Loop]]); the durable, resumable runner is [[Workflow.run]].
   *
   * @param initial the starting state
   * @return the output produced when the stepper finishes; fails with `E` if any step fails
   */
  def run(initial: S): ZIO[R, E, O] = Loop(initial)(next)


object Workflow:
  trait Id[A]:
    def encode(value: A): String

  object Id:
    def apply[A](using id: Id[A]): Id[A] = id

  trait Serde[A]:
    def encode(value: A): String
    def decode(encoded: String): Either[String, A]

  object Serde:
    def apply[A](using serde: Serde[A]): Serde[A]          = serde
    def error(cause: String): DecodingError & AdapterError =
      new DecodingError with AdapterError:
        override def message: String = cause

  trait Runner[Err]:
    def run[R, E, I: Id, S: Serde, O](
      workflow: Workflow[R, E, S, O],
      input: I,
    )(
      init: I => S
    ): ZIO[Scope & R, E | Err, O]

  object Runner:

    trait Inline extends Runner[Nothing]:
      def run[R, E, I, S, O](workflow: Workflow[R, E, S, O], input: I)(init: I => S): ZIO[R, E, O] =
        workflow.run(init(input))

    trait Default extends Runner[AdapterError]:
      def store: KeyValueStore[(String, String), String]
      def lock: KeyLock[(String, String)]

      override def run[R, E, I: Id, S: Serde, O](
        workflow: Workflow[R, E, S, O],
        input: I,
      )(
        init: I => S
      ): ZIO[Scope & R, E | AdapterError, O] = {
        val key = workflow.name -> Id[I].encode(input)
        lock.withPermit(key):
          for
            found  <- store.get(key)
            decoded = found.map(encoded => Serde[S].decode(encoded).left.map(Serde.error))
            state  <- ZIO.fromOption(decoded).flatMap(ZIO.fromEither(_)).orElseSucceed(init(input))
            result <- workflow.next(state)
            _      <- result match
                        case Continue(nextState) => store.set(key, Serde[S].encode(nextState))
                        case Done(outcome)       => store.delete(key)
            out    <- result match
                        case Done(outcome)       => ZIO.succeed(outcome)
                        case Continue(nextState) => run(workflow, input)(init)
          yield out
      }
