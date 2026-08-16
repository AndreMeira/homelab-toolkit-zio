package homelab.common.processing


import homelab.common.error.ApplicationError
import zio.*


/**
 * The set of [[Processor]]s an application runs, and the one thing that starts them. A processor registered
 * here need not be started by whoever built it — which is the point: `run` is called once, at the top, and
 * nothing else in the app has to remember to fork anything.
 *
 * Registration is expected at construction time, typically inside the layer that builds the processor.
 */
trait Graph {

  /**
   * Register `processor` to be started by [[run]].
   *
   * Registration after [[run]] has begun is not picked up: the graph starts the set it has at that moment and
   * does not watch for more.
   *
   * @param processor the processor to start when the graph runs
   * @tparam A the value it consumes
   * @return noop once registered
   */
  def add[A](processor: Processor[ApplicationError, A]): UIO[Unit]

  /**
   * Start every registered processor and run until one of them fails.
   *
   * Processors are started in registration order and then run concurrently, each on its own fiber in the
   * calling scope. Because a [[Processor.run]] never completes on its own, the only way this returns is a
   * failure — so the first failure anywhere fails the graph, and closing the scope stops the rest. A graph
   * with nothing registered returns immediately.
   *
   * @return noop; aborts with the first failure any processor produces
   */
  def run: ZIO[Scope, ApplicationError, Unit]
}


object Graph {

  /**
   * Build a graph over `processors` and run it — the application's entry point, where the graph is created,
   * filled and started in one step because nothing else needs to hold it.
   *
   * Each processor brings its [[Node.children]] with it, and theirs in turn, so only the roots need naming
   * here. That recursion terminates without a visited set: a [[Node]] takes its children as constructor
   * arguments, so a cycle would need one of them to exist before itself. A child reached by two parents is
   * fine and starts once, since [[Graph.run]] collapses processors by identity.
   *
   * Children are added before their parent, so a processor's downstream is already running when it starts.
   *
   * @param processors the root processors; their descendants come along
   * @return noop; aborts with the first failure any processor produces
   */
  def run(processors: List[Processor[ApplicationError, ?]]): ZIO[Scope, ApplicationError, Unit] =
    for
      graph <- default
      _     <- ZIO.foreachDiscard(processors.flatMap(expand))(graph.add)
      _     <- graph.run
    yield ()

  /**
   * A processor and everything it owns, children first.
   *
   * @param processor the processor to expand
   * @return its descendants in start order, itself last
   */
  private def expand(processor: Processor[ApplicationError, ?]): List[Processor[ApplicationError, ?]] =
    processor match
      case node: Node => node.children.flatMap(expand) :+ processor
      case leaf       => List(leaf)

  /**
   * Build a processor and register it with the [[Graph]] in the environment — the form for a layer, where the
   * processor is constructed and handed over in one step and nobody is left holding something they must
   * remember to start.
   *
   * @param processor builds the processor to register
   * @tparam R the environment building it needs, beyond the graph
   * @tparam E the error building it may fail with
   * @tparam A the value it consumes
   * @return noop once built and registered; aborts with `E` if building fails
   */
  def register[R, E, A](processor: ZIO[R, E, Processor[ApplicationError, A]]): ZIO[Graph & R, E, Unit] =
    for
      graph <- ZIO.service[Graph]
      proc  <- processor
      _     <- graph.add(proc)
    yield ()

  /**
   * An empty graph. Nothing starts until [[Graph.run]] is called, so a graph can be built, passed around and
   * registered into during wiring without anything running early.
   *
   * @return the new, empty graph
   */
  def default: UIO[Graph] =
    Ref
      .make[List[Processor[ApplicationError, ?]]](List.empty)
      .map(processors => new Default(processors))

  /**
   * The only [[Graph]]: the registered processors in registration order, started in that order and then left
   * to race each other to the first failure.
   *
   * @param processors the registered processors, oldest first
   */
  private class Default(processors: Ref[List[Processor[ApplicationError, ?]]]) extends Graph {
    override def add[A](processor: Processor[ApplicationError, A]): UIO[Unit] =
      processors.update(_ :+ processor)

    override def run: ZIO[Scope, ApplicationError, Unit] =
      processors.get.flatMap { ordered =>
        // Forked one at a time, so a processor is running before the next one starts. That is start *order*,
        // not readiness: nothing here waits for a processor to be consuming before starting its neighbour.
        ZIO.foreach(ordered.distinctBy(_.key))(_.run.forkScoped).flatMap {
          case Nil           => ZIO.unit
          case first :: rest => ZIO.raceAll(first.join, rest.map(_.join))
        }
      }
  }
}
