package homelab.incubator.processing.pool.v1

import zio.*


trait TaskPool[E, A, B]:
  def parallelism: Int
  def queue: Queue[(A, Promise[E, B])]
  def process(message: A): IO[E, B]

  def send(message: A): IO[E, B] =
    Promise.make[E, B].flatMap(promise => queue.offer(message -> promise) *> promise.await)

  protected def restartSchedule: Schedule[Any, Any, Any] =
    Schedule.exponential(100.millis).jittered && Schedule.recurs(5)

  def run: UIO[Nothing] =
    ZIO.foreachParDiscard(1 to parallelism) { _ =>
      queue.take
        .flatMap((msg, promise) => process(msg).retry(restartSchedule).onExit(promise.done))
        .ignore
        .forever
    } *> ZIO.never

  
  
  
