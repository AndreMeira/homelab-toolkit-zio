package homelab.incubator.processing.actor.v6

import zio.*

trait Runtime {
  def submit(task: Runtime.Task): UIO[Unit]
  def fork: UIO[Runtime]
}

object Runtime {
  trait Task:
    def run: UIO[Unit]
    def drop: UIO[Unit]
    
  case class Live(scope: Scope) extends Runtime {
    def submit(task: Task): UIO[Unit] = ???
    def fork: UIO[Runtime] = ???
  }
}
