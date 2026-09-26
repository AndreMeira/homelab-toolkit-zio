package homelab.common.processing


import homelab.common.error.ApplicationError
import zio.Chunk


trait Node(val children: Chunk[Processor[ApplicationError, ?]]) {
  self: Processor[?, ?] =>
}
