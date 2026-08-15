package homelab.common.processing

import homelab.common.error.ApplicationError

trait Node(val children: List[Processor[ApplicationError, ?]]) {
  self: Processor[?, ?] =>
}
