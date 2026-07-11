package homelab.incubator.flow.v2


import homelab.common.data.Batch
import zio.*


trait Batcher[R, E, In, Out]:
  def run(in: In): ZIO[R, E, Out]


object Batcher:
  trait Logic[R, E, BE, In, Out]:
    def run(input: Batch.Success[In]): ZIO[R, E, Batch[BE, Out]]
