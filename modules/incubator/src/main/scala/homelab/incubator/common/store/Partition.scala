package homelab.incubator.common.store

import homelab.common.store.KeyValueStore


object Partition {
  trait Key[A]:
    type Type
    def get(value: A): Type
}


trait Actor[I, O, S](using val key: Partition.Key[I]):
  def store: KeyValueStore[key.Type, S]
