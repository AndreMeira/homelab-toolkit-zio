package homelab.incubator.common.store

import zio.IO


trait ValueStore[E, A]:
  def get(using k: ValueStore.Key[A])(key: k.Type): IO[E, A]
  def set(using key: ValueStore.Key[A])(value: A): IO[E, Unit]
  def delete(using k: ValueStore.Key[A])(key: k.Type): IO[E, A]


object ValueStore:
  trait Key[A]:
    type Type
    def get(value: A): Type
