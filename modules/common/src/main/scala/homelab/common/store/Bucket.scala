package homelab.common.store


import homelab.common.error.ApplicationError.AdapterError
import zio.*


trait Bucket[A]:
  def set(value: A): IO[AdapterError, A]
  def get: IO[AdapterError, Option[A]]
  def empty: IO[AdapterError, Boolean]
