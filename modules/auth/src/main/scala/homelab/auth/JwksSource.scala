package homelab.auth


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*


trait JwksSource:
  def all: IO[AdapterError, JsonWebKey.Set]

  def get(keyId: String): IO[AdapterError, Option[JsonWebKey]] =
    all.map(set => set.keys.find(_.keyId == keyId))
