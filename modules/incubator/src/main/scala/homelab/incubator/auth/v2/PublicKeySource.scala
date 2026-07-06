package homelab.incubator.auth.v2


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*

import java.security.PublicKey


trait PublicKeySource {
  def get(keyId: String): IO[AdapterError, PublicKey]
}
