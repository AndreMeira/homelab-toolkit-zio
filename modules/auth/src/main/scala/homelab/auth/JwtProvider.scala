package homelab.auth


import homelab.common.error.ApplicationError
import homelab.common.types.SignedToken
import zio.*


trait JwtProvider:
  def get: IO[ApplicationError.AdapterError, SignedToken]
