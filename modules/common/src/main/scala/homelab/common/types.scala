package homelab.common

import java.util.UUID


object types:
  opaque type UserId <: UUID        = UUID
  opaque type UserName <: String    = String
  opaque type ServiceName <: String = String
  opaque type SignedToken <: String = String


  object UserId:
    def apply(uuid: UUID): UserId = uuid


  object UserName:
    def apply(name: String): UserName = name


  object ServiceName:
    def apply(name: String): ServiceName = name


  object SignedToken:
    def apply(token: String): SignedToken = token
