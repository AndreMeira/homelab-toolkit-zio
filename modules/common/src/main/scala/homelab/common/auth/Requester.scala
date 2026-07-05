package homelab.common.auth

import homelab.common.types.*

/**
 * The authenticated principal behind a request — a authenticate or a user — reconstructed from a token
 * (or its absence). Modelled as data so authorization is explicit at the type level: a handler asks
 * for the shape it needs ([[Requester.User.Authenticated]] where a login is required,
 * [[Requester.User.Anonymous]] tolerated where auth is optional).
 */
sealed trait Requester

object Requester:

  /** Another homelab authenticate calling this one (authenticate-to-authenticate), identified by its `ServiceName`. */
  case class Service(name: ServiceName) extends Requester

  /** A human caller — anonymous or authenticated. */
  sealed trait User extends Requester

  object User:
    /** No valid credentials presented. */
    case object Anonymous extends User

    /** A verified caller, carrying the identity claims lifted from the token. */
    case class Authenticated(userId: UserId, userName: UserName) extends User
