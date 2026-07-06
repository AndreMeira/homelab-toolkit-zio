package homelab.auth


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.types.SignedToken
import zio.*

import java.nio.file.{ Files, Path }


/**
 * A [[JwtProvider]] that reads the pod's projected service-account token from the filesystem, fresh on
 * every call.
 *
 * Reading per-call — rather than once at startup — is deliberate: the kubelet rotates the projected token
 * in place (~hourly, atomically via a symlink swap, so reads never tear), and each read picks up the
 * current token. Reading is a blocking effect, so it can also surface a missing/unreadable file (e.g.
 * automount disabled, or a restrictive `defaultMode` under a non-root user) as an `AdapterError` instead
 * of assuming the credential is always present.
 *
 * @param tokenPath path to the projected token file (see [[ProjectedTokenProvider.DefaultTokenPath]])
 */
final class ProjectedTokenProvider(tokenPath: Path) extends JwtProvider:

  /**
   * Read the current service-account token.
   *
   * @return the token; fails with [[ProjectedTokenProvider.TokenUnavailable]] if the file is missing or can't be read
   */
  def get: IO[AdapterError, SignedToken] =
    ZIO
      .attemptBlocking(Files.readString(tokenPath).trim)
      .mapBoth(ProjectedTokenProvider.TokenUnavailable(tokenPath, _), SignedToken(_))


object ProjectedTokenProvider:

  /** The standard in-pod path of the projected service-account token. */
  val DefaultTokenPath: Path = Path.of("/var/run/secrets/kubernetes.io/serviceaccount/token")

  /**
   * A provider reading the standard in-pod projected token path ([[DefaultTokenPath]]).
   *
   * @return a provider bound to the default in-pod token file
   */
  def inPod: ProjectedTokenProvider = new ProjectedTokenProvider(DefaultTokenPath)


  /** The projected token file is missing or unreadable (e.g. automount disabled, or a restrictive mode). */
  final case class TokenUnavailable(path: Path, cause: Throwable) extends AdapterError:
    override def message: String = s"could not read the service-account token at $path: ${cause.getMessage}"
