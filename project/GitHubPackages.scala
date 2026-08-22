import sbt._

/**
 * Publishing this repo's modules to its own GitHub Packages Maven registry.
 *
 * Kept out of `build.sbt` so that file stays declarative, and in one place because the two halves are
 * asymmetric in a way that is easy to get wrong: **publishing** from CI needs no secret — Actions' built-in
 * `GITHUB_TOKEN` can write to its own repo's registry — while **consuming** needs a *classic* PAT with
 * `read:packages`, since GitHub Packages serves Maven only to authenticated callers, public repo or not.
 *
 * NOTE: this is Scala 2.12 sbt-DSL code (`import sbt._`, not `sbt.*`), like every file under `project/`.
 */
object GitHubPackages {

  /** This repo's registry — where `publish` writes and where consumers resolve from. */
  val registry: MavenRepository =
    "GitHub Packages" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

  /**
   * Credentials from the environment in CI (`GITHUB_ACTOR` / `GITHUB_TOKEN`) and from
   * `~/.sbt/1.0/credentials` on a laptop — sbt does not read that file unless a build asks it to. Empty
   * when neither exists, so a publish with no credentials fails on the 401 rather than on a missing file.
   *
   * The realm is fixed by GitHub. Get it wrong and sbt silently skips these, which surfaces as a 401 that
   * reads like a bad token.
   */
  def credentials: Seq[Credentials] =
    (sys.env.get("GITHUB_ACTOR"), sys.env.get("GITHUB_TOKEN")) match {
      case (Some(actor), Some(token)) =>
        Seq(Credentials("GitHub Package Registry", "maven.pkg.github.com", actor, token))
      case _ =>
        val local = Path.userHome / ".sbt" / "1.0" / "credentials"
        if (local.exists) Seq(Credentials(local)) else Nil
    }
}
