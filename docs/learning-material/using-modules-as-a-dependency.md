---
title: "Depending on a toolkit module from your service"
type: learning-material
status: current
updated: 2026-08-22
tags: [sbt, dependencies, github-packages, publishing, modules, pat]
---

# Depending on a toolkit module from your service

This repo is a set of small libraries (`homelab-common`, `homelab-postgres`, …). This note is for the
**other side**: a ZIO service that wants to pull one of them in as an sbt dependency.

## What you can depend on

| Module | Artifact | Provides |
|--------|----------|----------|
| common | `homelab-common` | data, ports, `processing`, `messaging` (incl. the in-memory family) |
| postgres | `homelab-postgres` | persistence adapter — Magnum + Hikari + Flyway |
| telemetry | `homelab-telemetry` | the OTel implementation of the `Monitor` port |
| auth | `homelab-auth` | JWKS authenticator, hasher, EdDSA issuer |
| nats | `homelab-nats` | Core NATS + JetStream implementations of the messaging ports |

Coordinates: organization **`com.andremeira.homelab`**, cross-built for **Scala 3** (use `%%`, which appends
the `_3` suffix). Every adapter depends on `common`, so pulling one **transitively brings `homelab-common`**
and you rarely need both lines. The root aggregate and `incubator` are `publish / skip` — deliberately not
published.

## Released artifacts live in GitHub Packages

Publishing a **GitHub Release** on this repo triggers [`release.yml`](../../.github/workflows/release.yml),
which runs the suite and publishes the five modules to
`https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio`.

### Consumers need a token — yes, even though the repo is public

GitHub Packages serves Maven artifacts **only to authenticated callers**: *"You need an access token to
publish, install, and delete private, internal, and public packages."* Repo visibility does not change this;
the container registry (`ghcr.io`) is the only registry that allows anonymous reads. It also has to be a
**classic** PAT — *"GitHub Packages only supports authentication using a personal access token (classic)."*

So, once: create a classic PAT with the **`read:packages` scope and nothing else**. One token covers every
package on the account, so a single shared token serves the whole homelab.

Put it in `~/.sbt/1.0/credentials` (never in a repo):

```
realm=GitHub Package Registry
host=maven.pkg.github.com
user=AndreMeira
password=ghp_yourclassictoken
```

The realm string is fixed by GitHub. Get it wrong and sbt silently skips the credentials, which surfaces as
a 401 that reads like a bad token.

### In your service's `build.sbt`

```scala
resolvers += "homelab-toolkit-zio" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

libraryDependencies += "com.andremeira.homelab" %% "homelab-common" % "0.0.1"
```

### In your service's CI

Add the same PAT as an Actions secret (or an organization-level secret, which every repo inherits — the only
version with no per-service setup), and read credentials from the environment instead of the file:

```scala
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_ACTOR", ""),
  sys.env.getOrElse("GITHUB_TOKEN", ""),
)
```

```yaml
      - name: Test
        env:
          GITHUB_TOKEN: ${{ secrets.HOMELAB_PACKAGES_TOKEN }}
        run: sbt -batch -no-colors test
```

**Do not use the built-in `GITHUB_TOKEN` for this.** It can only read packages it has been granted access to,
one grant per package per consumer repo — five artifacts times every service, forever. The shared PAT avoids
that matrix entirely.

## Cutting a release

Draft a release in the GitHub UI, create the tag `v0.0.1` as part of it, publish — or, equivalently:

```bash
gh release create v0.0.1 --generate-notes
```

The workflow listens for `release: published`, **not** for a tag push: a release created in the UI creates
its tag internally and emits only the release webhook, so a tag-filtered workflow would never run. The
corollary is that a bare `git push origin v0.0.1` publishes nothing — it just moves a tag.

The version comes from the tag (`build.sbt` reads `RELEASE_VERSION`, which the workflow sets to `0.0.1`), so
a local build always says `0.0.1-SNAPSHOT` and cannot accidentally claim a release number.

**A published version is immutable** — GitHub Packages rejects a re-push of the same coordinates. A botched
release is fixed by tagging the next patch version, never by overwriting.

## The alternatives, and when they are still better

- **`publishLocal`** — the day-to-day loop while changing the toolkit and a service together. `sbt
  publishLocal` (or `sbt common/publishLocal`) writes to `~/.ivy2/local`, which is already on sbt's default
  resolver chain, so the service needs no resolver and no credentials. Re-run it after each toolkit change.
- **Git source dependency** — `ProjectRef(uri("https://github.com/AndreMeira/homelab-toolkit-zio.git#<ref>"),
  "common")` builds the toolkit from source alongside your service. No publishing, no token, and you can pin
  an exact commit — at the cost of building the toolkit in every consumer's build.
- **JitPack** — the only anonymous option, but it rewrites the coordinates to `com.github.AndreMeira.*`,
  drops the `com.andremeira.homelab` group, and needs its runner to offer a JDK this build can use.

## Things to know

- **`%%` vs `%`.** `%%` appends the Scala 3 binary suffix (`_3`); the toolkit builds with **Scala 3.8.3**, so
  your service needs a compatible Scala 3. Plain `%` is for Java libraries only.
- **You depend on the port, not the class.** Wire your code against `common`'s ports and inject the adapter —
  that is the whole point of the module split.
