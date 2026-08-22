# homelab-toolkit-zio docs

How docs are organised — folders, the promotion rule, the required frontmatter — is defined once for the
whole homelab in [`../../DOCS.md`](../../DOCS.md). It is the authority; nothing here restates it.

What is specific to this repo:

- **Sub-folders in use:** `architecture/`, `learning-material/`, `sessions/`. Others from the taxonomy are
  created when first needed — `decisions/` when a choice here earns its own ADR.
- **`architecture/` is one page per concern**, named after the package it describes (`messaging.md`,
  `processing.md`, `auth.md`), so a reader goes from a package to its page without a lookup.
- **Design rationale lives outside this repo**, in `research/library-design/` — it predates and outlives
  individual modules. When a "how it works now" crystallises there, it gets promoted into `architecture/`.
