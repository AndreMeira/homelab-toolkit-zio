# homelab-toolkit-zio docs

How docs are organised — folders, the promotion rule, the required frontmatter — is defined once for the
whole homelab in [`../../DOCS.md`](../../DOCS.md). It is the authority; nothing here restates it.

What is specific to this repo:

- **Sub-folders in use:** `architecture/`, `learning-material/`, `research/`, `sessions/`. Others from the
  taxonomy are created when first needed — `decisions/` when a choice here earns its own ADR.
- **`architecture/` is one page per concern**, named after the package it describes (`messaging.md`,
  `processing.md`, `auth.md`), so a reader goes from a package to its page without a lookup.
- **[`learning-material/`](learning-material/) explains a technology**, not this toolkit — currently
  [`json-schema-as-scala.md`](learning-material/json-schema-as-scala.md) (what each case of the schema ADT
  renders to, and which JSON Schema features the subset leaves out),
  [`tool-calling-wire-format.md`](learning-material/tool-calling-wire-format.md) and the rest.
- **[`research/`](research/) holds this toolkit's design rationale** — drafts kept as written, not
  current-state. It moved here from the homelab-wide `research/library-design/` on 2026-08-22, unedited;
  promoting any of it into `architecture/` is a separate, deliberate pass.
