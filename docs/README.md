# homelab-toolkit-zio docs

Same shape as `cluster/docs/` and the Kyo `../../homelab-toolkit/docs/`, scoped to this library:

- **`architecture/`** — the *living* current-state of the toolkit: one page per module/concern, edited
  in place. State, not pedagogy.
- **`learning-material/`** — *how-it-works* explainers and hard-won gotchas about the ZIO stack.
- **`decisions/`** — numbered ADRs for choices worth their own rationale + revisit-trigger.
- **`sessions/`** — dated checkpoints: what was done, current state, what's next (transient; promote
  durable facts into `architecture/`).

Sub-folders are created when first used. Repo design rationale lives in
[`../../research/library-design/`](../../research/library-design/); the stack decision (ZIO until Kyo
matures) is in
[`../../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md`](../../homelab-toolkit/docs/decisions/0001-effect-system-zio-until-kyo-matures.md).
