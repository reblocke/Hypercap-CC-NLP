Goal (incl. success criteria):
- Port the linked blog's figure-styling guidance into `AGENTS.md` as Python-first instructions.
- Success criteria:
  - `AGENTS.md` contains explicit publication-ready plotting guidance for Python (`matplotlib`-first).
  - R/`ggplot2`-specific style directives are not present in `AGENTS.md`.
  - Existing project constraints (Python-first, reproducibility, deterministic runs) remain intact.
  - Repo standard checks run successfully after docs edit.

Constraints/Assumptions:
- Scope is limited to `AGENTS.md` (+ ledger updates).
- No dependency changes.
- No scientific-method changes.
- Source URL is currently unreachable from shell (`curl` DNS failure), so mapping is best-effort from standard publication-figure practices.

Key decisions:
- Keep Python-first project policy unchanged.
- Expand the `Visualization` section with a concrete Python style checklist:
  - typography, sizing, line widths, color/contrast, axes/ticks, legends, multi-panel consistency, and export formats.
- Add deterministic plotting defaults (explicit size, rcParams policy, explicit save settings).
- Preserve Quarto usage guidance as tooling-neutral (not R-specific styling).

State:
- Done: previous notebook/pipeline stabilization work exists in working tree.
- Now: styling port and verification completed.
- Next: await user follow-up on any refinements.

Done:
- Re-read current `AGENTS.md` and identified visualization section to expand.
- Re-attempted source fetch:
  - `curl -sSL https://jaquent.github.io/2026/02/creating-actually-publication-ready-figures-for-journals-using-ggplot2/`
  - Result: `Could not resolve host` (still blocked).
- Updated `AGENTS.md` visualization guidance to a full Python publication-figure standard:
  - typography, sizing, color/accessibility, axes/scales integrity, legends/multi-panel layout, export/reproducibility defaults.
- Removed R-specific style wording from `AGENTS.md` (`R`, `ggplot2`, `Rscript` no longer present).
- Ran repo checks successfully:
  - `make lint` → pass.
  - `make test` → pass (`33 passed`).

Now:
- None.

Next:
- If needed, tune the visualization checklist for a specific journal style guide.

Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: exact one-to-one mapping fidelity to the unavailable blog page; content will follow publication-standard Python equivalents.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/CONTINUITY.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/AGENTS.md`
- Commands:
  - `curl -sSL "https://jaquent.github.io/2026/02/creating-actually-publication-ready-figures-for-journals-using-ggplot2/"`
  - `rg -n "\\bR\\b|ggplot2|Rscript" AGENTS.md`
  - `make lint`
  - `make test`
