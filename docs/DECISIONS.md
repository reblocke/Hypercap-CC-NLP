# Decisions

## 2026-05-30 - Public release branches exclude restricted/generated artifacts

Status: accepted

Context:
- The journal-submission repository needs a public release surface similar to the NRH-SCI-Vent release pattern.
- Earlier private development history included MIMIC-derived workbooks, draft manuscript artifacts, debug logs, generated outputs, and local metadata.
- Public reproducibility requires clear code/data boundaries because row-level MIMIC-derived artifacts cannot be redistributed.

Decision:
- Public release branches contain source code, Quarto notebooks, tests, specifications, documentation, and public classifier resources only.
- Public release branches must not track `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, `Legacy Code/`, local metadata, Office/PDF/image exports, generated Excel workbooks, release zips, or credentials.
- Generated manuscript assets may be attached outside git as reviewed GitHub/Zenodo release assets with checksums and a manifest.
- Legacy `.ipynb` compatibility notebooks and conversion helpers are retired from the public release surface.

Consequences:
- `Makefile` documents and exposes Quarto-first targets only.
- Static tests enforce public-release hygiene in addition to notebook self-containment.
- Private full-history backups must stay outside any repository that may later become public.

## 2026-04-18 - Canonical generated outputs live in `Results/YYYY-MM-DD`

Status: accepted

Context:
- The previous output contract split generated files across the repository root, `artifacts/reports/<run_id>/`, `annotation_agreement_outputs_nlp/`, `artifacts/Reyan Run Mar 31/`, and `Drafts/Apr 16 2026/`.
- That made reruns and cleanup harder because canonical outputs, QA sidecars, and working-draft files were intermixed.
- The project now needs a single canonical generated-output surface that leaves all of `Drafts/` untouched.

Decision:
- Generated notebook PDFs, figures, tables, HTML bundles, and prior root-level analysis exports now write to a flat `Results/YYYY-MM-DD/` directory.
- `RESULTS_DATE` is the canonical date selector for generated outputs, with `RESULTS_DIR=Results/<date>`.
- `artifacts/` is reserved for QA/debug/manifests only, including:
  - `artifacts/qa/cohort/`
  - `artifacts/qa/rater_agreement/`
  - `artifacts/qa/analysis/`
- `Drafts/` is manual-only working space and is no longer a canonical render target.
- Stale generated outputs outside `Drafts/` are archived under `Legacy Code/generated-output-archive/<date>/`, preserving original relative paths.

Consequences:
- `Makefile`, notebook stage contracts, and documentation should no longer describe `artifacts/reports/<run_id>/`, root-level “latest PDF” copies, `annotation_agreement_outputs_nlp/`, or `Drafts/Apr 16 2026/` as canonical render targets.
- Manual cleanup/migration tooling must avoid modifying any files under `Drafts/`.
- The 2026-04-17 merged-analysis decision remains in force for stage ownership, but its old output-location language is superseded by this decision.
- The live current-state contract now belongs in `docs/SPEC.md`; this file records the rationale and superseded location contracts.

## 2026-04-17 - Renderable Quarto notebooks must be self-contained

Status: accepted

Context:
- The main 4-stage pipeline and manuscript-facing analysis notebooks are the canonical execution surface for collaborators.
- Reproducibility requires that sharing a `.qmd` plus the declared input data is sufficient to recreate that stage without repo-local Python modules.

Decision:
- Renderable analysis notebooks must not rely on runtime imports from `src/` or other repo-local packages.
- Execution-critical helpers for cohort assembly, classifier logic, agreement analysis, and manuscript analysis must live in clearly labeled `Local helper functions` sections inside the corresponding `.qmd` files.
- `src/` remains available for QA, contracts, audits, parity checks, tests, and offline scripts that are not required at notebook render time.
- Small helper duplication across notebooks is acceptable when needed to preserve notebook self-containment.

Consequences:
- Notebook contract tests should fail on `src/` path injection or repo-local runtime imports in renderable notebooks.
- Tests for notebook-native helpers should execute code extracted from the `.qmd` source rather than importing analysis logic from `src/`.

## 2026-04-17 - Merge Reyan manuscript outputs into the Analysis notebook

Status: accepted

Context:
- `Hypercap CC NLP Analysis.qmd` and `Hypercap CC NLP Reyan Figures.qmd` had drifted into overlapping analysis/manuscript responsibilities.
- The manuscript draft already uses canonical numbering `Figure 1`-`Figure 5` and `Table 1`-`Table 2`, but the notebook/export contract still used legacy Reyan names such as `Figure 1_NEW.xlsx` and `Figure7_Underlying_Data.xlsx`.
- Maintaining two renderable notebooks for the same stage created duplicated setup/helpers and made output provenance harder to follow.

Decision:
- `Hypercap CC NLP Analysis.qmd` is now the single executable notebook for the combined analysis + manuscript-facing stage.
- At the time of this decision, manuscript-facing assets were written directly to `Drafts/Apr 16 2026/` on the canonical manuscript filenames:
  - `Figure 1.pdf`
  - `Figure 2-5.png/.xlsx`
  - `Table 1-2.pdf/.xlsx`
- At the time of this decision, retained non-manuscript outputs stayed under `artifacts/Reyan Run Mar 31/`, but used descriptive filenames rather than legacy figure numbers.
- `make quarto-reyan-figures` remains only as a compatibility alias and renders `Hypercap CC NLP Analysis.qmd` instead of a separate Reyan notebook.
- The output-location portion of this 2026-04-17 decision is superseded by the 2026-04-18 `Results/YYYY-MM-DD` contract and by `docs/SPEC.md`; the stage-ownership portion remains in force.

Consequences:
- Notebook/documentation/tests should no longer treat `Hypercap CC NLP Reyan Figures.qmd` as an active canonical stage.
- The merged Analysis notebook owns the manuscript-aware export registry, figure manifest, and manuscript asset verification.
- Legacy Reyan filenames such as `Figure 1_NEW.xlsx`, `Figure 2_NEW.xlsx`, `Figure7_Underlying_Data.xlsx`, and `timing_acidemia_summary.xlsx` are no longer the canonical manuscript output contract.

## 2026-04-17 - Main manuscript figure set is flow/workflow + 3 prevalence results figures

Status: accepted

Context:
- After the multi-label RFV refactor and estimand clarification, the manuscript's primary estimand is admission-level, non-mutually-exclusive prevalence rather than mutually exclusive composition.
- The previous merged notebook still treated acidemia timing as a fifth main figure and did not surface the analytic cohort flow in the main figure set.
- The manuscript narrative is cleaner when the main Results figures follow cohort yield/flow first, then primary and secondary prevalence comparisons.

Decision:
- The canonical main-manuscript figure contract is now:
  - `Figure 1.pdf` = combined two-panel analytic cohort construction plus chief complaint NLP workflow
  - `Figure 2.png/.xlsx` = grouped presenting category prevalence across ascertainment routes
  - `Figure 3.png/.xlsx` = grouped presenting category prevalence across age groups
  - `Figure 4.png/.xlsx` = grouped presenting category prevalence across acidemia severity bands
- Acidemia timing is demoted to supplement as `Figure S1.png/.xlsx`.
- RFV1-only stacked-bar sensitivity figures are supplement-only as `Figure S2-S5`.
- Overlap, time-to-gas, and recognition figures are supplement-only as `Figure S6-S8`.
- Primary multi-label prevalence figures share a common grouped-category order, a common x-axis scale, and the grouped label `Other grouped RFV categories`.

Consequences:
- At the time of this decision, `Drafts/Apr 16 2026/` should no longer contain canonical main-manuscript `Figure 5.*` assets after render.
- The draft manuscript should reference timing as `Supplementary Figure S1`, not as main `Figure 5`.
- The output-location portion of this consequence block is historical and superseded by the 2026-04-18 `Results/YYYY-MM-DD` contract and `docs/SPEC.md`.
