# Decisions

## 2026-06-07 - Paired qualifying-gas pH audit precedes acidemia contract change

Status: accepted

Context:
- The manuscript acidemia figures are useful but can be misleading if pH is not paired to the same specimen or panel as the qualifying pCO2.
- Local early-gas review showed near-complete same-panel pH for within-24h qualifying-gas admissions, but the broad any-time gas cohort still needs a formal paired-pH completeness audit.
- Silently switching Figure 4 to paired qualifying-gas pH before that audit could drop an unknown fraction of broad gas-positive admissions.

Decision:
- The cohort notebook now exports paired qualifying-gas pH fields for the earliest qualifying pCO2 across the full any-time gas cohort.
- Pairing is based on admission, source branch, site, and panel time, preferring exact matches and then nearest matches within a narrow tolerance; pH availability does not determine the qualifying gas selection.
- The cohort stage writes an aggregate-only paired-pH completeness audit by broad gas-positive, 24h gas, 6h gas, late gas, ICD+gas, and ICD-only scopes.
- A 2026-06-06 private rerun before stricter ICU site-compatible pairing suggested near-complete broad paired-pH availability (11,520 of 11,521 broad gas-positive admissions). Those counts are pre-strict-site-pairing evidence only; the stricter contract requires a new private cohort rerun before using paired-pH completeness counts as current support for manuscript claims.
- Figure 4 now uses pH from the same specimen/panel as the earliest qualifying pCO2 when available; the source-priority pH rule remains only as a denominator/context audit.
- Figure 4 and Figure S1 source workbooks include denominator and missingness sheets.

Consequences:
- Manuscript wording should describe the paired qualifying-gas pH rule plainly and report denominator/missingness support for Figure 4 and Figure S1.
- Because the analysis notebook is configured to use paired qualifying-gas pH for Figure 4, it fails closed when `qualifying_ph` is absent from the private handoff workbook.
- Public outputs remain aggregate-only; paired pH fields are restricted row-level handoff fields and must not be committed.

## 2026-06-07 - Gas timing safeguard supports ED-triage chief complaint interpretation

Status: accepted

Context:
- Chief complaint text is measured at ED triage, while gas-based hypercapnia ascertainment can occur later in the admission.
- The cohort-stage contract intentionally keeps broad EHR ascertainment by ICD code or qualifying pCO2 after ED presentation through discharge.
- Reviewers may question whether chief complaint distributions describe presentation-time hypercapnia when the first qualifying gas occurs many hours after ED arrival.

Decision:
- The broad EHR-ascertained admission-level cohort remains the primary analysis cohort.
- `Hypercap CC NLP Analysis.qmd` elevates gas timing to an analysis-stage inferential safeguard by comparing RFV prevalence across the broad cohort, first qualifying gas within 24h, first qualifying gas within 6h, any ICD-positive admissions, and ICD-positive admissions with qualifying gas within 24h.
- The timing comparison is exported as an aggregate-only secondary workbook and summarized in the working manuscript draft as a main-text safeguard.
- The cohort-stage enrollment logic is not changed by this safeguard.

Consequences:
- Manuscript wording should distinguish broad EHR ascertainment from presentation-linked early-gas sensitivity cohorts.
- Static tests should enforce the timing-safeguard output contract and the 6h/24h cohort definitions without requiring private MIMIC data.

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
  - `Figure 1.pdf` = two-panel analytic cohort construction, mutually exclusive ascertainment strata, and chief complaint NLP workflow
  - `Figure 2.png/.xlsx` = grouped presenting category prevalence across overlapping ascertainment indicators
  - `Figure 3.png/.xlsx` = grouped presenting category prevalence across age groups
  - `Figure 4.png/.xlsx` = grouped presenting category prevalence across acidemia severity bands
- Acidemia timing is demoted to supplement as `Figure S1.png/.xlsx`.
- RFV1-only stacked-bar sensitivity figures are supplement-only as `Figure S2-S5`.
- The expanded ascertainment-overlap figure remains supplement-only as `Figure S6`, showing the full source-specific ABG/VBG/ICD/UNKNOWN-source gas overlap so indeterminate-source gas is visible without crowding the main Figure 1 workflow.
- Time-to-gas and recognition figures are supplement-only as `Figure S7-S8`.
- Primary multi-label prevalence figures share a common grouped-category order, a common x-axis scale, and the grouped label `Other grouped RFV categories`.

Consequences:
- At the time of this decision, `Drafts/Apr 16 2026/` should no longer contain canonical main-manuscript `Figure 5.*` assets after render.
- The draft manuscript should reference timing as `Supplementary Figure S1`, not as main `Figure 5`.
- The output-location portion of this consequence block is historical and superseded by the 2026-04-18 `Results/YYYY-MM-DD` contract and `docs/SPEC.md`.
