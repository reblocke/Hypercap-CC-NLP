Goal (incl. success criteria):
- Implement residual QA/integrity remediation across rater agreement, anthropometric cleaning, POC pCO2 QC audit clarity, and `first_pco2` naming/export cleanup.
- Success criteria:
  - R3-vs-NLP binary category metrics are consistent with set-level outputs and fail fast on degenerate expansion,
  - anthropometric model columns enforce adult-strict plausibility bounds (BMI 10–100, height 100–230, weight 25–400),
  - pCO2 itemid QC audit explicitly distinguishes raw sentinel extremes from cleaned physiologic values,
  - `first_pco2` removed from final export and `qualifying_pco2_mmhg` is canonical.

Constraints/Assumptions:
- Core phase logic must remain in the 4 `.qmd` notebooks.
- `.py` changes limited to QA/contracts/tests/helpers.
- No dependency/version changes.
- Hypercapnia enrollment logic remains unchanged (ABG>=45, VBG/UNKNOWN>=50, LAB+POC, ED 0–24h).

Key decisions:
- Keep canonical union flag `pco2_threshold_0_24h`.
- Anthropometric cleaning policy is adult-strict with no override of valid recorded BMI.
- POC sentinel detection threshold for audit telemetry is raw pCO2 >= 100000 mmHg.
- Drop `first_pco2` from final export now; keep internal calculations as needed.

State:
- Done: prior full rerun + packaging complete (`outputs20260225_180924.zip`).
- Done: residual QA remediation implemented and validated end-to-end.
- Now: handoff/results summary for user.
- Next: optional output bundling for this new run if requested.

Done:
- Confirmed active issues in latest outputs:
  - degenerate R3-vs-NLP binary category table despite non-trivial set-level disagreement,
  - low-end anthropometric outliers still present in model columns,
  - pCO2 QC audit contains sentinel raw max (999998) without explicit removed/sentinel accounting,
  - `first_pco2` equals `qualifying_pco2_mmhg` for gas-positive rows and is naming-redundant.
- Implemented fixes:
  - `Rater Agreement Analysis.qmd`: normalized category indexing in binary expansion + fail-fast consistency assertions between set and binary metrics.
  - `MIMICIV_hypercap_EXT_cohort.qmd`: adult-strict anthro model bounds; BMI recorded-vs-computed audit diagnostics (no override); sentinel-aware raw/clean POC pCO2 QC fields; `first_pco2` dropped from final export; data dictionary expanded for qualifying pCO2 canonical fields.
  - `src/hypercap_cc_nlp/pipeline_contracts.py`: anthropometric model bounds aligned (BMI 10–100, height 100–230, weight 25–400).
  - Tests/docs updated in `tests/test_pipeline_contracts.py`, `tests/test_notebook_output_contracts.py`, and `README.md`.
- Validation completed:
  - `make lint` passed,
  - `make test` passed (105),
  - `make quarto-rater` passed,
  - `make quarto-cohort` passed,
  - `make contracts-check STAGE=all` passed warning-only,
  - `make quarto-pipeline` passed.
- Post-run acceptance checks:
  - R3-vs-NLP binary stats now non-degenerate (`nonzero_prevalence_categories=12`, `binary_disagreement_total=11`, `macro_cohen_kappa=0.6787`),
  - anthropometric model extremes removed (`weight<25 n=0`, `BMI<10 n=0`, `height>230 n=0`),
  - sentinel-aware pCO2 QC audit present (`raw_max_mmhg=999998`, `clean_max_mmhg=189`, sentinel removed counts populated),
  - `first_pco2` absent from final cohort export; `qualifying_pco2_mmhg` retained.

Now:
- Prepare concise handoff with changed files, checks, and residual warnings.

Next:
- Optional: create a new outputs zip for the 2026-02-26 run artifacts.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- Files:
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Rater Agreement Analysis.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMICIV_hypercap_EXT_cohort.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_notebook_output_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
- Commands:
  - `make lint`
  - `make test`
  - `make quarto-rater`
  - `make quarto-cohort`
  - `make contracts-check STAGE=all`
  - `make quarto-pipeline`
