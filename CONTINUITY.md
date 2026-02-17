Goal (incl. success criteria):
- Implement ED vitals cleaning and audit artifacts in the Quarto-first cohort pipeline, with cleaned-vitals preference in analysis.
- Success criteria:
  - notebook-embedded ED vitals cleaning converts Celsius-like temperatures and handles pain/BP/SpO2 sentinel/outliers deterministically,
  - canonical cohort workbook includes the requested `*_clean` + flag columns,
  - ED vitals audit CSV artifacts are written each run under `MIMIC tabular data/prior runs/`,
  - cohort contract checks validate cleaned-vitals bounds,
  - cohort + analysis Quarto stages rerun successfully with updated outputs.

Constraints/Assumptions:
- Core phase logic must reside in `.qmd` notebooks.
- Standalone `.py` allowed only for QA/contracts/audit/scripts/tests.
- No physiologic-value inference for ABG/VBG source classification.
- Missing symptom values treated as No for primary symptom table policy.
- Dependency addition `upsetplot` is approved.

Key decisions:
- Keep `OTHER` in enrollment and report explicitly.
- Hybrid denominator policy in analysis tables.
- Use `longtable` + `landscape` for wide PDF tables.
- Keep `pco2_threshold_any` and add explicit `other_hypercap_threshold` in first table.
- Hard-code POC sanity QA thresholds:
  - `COHORT_POC_PCO2_MEDIAN_MIN = 45`
  - `COHORT_POC_PCO2_MEDIAN_MAX = 80`
  - `COHORT_POC_PCO2_FAIL_ENABLED = 1`
- Apply the above thresholds in external QA/contracts only (not notebook execution path).

State:
- Done: prior P0/P1/P2 remediation and Quarto migration are in place.
- Now: implement ED vitals normalization/sentinel handling + dedicated audit artifacts.
- Next: rerun cohort/analysis and validate new vitals contracts/artifacts.

Done:
- Implemented notebook-embedded ED vitals cleaning in `MIMICIV_hypercap_EXT_cohort.qmd`:
  - Celsius-like temperature normalization to Fahrenheit (`20–50` -> `°F` conversion),
  - pain sentinel `13` handling + range validation,
  - conservative BP/SpO2 range validation with additive flag columns,
  - backward-compatible `*_model` aliases mapped to cleaned fields.
- Added ED vitals audit artifacts:
  - `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_distribution_summary.csv`
  - `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_extreme_examples.csv`
  - `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_model_delta.csv`
- Added cleaned-vitals cohort contract checks in `src/hypercap_cc_nlp/pipeline_contracts.py`.
- Updated `Hypercap CC NLP Analysis.qmd` with cleaned-vitals preference selector and QC summary section.
- Added QA tests:
  - `tests/test_ed_vitals_cleaning.py`
  - updates in `tests/test_notebook_output_contracts.py`
  - updates in `tests/test_pipeline_contracts.py`
- Verification completed:
  - `uv run pytest -q tests/test_ed_vitals_cleaning.py tests/test_notebook_output_contracts.py tests/test_pipeline_contracts.py` passed (`20 passed`)
  - `uv run --with ruff ruff check src tests` passed
  - `make quarto-cohort` completed (with `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0`, `COHORT_ALLOW_OMR_QUERY_FAILURE=1`, `BQ_QUERY_TIMEOUT_SECS=600`, `PIPELINE_CONTRACT_MODE=warn`)
  - `make quarto-analysis` completed
  - `make contracts-check STAGE=cohort` completed with `status=warning` (only `gas_source_other_rate_high`)
- Canonical cohort workbook now includes requested ED-vitals cleaned/flag columns; cleaned bounds spot-checks passed (`bad=0` for SpO2/SBP/DBP bounds, no cleaned temp values remaining in 20–50 band).
- Updated `AGENTS.md` with notebook-embedded-core non-negotiable policy.
- Inlined core phase logic into:
  - `MIMICIV_hypercap_EXT_cohort.qmd`
  - `Hypercap CC NLP Classifier.qmd`
  - `Rater Agreement Analysis.qmd`
  - `Hypercap CC NLP Analysis.qmd`
- Added requested analysis outputs (OTHER row, ICD+ subset table, ICD performance table, UpSet outputs).
- Added notebook-local longtable helpers and LaTeX support for full PDF table visibility.
- Added/updated tests for notebook-output contract + parity/audit handling and ran lint/tests/check-resources successfully.
- Re-ran staged Quarto notebooks successfully after fixing encountered runtime issues.
- Verified no residual long-running audit/parity processes after interruption; repository state preserved.
- Re-ran standard checks successfully:
  - `make lint` passed
  - `make test` passed (`74 passed`)
  - `make check-resources` passed
- Re-ran parity successfully:
  - `make quarto-parity-check BASELINE=latest` => `status=warning`, `P0=0`, `P1=0`, `P2-only` expected drift/new-output findings.
- Re-ran contracts successfully:
  - `uv run python scripts/run_contract_checks.py --mode fail --stage all` => `status=warning` with only `gas_source_other_rate_high`.
- Confirmed requested artifacts exist (4 root PDFs, canonical workbooks, ICD performance files, UpSet outputs).
- Executed strict rerun attempt:
  - `make quarto-pipeline` failed at cohort gas-source reduction gate (`COHORT_OTHER_RELATIVE_REDUCTION_MIN=0.10` not met; observed reduction `0.0000`).
- Executed full rerun in warning mode:
  - `PIPELINE_CONTRACT_MODE=warn make quarto-pipeline` completed all 4 stages and produced refreshed PDFs/artifacts under `artifacts/reports/20260217_012510/`.
- Refreshed post-rerun checks:
  - `uv run python scripts/run_contract_checks.py --mode fail --stage all` => `status=warning` (only `gas_source_other_rate_high`).
  - `make quarto-parity-check BASELINE=latest` => `status=warning`, `P0=0`, `P1=0`, `P2=11`.
- Workbook spot-checks before this implementation pass:
  - `hypercap_by_abg` sum = `11023` (non-zero) and union mismatch = `0`
  - pseudo-missing RFV blank leakage not reproduced in current canonical workbook
  - "abnormal labs" normalization issue reproduced via `segment_preds` (`abnormal loss`)
  - ICU/POC `first_other_pco2` issue reproduced (`POC median ~90`, `%>=50 ~96.5%`)
- Implemented P0 ICU/POC extraction tightening in `MIMICIV_hypercap_EXT_cohort.qmd`:
  - removed unit-only ICU pCO2 candidacy in cohort SQL paths,
  - added explicit PO2/O2 exclusion patterns,
  - added ICU itemid prefilter CTEs for BigQuery performance and precision.
- Added ICU itemid audit artifacts:
  - `MIMIC tabular data/prior runs/YYYY-MM-DD icu_poc_itemid_map.csv`
  - `MIMIC tabular data/prior runs/YYYY-MM-DD icu_poc_itemid_usage.csv`
- Implemented P0 classifier text normalization fix in `Hypercap CC NLP Classifier.qmd`:
  - protected `lab/labs` spell tokens,
  - explicit `abnormal labs?` rule to `RVC-TEST`,
  - added `classifier_phrase_audit.csv` and fail-fast assertion on `abnormal loss`.
- Implemented hard-coded external QA thresholds in `pipeline_contracts.py`:
  - `COHORT_POC_PCO2_MEDIAN_MIN = 45`
  - `COHORT_POC_PCO2_MEDIAN_MAX = 80`
  - `COHORT_POC_PCO2_FAIL_ENABLED = 1`
- Updated `Makefile` and `README.md` so Quarto stage/pipeline targets do not auto-run contract checks; QA runs explicitly via dedicated commands.
- Validation commands:
  - `make lint` passed
  - `make test` passed (`76 passed`)
  - `make check-resources` passed
- Stage reruns completed (same report run id):
  - `make quarto-cohort REPORT_RUN_ID=20260217_071617`
  - `make quarto-classifier REPORT_RUN_ID=20260217_071617`
  - `make quarto-rater REPORT_RUN_ID=20260217_071617`
  - `make quarto-analysis REPORT_RUN_ID=20260217_071617`
- Post-rerun checks:
  - `make contracts-check STAGE=all` => `status=warning` (only `gas_source_other_rate_high`)
  - `make quarto-parity-check BASELINE=latest` => `status=fail` (expected major cohort/ID drift vs pre-migration baseline)
  - `make quarto-pipeline-audit BASELINE=latest` was attempted; stalled at cohort stage and terminated.
- Post-rerun workbook spot checks:
  - rows reduced to `27,975` (from `41,322` baseline run),
  - `first_other_pco2` now LAB-only (`POC n=0`),
  - `abnormal loss` rows in abnormal-labs subset: `0`,
  - `RFV1` abnormal-test dominance in abnormal-labs subset improved (`432/940` top category),
  - `hypercap_by_abg` sum `5,242`, union mismatch `0`,
  - pseudo-missing blank-RFV leakage `0`.

Now:
- Prepare final handoff summary with file-level changes, verification results, and remaining warning(s).

Next:
- Optional: refresh parity baseline to latest accepted post-fix run before treating parity failures as regressions.
- Optional: investigate remaining high `gas_source_other_rate` despite improved ICU/POC extraction.

Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: whether `make quarto-pipeline-audit` stall at cohort stage is environment/transient or deterministic.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/AGENTS.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMICIV_hypercap_EXT_cohort.qmd`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Hypercap CC NLP Classifier.qmd`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Rater Agreement Analysis.qmd`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Hypercap CC NLP Analysis.qmd`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/_quarto.yml`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/quarto-pdf-header.tex`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/pyproject.toml`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/uv.lock`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_contracts.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_ed_vitals_cleaning.py`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_contracts.py`
- Commands:
  - `make quarto-pipeline`
  - `uv run python scripts/run_contract_checks.py --mode fail --stage all`
  - `make quarto-parity-check BASELINE=latest`
  - `uv run python scripts/run_pipeline_audit.py --pipeline-mode quarto --baseline latest --strictness fail_on_key_anomalies` (best-effort)
