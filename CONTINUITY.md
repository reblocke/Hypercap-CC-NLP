Goal (incl. success criteria):
- Re-run the Quarto pipeline after interrupted execution and assess whether all agreed criteria are currently met.
- Success criteria:
  - pipeline executes end-to-end,
  - contract/parity checks are current and interpreted against thresholds,
  - required artifacts exist and match requested outputs,
  - final report marks met vs unmet criteria.

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

State:
- Done: notebook-embedded refactor and prior validations are in place.
- Now: finalize criterion-by-criterion assessment from latest rerun outputs.
- Next: hand off met vs unmet criteria and blocker details.

Done:
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
- Workbook sanity checks after rerun:
  - `hypercap_by_abg` sum = `11023` (non-zero)
  - `hypercap_by_bg == hypercap_by_abg | hypercap_by_vbg` mismatch count = `0`
  - `RFV1` missing count = `0`
  - `cc_missing_flag`, `cc_pseudomissing_flag`, `cc_missing_reason` present.

Now:
- Synthesize latest strict vs warn-mode rerun outcomes into final criteria status.

Next:
- If strict success is required, adjust or satisfy the OTHER-rate relative-reduction gate.

Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: root cause of intermittent `run_pipeline_audit.py` stall (observed at `make quarto-cohort` startup with empty stage log).

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
- Commands:
  - `make quarto-pipeline`
  - `uv run python scripts/run_contract_checks.py --mode fail --stage all`
  - `make quarto-parity-check BASELINE=latest`
  - `uv run python scripts/run_pipeline_audit.py --pipeline-mode quarto --baseline latest --strictness fail_on_key_anomalies` (best-effort)
