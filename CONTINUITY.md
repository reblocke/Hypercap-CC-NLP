Goal (incl. success criteria):
- Create a one-time zip containing outputs from the most recent run.
- Success criteria:
  - dated zip exists in repo root,
  - zip contains canonical workbook plus run diagnostics/manifests/pdf from latest classifier run,
  - zip content listing verified.

Constraints/Assumptions:
- Core phase logic remains embedded in `.qmd` notebooks.
- No dependency/version changes.
- ABG/VBG/UNKNOWN inference stays metadata/specimen-driven only.
- Unknown-source definitive pCO2 remains cohort-eligible.

Key decisions:
- Timing tri-state export columns remain removed.
- `dt_qualifying_hypercapnia_hours` remains canonical timing field; `hypercap_timing_class` retained.
- `cc_missing_flag` and `cc_pseudomissing_flag` remain internal/diagnostic only and are removed from final classifier export.
- `cc_missing_reason` remains in final classifier export.
- Most recent run interpreted as latest classifier stage run (`classifier_20260223_075133`) and its immediately-following classifier contract check.

State:
- Done: validated candidate LAB/ICU pH/pO2 itemids from BigQuery dictionaries.
- Now: output bundle created and verified.
- Next: await user follow-up.

Done:
- Removed `qualifying_gas_observed`, `presenting_hypercapnia_tri`, `late_hypercapnia_tri` from cohort notebook timing derivation/export.
- Updated analysis notebook QC metric from `qualifying_gas_observed_rate` to `qualifying_gas_time_observed_rate` computed from `dt_qualifying_hypercapnia_hours`.
- Updated contract logic to remove tri-state consistency checks and related summary fields.
- Updated notebook/contract tests for removed timing tri-state references.
- Updated README timing policy text to reflect dt-driven timing contract.
- Verification complete:
  - `make lint` passed.
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py tests/test_workflow_contracts.py` passed (`44 passed`).
  - `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0 make quarto-cohort` passed.
  - `make quarto-classifier` passed.
  - `make quarto-analysis` passed.
  - `make contracts-check STAGE=cohort` passed with warning-only status (known warnings unchanged: missing exported gas-source diagnostic columns by design, low `first_hco3` coverage).
  - Canonical outputs:
    - cohort workbook no longer includes `qualifying_gas_observed`, `presenting_hypercapnia_tri`, `late_hypercapnia_tri`.
    - cohort workbook retains `dt_qualifying_hypercapnia_hours` and `hypercap_timing_class`.
- Implemented classifier missingness export narrowing:
  - `Hypercap CC NLP Classifier.qmd` now exports `df_export` with `cc_missing_flag` and `cc_pseudomissing_flag` removed from final workbook.
  - Notebook contract validation now runs on the exported dataframe.
  - Local notebook contract logic now requires `cc_missing_reason` and treats the two flag columns as optional diagnostics.
  - `src/hypercap_cc_nlp/classifier_quality.py` updated to the same contract behavior.
  - Tests updated (`tests/test_classifier_quality.py`, `tests/test_pipeline_contracts.py`, `tests/test_notebook_output_contracts.py`) for optional flag columns in exported classifier output.
  - `README.md` updated to document final export keeps `cc_missing_reason` and keeps flag columns in diagnostics/audits only.
- Verification complete (this task):
  - `make lint` passed.
  - `uv run pytest -q tests/test_classifier_quality.py tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py` passed (`42 passed`).
  - `make quarto-classifier` passed.
  - `make contracts-check STAGE=classifier` passed.
  - Canonical NLP workbook check:
    - `cc_missing_reason` present,
    - `cc_missing_flag` absent,
    - `cc_pseudomissing_flag` absent.
- Created output bundle for latest run:
  - zip: `outputs20260223_075423.zip`
  - includes canonical classifier workbook, classifier run manifest/audits, classifier render PDF, run-manifest git diff, and latest classifier contract report.
  - bundle staging files recorded under `debug/output_bundle_20260223_075423/`.

Now:
- Await user review.

Next:
- None.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- Files:
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMICIV_hypercap_EXT_cohort.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Hypercap CC NLP Analysis.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_notebook_output_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/classifier_quality.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_classifier_quality.py`
- Commands:
  - `make lint`
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py tests/test_workflow_contracts.py`
  - `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0 make quarto-cohort`
  - `make quarto-classifier`
  - `make quarto-analysis`
  - `make contracts-check STAGE=cohort`
  - `make quarto-classifier`
  - `make contracts-check STAGE=classifier`
