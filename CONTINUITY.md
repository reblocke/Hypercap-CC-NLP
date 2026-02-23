Goal (incl. success criteria):
- Resolve naming confusion where `pco2_threshold_any` duplicates `pco2_threshold_0_24h`.
- Success criteria:
  - canonical pipeline uses `pco2_threshold_0_24h` as the analysis-facing union flag,
  - `pco2_threshold_any` is not used by core notebooks/exports,
  - contracts/tests pass with compatibility fallback only in QA.

Constraints/Assumptions:
- Core phase logic remains embedded in `.qmd` notebooks.
- No dependency/version changes.
- ABG/VBG/UNKNOWN inference stays metadata/specimen-driven only.

Key decisions:
- Keep backward compatibility in QA contracts: accept legacy `pco2_threshold_any` with deprecation warning.
- Canonical notebook/analysis/classifier usage is `pco2_threshold_0_24h` only.

State:
- Done: naming cleanup implemented and validated with lint + targeted tests.
- Now: awaiting user’s next task (full pipeline rerun not executed in this turn).
- Next: if requested, run `make quarto-pipeline` and contracts to validate runtime outputs.

Done:
- Updated `Hypercap CC NLP Analysis.qmd` to replace `pco2_threshold_any` with `pco2_threshold_0_24h` in criteria, prevalence labels, inclusion typing, overlap logic, and ICD performance targets.
- Updated `Hypercap CC NLP Classifier.qmd` hypercap flag audit to use `pco2_threshold_0_24h`.
- Updated `src/hypercap_cc_nlp/pipeline_contracts.py`:
  - union threshold check now prefers `pco2_threshold_0_24h`,
  - legacy alias `pco2_threshold_any` accepted with warning code `pco2_threshold_any_deprecated_alias`,
  - hard error if neither union column is present.
- Updated `tests/test_pipeline_contracts.py` fixtures/assertions to canonical column and added deprecation-alias warning test.
- Updated `current_columns.json` to remove `pco2_threshold_any`.
- Validation commands run successfully:
  - `make lint`
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py` (37 passed)

Now:
- No active implementation in progress.

Next:
- Optional runtime verification: `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0 make quarto-pipeline` then `make contracts-check STAGE=all`.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- Files:
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Hypercap CC NLP Analysis.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/Hypercap CC NLP Classifier.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/current_columns.json`
- Commands:
  - `make lint`
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py`
