Goal (incl. success criteria):
- Implement residual hardening ticket items 2A/2B/2D/2E after the any-time-gas refactor.
- Success criteria:
  - 2A: rendered docs no longer claim enrollment is limited to 24h; 24h is only a timing marker.
  - 2B: gas-source diagnostics are exported as a keyed artifact and contracts validate artifact presence/shape (not main workbook columns).
  - 2D: `hco3_band` is null whenever `first_hco3_qc_flag` is false, with contract enforcement.
  - 2E: POC pCO2 QC uses broad plausibility + contamination heuristics so plausible itemids pass while clear contamination fails.

Constraints/Assumptions:
- Core phase logic remains in the 4 `.qmd` notebooks.
- `.py` changes are limited to QA/contracts/tests/audit helpers.
- No dependency/version changes.
- Gas thresholds unchanged: ABG >=45; VBG/UNKNOWN >=50.

Key decisions:
- Qualification route for gas = any-time stay (`pco2_threshold_any`), not 0–24h.
- Keep `dt_first_qualifying_gas_hours` as ED-anchored elapsed time to earliest qualifying gas (can exceed 24h).
- Reuse `pco2_threshold_0_24h` as marker `dt_first_qualifying_gas_hours <= 24`.
- Prefer ED anchor (`ed_intime_first` / `ed_intime`); allow documented fallback to `admittime` when ED anchor missing.
- For 2B, keep main export lean; place gas-source diagnostic rates in separate artifact keyed by `ed_stay_id`.
- For 2D, do not add extra cohort columns; QC-aware banding only.
- For 2E, implement preferred recalibration (broad plausibility, PO2 contamination heuristic, sentinel/out-of-range tiered thresholds).

State:
- Done: baseline any-time gas refactor and full pipeline run completed.
- Done: mapped code locations for new ticket items 2A/2B/2D/2E.
- Done: implemented 2A/2B/2D/2E in cohort notebook, contract checks, tests, and README.
- Done: validation checks passed (`make lint`, `make test`, targeted pytest, `make quarto-cohort`, `make contracts-check STAGE=all`).
- Now: prepare user handoff summary.
- Next: optional full `make quarto-pipeline` rerun if requested.

Done:
- 2A docs alignment:
  - Cohort notebook goal text now states enrollment is any-time definitive blood-gas hypercapnia with a separate ≤24h marker.
- 2B preferred fix (separate diagnostics artifact):
  - Cohort now writes `artifacts/gas_source_diagnostics_by_ed_stay.csv` (keyed by `ed_stay_id`, includes `hadm_id` + required gas-source fields).
  - Contract no longer expects gas-source diagnostic columns in the main workbook.
  - Contract now validates artifact existence, key uniqueness, row-count alignment, required columns, and rate bounds.
- 2D HCO3 band QC gate:
  - `hco3_band` assignment now requires `first_hco3_qc_flag == True`.
  - Added notebook fail-fast + contract error `hco3_band_qc_inconsistency`.
- 2E POC pCO2 QC recalibration:
  - Replaced narrow median fail with broad plausibility checks (p05/p50/p95), PO2 contamination heuristic, and out-of-range-rate thresholds.
  - Sentinel removals are warning-only unless broader range failures occur.
  - Audit now includes `clean_p05_mmhg`, `clean_p25_mmhg`, `distribution_plausible`, `possible_po2_contamination`, and `insufficient_data_flag`.
- Test/docs updates:
  - Updated `tests/test_pipeline_contracts.py` and `tests/test_notebook_output_contracts.py` for new artifact and HCO3 QC invariants.
  - Updated `README.md` to document artifact and revised POC QC semantics.
- Cohort logic changes in `MIMICIV_hypercap_EXT_cohort.qmd`:
  - `co2_thresholds_sql` now qualifies ABG/VBG/UNKNOWN hypercapnia any-time in stay (`pco2_threshold_any=1`).
  - `pco2_threshold_0_24h` is now computed marker `dt_qualifying_hypercapnia_hours <= 24`.
  - Enrollment now uses ICD OR `pco2_threshold_any`.
  - `dt_first_qualifying_gas_hours` updated to any-time semantics; negative dt values are nulled.
  - `presenting_hypercapnia` / `late_hypercapnia` generation removed; `hypercap_timing_class` now uses `within_24h|after_24h|icd_only_or_no_qualifying_gas`.
  - Added run-log telemetry for anchor fallback usage (`qualifying_anchor_fallback_n`).
- Analysis updates in `Hypercap CC NLP Analysis.qmd`:
  - Hypercap criteria now include `pco2_threshold_any` (gas-positive definition) and keep `pco2_threshold_0_24h` as marker.
  - Inclusion/overlap/performance tables now key gas-positive logic off `pco2_threshold_any`.
- Classifier audit alignment in `Hypercap CC NLP Classifier.qmd`:
  - Hypercap union mismatch audit now compares ABG/VBG/UNKNOWN union to `pco2_threshold_any`.
- Contracts/tests/docs:
  - `src/hypercap_cc_nlp/pipeline_contracts.py` now enforces:
    - `pco2_threshold_0_24h <= pco2_threshold_any`,
    - dt-marker consistency (`pco2_threshold_0_24h == int(dt<=24)` when gas-positive dt is present),
    - updated timing class values.
  - Tests updated in `tests/test_pipeline_contracts.py` and `tests/test_notebook_output_contracts.py`.
  - README updated for any-time qualification + within-24h marker semantics.
- Validation outcomes:
  - `make lint`: pass.
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py`: pass (`42 passed`).
  - `make test`: pass (`108 passed`).
  - `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0 make quarto-cohort`: pass (rendered PDF and updated outputs).
  - `make contracts-check STAGE=all`: pass (no findings).
- Latest contract snapshot (`debug/contracts/20260226_222124/contract_report.json`):
  - cohort rows: `11,945`,
  - `pco2_threshold_any_n=11,521`,
  - `pco2_threshold_0_24h_n=6,230`,
  - dt distribution confirms both ≤24h and >24h groups.
- Output packaging:
  - Created `outputs20260226_152416.zip` with PDFs, canonical workbooks, analysis exports, `annotation_agreement_outputs_nlp/`, dated `prior runs` artifacts, contract reports, and run-manifest diffs.
  - Archive integrity check passed (`unzip -t` no errors).

Now:
- Summarize implementation and verification for user.

Next:
- Await next instruction.

Open questions (UNCONFIRMED if needed):
- None.

Working set (files/ids/commands):
- Files:
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMICIV_hypercap_EXT_cohort.qmd`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/src/hypercap_cc_nlp/pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_pipeline_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/tests/test_notebook_output_contracts.py`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/README.md`
  - `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/artifacts/gas_source_diagnostics_by_ed_stay.csv`
- Commands:
  - `make lint`
  - `uv run pytest -q tests/test_pipeline_contracts.py tests/test_notebook_output_contracts.py`
  - `COHORT_OTHER_RELATIVE_REDUCTION_MIN=0 make quarto-cohort`
  - `make test`
  - `make contracts-check STAGE=all`
