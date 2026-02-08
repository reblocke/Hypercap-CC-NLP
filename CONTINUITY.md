Goal (incl. success criteria):
- Implement and verify fixes for residual output anomalies in `MIMICIV_hypercap_EXT_cohort.ipynb` so final outputs are semantically correct and reproducible.
- Success = canonical workbook and QA artifacts show coherent ABG/VBG/other thresholds+flags, any-gas aggregation, and clear row-count semantics with no runtime errors.

Constraints/Assumptions:
- Preserve hadm-level canonical output contract (`MIMICIV all with CC.xlsx`).
- Keep notebook runnable start-to-finish via VS Code and nbconvert.
- Avoid adding debug-only forensic logic into production notebook.

Key decisions:
- Use threshold semantics: ABG >=45 mmHg, VBG >=50 mmHg, OTHER >=50 mmHg.
- Keep source buckets as arterial / venous / other (exclude non-blood-gas CO2 labels).
- Add atomic workbook write to prevent zero-byte canonical output if run is interrupted.

State:
- Done: Debug forensics executed and Option B integrated.
- Done: Threshold normalization and hadm->ed flag alignment are in notebook.
- Done: Patched final export cell to write dated/canonical Excel via tmp file + `os.replace`.
- Done: Terminated orphan nbconvert background process that had clobbered canonical workbook.
- Done: Restored canonical workbook from latest successful dated artifact.
- Now: Reporting verification and residual warnings.
- Next: Optional cleanup of historical zero-byte archive artifacts and optional FutureWarning noise suppression.

Done:
- `MIMICIV_hypercap_EXT_cohort.ipynb`: atomic write helper (`_atomic_write_excel`) added in final export cell.
- Syntax check over notebook code cells passed (0 syntax errors).
- Output integrity checks:
  - Canonical workbook size restored to non-zero (32,703,370 bytes).
  - Workbook rows: 41,322 (hadm-level with CC), unique `hadm_id` and `ed_stay_id` = 41,322.
  - Key thresholds/flags non-zero and coherent:
    - `abg_hypercap_threshold` 10,748
    - `vbg_hypercap_threshold` 17,549
    - `other_hypercap_threshold` 36,518
    - `pco2_threshold_any` 41,080
    - `flag_any_gas_hypercapnia` 41,080
- `qa_summary.json` reflects expected rows (`hadm_cc_rows` 41,322; `ed_spine_rows` 41,394).

Now:
- Residual warning profile:
  - No notebook `output_type:error` present.
  - One pandas `FutureWarning` remains in outputs (non-fatal).
  - `gas_source_other_rate` remains 1.0 (expected under current source mapping logic).

Next:
- If requested, patch non-fatal `FutureWarning` source.
- If requested, add run lock/guard to prevent concurrent writer collisions.

Open questions (UNCONFIRMED if needed):
- UNCONFIRMED: Whether to treat `gas_source_other_rate == 1.0` as acceptable current-state behavior or require additional source inference refinement now.

Working set (files/ids/commands):
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMICIV_hypercap_EXT_cohort.ipynb`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMIC tabular data/MIMICIV all with CC.xlsx`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/MIMIC tabular data/prior runs/2026-02-07 MIMICIV all with CC.xlsx`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/qa_summary.json`
- `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/debug/abg_vbg_capture/artifacts/nbconvert_verify_debug_20260207_171136.log`
