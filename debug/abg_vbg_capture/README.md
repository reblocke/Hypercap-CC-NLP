# ABG/VBG Capture Forensics (Debug-Only)

This debug package investigates ABG/VBG capture deltas without changing main pipeline logic.

## Scope
- Compare three hadm-level capture views on the same hadm universe:
  - `current_strict`
  - `legacy_replay`
  - `permissive_envelope`
- Attribute dropped admissions to deterministic reason codes.
- Produce evidence-backed fix candidates for later minimal back-port to `MIMICIV_hypercap_EXT_cohort.ipynb`.

## Files
- `01_extract_current_vs_legacy.ipynb`
- `02_reason_attribution.ipynb`
- `03_fix_validation.ipynb`
- `sql_registry.py`
- `artifacts/` (timestamped outputs)

## Inputs
- Canonical current comparator:
  - `MIMIC tabular data/MIMICIV all with CC.xlsx`
- Legacy comparator:
  - `MIMIC tabular data/2025-10-14 MIMICIV all with CC.xlsx`

Only `hadm_id` is required from both files.

## Outputs (under `artifacts/`)
- `capture_comparison_<timestamp>.parquet`
- `capture_comparison_<timestamp>.csv`
- `permissive_long_<timestamp>.parquet`
- `reason_attribution_<timestamp>.csv`
- `reason_top_labels_<timestamp>.csv`
- `manual_review_sample_<timestamp>.csv`
- `fix_candidates_<timestamp>.csv`
- `fix_candidates.md`
- `run_metadata_<timestamp>.json`

## Run order
1. Run `01_extract_current_vs_legacy.ipynb`.
2. Run `02_reason_attribution.ipynb`.
3. Run `03_fix_validation.ipynb`.

## Notes
- This is intentionally debug-only; no changes to the main cohort notebook are made here.
- The 2025 output is used as comparator only, not as ground truth.
