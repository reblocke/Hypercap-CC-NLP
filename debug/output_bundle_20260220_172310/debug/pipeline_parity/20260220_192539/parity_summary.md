# Pipeline Parity Summary

- Status: **fail**
- Generated UTC: `2026-02-20T19:26:18.230129+00:00`
- Baseline: `/Users/blocke/Box Sync/Residency Personal Files/Scholarly Work/Locke Research Projects/Hypercap-CC-NLP/artifacts/baselines/jupyter/20260220_152137_post_residual_integrity`
- Findings: P0=6, P1=2, P2=10

## Metric Drift
- icu_link_rate: baseline=0.7677753266587477, current=0.7726366506064131, severity=ok
- pct_any_gas_0_6h: baseline=0.41897166129305957, current=0.4184249875394584, severity=ok
- pct_any_gas_0_24h: baseline=0.9590191752927202, current=0.9582987207177271, severity=ok
- gas_source_other_rate: baseline=0.6617519019140463, current=0.6626423339262455, severity=ok

## Findings
- [P1] `row_count_mismatch` cohort row count mismatch: baseline=11769 current=12020
- [P0] `id_contract_mismatch` cohort hadm_id_hash differs between baseline and current run.
- [P0] `id_contract_mismatch` cohort subject_id_hash differs between baseline and current run.
- [P0] `id_contract_mismatch` cohort id_pair_hash differs between baseline and current run.
- [P1] `row_count_mismatch` classifier row count mismatch: baseline=11769 current=12020
- [P0] `id_contract_mismatch` classifier hadm_id_hash differs between baseline and current run.
- [P0] `id_contract_mismatch` classifier subject_id_hash differs between baseline and current run.
- [P0] `id_contract_mismatch` classifier id_pair_hash differs between baseline and current run.
- [P2] `analysis_numeric_sum_drift` Symptom_Composition_by_Hypercapnia_Definition.xlsx::Sheet1 numeric sum drift 8616.010000.
- [P2] `analysis_numeric_sum_drift` Symptom_Composition_Pivot_ChartReady.xlsx::Sheet1 numeric sum drift 0.020000.
- [P2] `analysis_numeric_sum_drift` Symptom_Composition_by_ABG_VBG_Overlap.xlsx::Sheet1 numeric sum drift 0.400000.
- [P2] `analysis_numeric_sum_drift` Symptom_Composition_by_Gas_Source_Overlap.xlsx::Sheet1 numeric sum drift 0.600000.
- [P2] `analysis_numeric_sum_drift` Symptom_Composition_by_ICD_Gas_Overlap.xlsx::Sheet1 numeric sum drift 0.100000.
- [P2] `analysis_numeric_sum_drift` ICD_Positive_Subset_Breakdown.xlsx::Gas_criteria numeric sum drift 91.300000.
- [P2] `analysis_numeric_sum_drift` ICD_vs_Gas_Performance.xlsx::Sheet1 numeric sum drift 1004.123545.
- [P2] `analysis_numeric_sum_drift` Ascertainment_Overlap_Intersections.xlsx::Sheet1 numeric sum drift 251.000000.
- [P2] `analysis_binary_hash_drift` Ascertainment_Overlap_UpSet.png binary hash changed between baseline and current run.
- [P2] `rater_match_count_delta` Rater matched_rows changed: baseline=60 current=61.
