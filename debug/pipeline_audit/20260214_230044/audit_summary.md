# Pipeline Audit Summary (20260214_230044)

- Status: **warning**
- Generated UTC: `2026-02-14T23:19:10.217990+00:00`
- Strictness: `fail_on_key_anomalies`
- Findings: P0=0, P1=0, P2=1

## Stage Health
- `01_cohort`: status=ok, returncode=0, duration_s=494.957173
- `02_classifier`: status=ok, returncode=0, duration_s=346.511508
- `03_rater`: status=ok, returncode=0, duration_s=47.647889
- `04_analysis`: status=ok, returncode=0, duration_s=48.133867

## Drift Summary
- classifier_rows: current=41322.0, baseline=41322.0, severity=ok
- cohort_rows: current=41322.0, baseline=41322.0, severity=ok
- first_other_pco2_pct_eq_160_poc: current=0.1367172643, baseline=0.1367172642716365, severity=ok
- gas_source_other_rate: current=0.7711144274014472, baseline=0.7711144274014472, severity=ok
- icu_link_rate: current=0.770908827366285, baseline=0.770908827366285, severity=ok
- pct_any_gas_0_24h: current=0.9468280427115041, baseline=0.9468280427115041, severity=ok
- pct_any_gas_0_6h: current=0.37821906556505774, baseline=0.37821906556505774, severity=ok

## Findings
- [P2] `missing_work_dir_env_key` Optional .env key WORK_DIR is unset; reproducibility manifest will rely on cwd.
