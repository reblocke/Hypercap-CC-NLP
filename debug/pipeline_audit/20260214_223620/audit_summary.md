# Pipeline Audit Summary (20260214_223620)

- Status: **fail**
- Generated UTC: `2026-02-14T22:59:48.659978+00:00`
- Strictness: `fail_on_key_anomalies`
- Findings: P0=0, P1=1, P2=0

## Stage Health
- `01_cohort`: status=ok, returncode=0, duration_s=773.161009
- `02_classifier`: status=ok, returncode=0, duration_s=348.443217
- `03_rater`: status=ok, returncode=0, duration_s=48.479748
- `04_analysis`: status=ok, returncode=0, duration_s=57.999966

## Drift Summary
- classifier_rows: current=41322.0, baseline=41322.0, severity=ok
- cohort_rows: current=41322.0, baseline=41322.0, severity=ok
- first_other_pco2_pct_eq_160_poc: current=0.1367172643, baseline=0.1367172642716365, severity=ok
- gas_source_other_rate: current=0.7711144274014472, baseline=0.7711144274014472, severity=ok
- icu_link_rate: current=0.770908827366285, baseline=0.770908827366285, severity=ok
- pct_any_gas_0_24h: current=0.9468280427115041, baseline=0.9468280427115041, severity=ok
- pct_any_gas_0_6h: current=0.37821906556505774, baseline=0.37821906556505774, severity=ok

## Findings
- [P1] `missing_required_env_keys` Missing required .env keys: ['WORK_DIR']
