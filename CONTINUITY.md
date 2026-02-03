Goal (incl. success criteria):
- Create a single merged spreadsheet that preserves the old 2025-10-14 cohort columns and appends new columns from the refactored outputs; document which sources contribute which fields.

Constraints/Assumptions:
- Follow AGENTS.md instructions; Python-first; prioritize accuracy and reproducibility.
- Use evidence from notebooks; avoid speculation.

Key decisions:
- Use existing code/notebooks to determine cohort flow counts and definitions; flag gaps explicitly.

State:
- Done: Notebook updated to emit merged CC spreadsheet and OR comorbidity flags across ED + hospital sources.
- Now: Verified new 2026-02-02 CC output against old and merged files.
- Next: Confirm if per-source comorbidity columns should remain.

Done:
- Updated ledger for cohort-construction review request.
- Located cohort inclusion logic, thresholds, and printed counts in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added cohort flow count cell (ED/ICU/blood gas/hypercapnia/CC) to MIMICIV_hypercap_EXT_cohort.ipynb.
- Added ascertainment overlap counts cell (ABG-only/VBG-only/both; ICD+gas) to MIMICIV_hypercap_EXT_cohort.ipynb.
- Verified ICD code handling (any diagnosis; ED + hospital sources combined) and code list in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added ICD source flags and categorical source variable (ED vs HOSP vs BOTH vs NONE) in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added missingness summary cell (chief complaint, race/ethnicity, ED triage/vitals) in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added symptom distribution by overlap cell and exports in Hypercap CC NLP Analysis.ipynb.
- Added rationale markdown cells and section headings across all notebooks (cohort, NLP classifier/analysis, rater agreement, legacy notebooks).
- Added ED-stay cohort expansion section with inventory, enrichment, labs, OMR, comorbidities, timing phenotypes, QA, and output exports.
- Added ICU POC gas capture (chartevents) phase with item discovery, extraction, panels, and incremental yield reporting.
- Added Excel export for ED-stay cohort dataset.
- Added two final Excel outputs: all encounters with ED linkage flag, and ED chief-complaint-only ED-stay rows.
- Added safeguards to rename ed_stay_id from stay_id in ED triage/vitals data to avoid merge KeyError.
- Replaced ED triage/vitals cell with forced re-query + column diagnostics and merges on ed_stay_id.
- Audited notebooks for randomness; only random sampling found was already seeded.
- Added ED unique count in inventory and grouped missing-fields report by section.
- Added diagnostic fingerprint comment in ED triage/vitals cell.
- Patched run_sql_bq to accumulate array parameters instead of overwriting query_parameters.
- Added notebook edit/run protocol to AGENTS.md.
- Fixed lab item regex patterns (word boundaries) and added missing-column guard for panel aggregation.
- Added missing-column guard for POC panel aggregation (pco2/ph/hco3/lactate).
- Added guard to drop existing flag_any_gas_hypercapnia_poc before merging.
- Inserted pipeline stages markdown, helper utilities, SQL registry, and QA checks in notebook.
- Migrated 19 *_sql definitions into SQL registry and replaced run_sql_bq(...) calls to use sql("name").
- Updated ED triage/vitals merge to select common keys dynamically (ed_stay_id or hadm_id).
- Added ed_df column snapshot output in inventory cell.
- Switched ICD comorbidity flags to SQL-side aggregation (per-hadm output).
- Added prefix filters to reduce BigQuery CPU usage for ICD query.
- Inserted execution timing note near top of notebook.
- Redirected ed_vitals_long/labs_long/gas_panels/gas_panels_poc parquet outputs to DATA_DIR.
- Redirected cohort_ed_stay.parquet to DATA_DIR.
- Compared Excel outputs for column and key differences.
- Extracted schema summaries for all Excel/Parquet files in MIMIC tabular data.
- Created merged CC spreadsheet with old NLP columns + new refactor columns:
  - mimic_hypercap_EXT_CC_with_NLP_plus_newcols_20260202_171627.xlsx (186 cols).
- Modified comorbidity ICD flags to include ED + hospital sources (combined OR into flag_*).
- Added merged CC export cell (outputs YYYY-MM-DD MIMICIV all with CC.xlsx).
- Compared new 2026-02-02 CC file vs old/merged:
  - New vs old: 0 diffs on shared columns; new has 75 extra columns.
  - New vs merged: only diffs in combined comorbidity flags; new adds *_ed/_hosp columns.

Now:
- Fixed QA cell to use per-stay gas_source_unknown_rate instead of undefined panel_source_unknown_rate.
- Appended data dictionary export cell to notebook.

Next:
- User to re-run notebook; verify new time-based fields and data dictionary outputs.

Open questions (UNCONFIRMED if needed):
- Whether to keep per-source comorbidity columns (_hosp/_ed) in final outputs.

Working set (files/ids/commands):
- MIMIC tabular data/*.xlsx, *.parquet
- MIMICIV_hypercap_EXT_cohort.ipynb
- CONTINUITY.md
