# Pipeline Specification

This document is the canonical current-state contract for the pipeline. Use [`README.md`](../README.md) for public setup/runbook guidance and [`docs/DECISIONS.md`](DECISIONS.md) for dated rationale and superseded decisions.

## Public Release Boundary

The public repository contains source code, specifications, tests, and documentation only. It must not track row-level MIMIC-derived data, annotation workbooks, generated results, debug logs, local environment files, or release asset zips.

Generated manuscript assets are created locally under `Results/YYYY-MM-DD/` and should be distributed, when appropriate, as GitHub/Zenodo release assets with checksums and a manifest. They are not committed to git.

## Canonical Pipeline Stages

The main four-stage pipeline is Quarto-first and notebook-native:

1. `MIMICIV_hypercap_EXT_cohort.qmd`
2. `Hypercap CC NLP Classifier.qmd`
3. `Rater Agreement Analysis.qmd`
4. `Hypercap CC NLP Analysis.qmd`

Stage ownership is fixed to those notebooks. `make quarto-reyan-figures` remains a compatibility alias that renders `Hypercap CC NLP Analysis.qmd`.

`Chart Review Sample Calc.qmd` is a supporting Quarto notebook for chart-review sample size calculations. It is not one of the core four stages, but it follows the same local output-location contract.

## Private Handoffs And Local Outputs

| Stage | Private inputs | Local outputs |
|---|---|---|
| `MIMICIV_hypercap_EXT_cohort.qmd` | BigQuery-backed MIMIC-IV HOSP/ICU/ED data, the official derived `ventilation` concept, and local `.env` settings | `MIMIC tabular data/MIMICIV all with CC.xlsx` and required private `MIMICIV IMV source provenance.json`; dated manifests/audits under `MIMIC tabular data/prior runs/`; QA payloads under `artifacts/qa/cohort/` |
| `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/MIMICIV all with CC.xlsx` unless overridden by `CLASSIFIER_INPUT_FILENAME`; optional private annotation workbook when `CLASSIFIER_ANNOTATION_BENCHMARK_MODE` is `auto` or `required` | `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`; optional direct annotation benchmark workbooks/sidecars under `MIMIC tabular data/`; dated manifests/audits under `MIMIC tabular data/prior runs/` |
| `Rater Agreement Analysis.qmd` | Canonical NLP workbook plus private annotation workbook (`RATER_ANNOTATION_PATH` when overridden); optional direct annotation NLP benchmark workbook and candidate sidecars | Agreement audits, direct-vs-cohort benchmark diagnostics, and summaries under `artifacts/qa/rater_agreement/` |
| `Hypercap CC NLP Analysis.qmd` | `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx` unless overridden by `ANALYSIS_INPUT_FILENAME`, plus its matching required private `MIMICIV IMV source provenance.json` | Figures, tables, PDFs, submission bundle, and analysis exports under `Results/YYYY-MM-DD/`; QA checks under `artifacts/qa/analysis/` |
| `Chart Review Sample Calc.qmd` | Local R package environment plus notebook inputs | `Results/YYYY-MM-DD/Chart Review Sample Calc.html` and `Results/YYYY-MM-DD/Chart Review Sample Calc_files/` |

Pipeline order is `cohort -> classifier -> rater -> analysis`. The analysis stage's statistical estimates come from the canonical NLP workbook. The committed `analysis_manifest.yml` freezes definition-only manuscript rules including the admission-level unit of analysis, RFV taxonomy, gas thresholds, timing windows, pH/HCO3 bands, and sensitivity definitions. The clean submission bundle copies selected publication-facing assets into `Results/YYYY-MM-DD/submission_assets/` and writes `submission_assets_manifest.csv`. Direct analysis-stage renders must not fail solely because optional upstream supplement workbooks are absent; missing optional upstream assets are recorded under `artifacts/qa/analysis/submission_asset_optional_missing.csv`.

Manual annotation workbook curation remains private and independent. The public `Annotation/` directory contains only classifier resource CSVs and the example resource manifest.

## Output Contract

Generated outputs are local/private by default:

- stage PDFs: `Results/YYYY-MM-DD/MIMICIV_hypercap_EXT_cohort.pdf`, `Hypercap CC NLP Classifier.pdf`, `Rater Agreement Analysis.pdf`, and `Hypercap CC NLP Analysis.pdf`
- chart-review render: `Results/YYYY-MM-DD/Chart Review Sample Calc.html` plus its HTML bundle
- manuscript assets: `Figure 1.pdf`, `Figure 2-4.pdf/.xlsx`, `Table 1-2.xlsx`, supplement figure aliases `Figure S1-S10.pdf/.png`, and selected `Figure S*.xlsx` workbooks as produced by the analysis notebook
- submission bundle: `Results/YYYY-MM-DD/submission_assets/`
- run-level output manifest and reviewer README: `Results/YYYY-MM-DD/submission_manifest.xlsx`, `submission_manifest.csv`, and `OUTPUTS_README.md`
- supplement-ready aggregate analysis workbooks: `Supplementary_Table_Acid_Base_Source_Missingness.xlsx`, `Candidate_Definition_Yield_Composition.xlsx`, `Sensitivity_Analysis_Suite.xlsx`, and `IMV_Qualifying_Gas_Timing_Sensitivity.xlsx`
- IMV timing sensitivity assets: `Figure S10.pdf`, `Figure S10.xlsx`, and `imv_timing_manuscript_summary.md`
- cohort-stage IMV timing QA: `artifacts/qa/cohort/imv_qualifying_gas_timing_audit.csv`
- QA/debug/manifests: `artifacts/qa/cohort/`, `artifacts/qa/rater_agreement/`, `artifacts/qa/analysis/`, `artifacts/qa/baselines/`, and `debug/...`
- private handoff workbooks: `MIMIC tabular data/MIMICIV all with CC.xlsx` and `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- required private IMV source-evidence sidecar: `MIMIC tabular data/MIMICIV IMV source provenance.json` and a dated copy under `prior runs/`; never a submission asset
- optional private direct annotation benchmark outputs: `MIMIC tabular data/annotation_benchmark_with_NLP.xlsx`, `annotation_visit_candidate_scores.csv`, and `annotation_segment_candidate_scores.csv`
- rater supplement validation outputs include direct-benchmark denominator notes, per-category support/precision/recall/F1, bootstrap confidence intervals, canonical/grouped confusion matrices, and redacted disagreement examples; encounter identifiers remain excluded from supplement-facing disagreement sheets

Submission-facing aggregate outputs must not include `subject_id`, `hadm_id`, `ed_stay_id`, raw chief complaint text, or other row-level MIMIC data. Private row-level audit files, when generated for local review, are allowed only under ignored output locations and must not be included in `submission_assets/`.

`Drafts/` is manual-only working space and must not be modified by automated render or migration steps. Public release branches must not include `Drafts/`, `Results/`, `MIMIC tabular data/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, or `Legacy Code/`.

Manuscript-facing ascertainment terminology is fixed:

- **Ascertainment indicators** are overlapping ABG-positive, VBG-positive, and ICD-positive indicators; admissions may satisfy more than one indicator. Figure 2 uses this overlapping indicator vocabulary.
- **Source-specific overlap displays** are reconciliation views across ABG, VBG, ICD, and UNKNOWN-source gas. Figure S6 shows all nonzero source-specific intersections, including indeterminate-source gas, and its counts sum to the analytic cohort.
- **Ascertainment strata** are mutually exclusive gas-only, ICD-only, and both ICD + gas groups. Figure 1 Panel A and Table 1 use this strata vocabulary.

## Notebook Runtime Contract

The core phase logic for the four-stage pipeline must live in the `.qmd` notebooks listed above. Standalone `.py` modules are allowed for QA, contracts, audits, parity checks, scripts, and tests only.

Renderable notebooks must be self-contained at runtime:

- no runtime imports from `src/` or other repo-local Python packages
- no `src/` path injection in executed code
- execution-critical helpers must be defined in clearly labeled `Local helper functions` sections inside the notebook that uses them

If a collaborator receives a `.qmd` plus the declared input data and environment settings, they should be able to recreate that stage's outputs by rendering that notebook alone.

Legacy `.ipynb` compatibility notebooks are not part of the public release.

## Canonical Environment Knobs

These environment variables change the contract-relevant execution surface:

| Variable | Contract effect |
|---|---|
| `RESULTS_DATE` | Selects the dated output folder under `Results/YYYY-MM-DD/` and the cohort/classifier/analysis producer-manifest filenames; `generated_utc` retains actual execution time |
| `RESULTS_DIR` | Overrides the concrete results path; defaults to `Results/<date>` |
| `CLASSIFIER_INPUT_FILENAME` | Overrides the classifier's cohort-workbook handoff input |
| `CLASSIFIER_ANNOTATION_BENCHMARK_MODE` | Controls direct annotation scoring: `auto` skips when absent, `required` fails when absent, and `off` disables it |
| `CLASSIFIER_ANNOTATION_BENCHMARK_PATH` | Overrides the private annotation workbook scored by the classifier for direct NLP validation |
| `CLASSIFIER_ANNOTATION_BENCHMARK_SHEET` | Overrides the annotation workbook sheet for direct classifier scoring |
| `CLASSIFIER_ANNOTATION_CC_COLUMN` | Overrides chief-complaint column detection for direct annotation scoring |
| `RATER_NLP_INPUT_FILENAME` | Overrides the rater stage NLP-workbook handoff input |
| `RATER_ANNOTATION_PATH` | Overrides the annotation workbook used by the rater stage |
| `RATER_BENCHMARK_SOURCE` | Selects NLP validation denominator: `auto`, `annotation_direct`, or `cohort_overlap` |
| `RATER_ANNOTATION_BENCHMARK_NLP_FILENAME` | Overrides the direct annotation NLP workbook consumed by the rater stage |
| `RATER_ANNOTATION_VISIT_CANDIDATE_SCORES_FILENAME` | Overrides the direct annotation visit-candidate sidecar consumed by the rater stage |
| `RATER_ANNOTATION_SEGMENT_CANDIDATE_SCORES_FILENAME` | Overrides the direct annotation segment-candidate sidecar consumed by the rater stage |
| `ANALYSIS_INPUT_FILENAME` | Overrides the analysis stage NLP-workbook handoff input |
| `BQ_DATASET_DERIVED` | Names the official MIMIC-IV derived dataset; defaults to `mimiciv_derived` and is used for `_metadata` and `ventilation`, not `bg` |
| `BQ_QUERY_TIMEOUT_SECS` | Overrides the cohort-stage BigQuery timeout |
| `WRITE_ARCHIVE_XLSX_EXPORTS` | Enables additional dated private workbook archives in `MIMIC tabular data/prior runs/` without changing canonical outputs |
| `COHORT_DEBUG_INVENTORY` | Enables extra cohort inventory/debug exports outside the default path |
| `COHORT_FAIL_ON_ALL_OTHER_SOURCE` | Fails the cohort stage if gas-source attribution collapses to all other/unknown |
| `COHORT_WARN_OTHER_RATE` | Warn threshold for high gas-source UNKNOWN/other rate |
| `COHORT_FAIL_OTHER_RATE` | Optional hard-fail threshold for high gas-source UNKNOWN/other rate |
| `COHORT_GAS_SOURCE_INFERENCE_MODE` | Sets the gas-source inference mode for the cohort stage |
| `COHORT_OTHER_RELATIVE_REDUCTION_MIN` | Optional relative UNKNOWN-rate reduction target |
| `COHORT_OTHER_HADM_INCREASE_FAIL_PP` | Optional hadm-level fail guard for sharp UNKNOWN-rate increase |
| `COHORT_FAIL_ON_OMR_ATTACH_INCONSISTENCY` | Fails when OMR candidates exist but attached outputs are all null |
| `COHORT_ALLOW_EMPTY_OMR` | Allows intentional reruns with empty OMR |
| `COHORT_ALLOW_OMR_QUERY_FAILURE` | Allows continuation with empty OMR when the OMR query fails |
| `COHORT_ANTHRO_CHARTED_FALLBACK` | Enables charted anthropometric fallback extraction |
| `COHORT_ANTHRO_NEAREST_ANYTIME` | Enables nearest-anytime anthropometric fallback selection |
| `COHORT_ANTHRO_MIN_BMI_COVERAGE` | Minimum BMI coverage floor enforced by the cohort contract |
| `CLASSIFIER_STRICT_RESOURCE_HASH` | Fails on resource-manifest hash mismatch when enabled |
| `CC_SPELL_CORRECTION_MODE` | Selects classifier spell-correction behavior |
| `PIPELINE_CONTRACT_MODE` | Controls whether contract checks warn or fail |
| `RUN_MANIFEST_STAGE_SCOPE` | Controls which stage manifests are emitted |
| `RUN_MANIFEST_REQUIRE_CLEAN_GIT` | Controls whether dirty git state is recorded or treated as a hard failure |
| `WORK_DIR` | Sets the explicit workspace root used by manifests and notebooks |
| `GOOGLE_APPLICATION_CREDENTIALS` | Optional service-account auth path for BigQuery access |
| `HF_TOKEN` | Optional authenticated Hugging Face access token |

Required backend/data-access settings for the canonical BigQuery workflow remain:

- `MIMIC_BACKEND=bigquery`
- `WORK_PROJECT`
- `BQ_PHYSIONET_PROJECT`
- `BQ_DATASET_HOSP`
- `BQ_DATASET_ICU`
- `BQ_DATASET_ED`
- `BQ_DATASET_DERIVED` (default `mimiciv_derived`)

## IMV Timing Relative To The Qualifying Gas

This is a secondary, descriptive sensitivity analysis. It does not change the
primary cohort, existing primary results, RFV taxonomy, or any regression or
prediction model. It asks only whether reliable observed IMV evidence preceded
the earliest qualifying PCO2 timestamp; temporal ordering is not evidence that
IMV caused hypercapnia.

### Fail-closed source contract

- `BQ_DATASET_DERIVED` defaults to `mimiciv_derived`. The cohort stage queries
  `{BQ_PHYSIONET_PROJECT}.{BQ_DATASET_DERIVED}._metadata` as an
  `attribute`/`value` table and requires exactly one normalized
  `attribute = mimic_version` row whose `value` is exactly `3.1`.
- The metadata table, expected attribute/value schema, and
  `{BQ_PHYSIONET_PROJECT}.{BQ_DATASET_DERIVED}.ventilation` must all be
  accessible. Any failure stops the cohort stage. The implementation must not
  query a derived `bg` table or silently substitute the legacy regex.
- Official episodes are restricted to `ventilation_status = 'InvasiveVent'`.
  `stay_id` is joined to the active ICU `icustays` table to recover `hadm_id`.
  That ICU dataset must resolve explicitly to `mimiciv_3_1_icu` or
  `mimiciv_v3_1_icu`; an unversioned ICU fallback is rejected before the join,
  and the admission-level minimum `starttime` becomes
  `first_derived_imv_starttime`. Episode rows may remain in memory for QA but
  must not be exported to submission-facing assets.
- ICU `procedureevents` item IDs `224385` and `225792` provide explicit
  Intubation and Invasive Ventilation times. Before extraction, both IDs must be
  present in the active ICU `d_items` table and their normalized labels must be
  exactly `intubation` and `invasive ventilation`; otherwise the stage fails.
  `starttime` is minimized by admission for each item.
- `first_imv_time`, `imv_chart_flag`, and the combined legacy `imv_flag` remain
  QA-only. They do not define the new temporal subgroup.

### Private handoff fields

These eleven admission-level fields are restricted MIMIC-derived data. They are
required in both canonical private handoffs and must never be copied as
admission-level rows into `Results/`, `submission_assets/`, or git.

| Field | Exact handoff type and allowed values | Derivation and validation |
|---|---|---|
| `first_derived_imv_starttime` | `datetime64[ns]`; timestamp or `NaT` | Minimum `starttime` across official `InvasiveVent` episodes after `stay_id`-to-`hadm_id` linkage; must equal the admission minimum when present. |
| `first_intubation_procedure_time` | `datetime64[ns]`; timestamp or `NaT` | Minimum ICU `procedureevents.starttime` for validated item `224385`; may be missing when no event is observed. |
| `first_invasive_ventilation_procedure_time` | `datetime64[ns]`; timestamp or `NaT` | Minimum ICU `procedureevents.starttime` for validated item `225792`; may be missing when no event is observed. |
| `first_observed_imv_time` | `datetime64[ns]`; timestamp or `NaT` | Minimum nonmissing value across the three robust timestamps; must be missing exactly when all three are missing. |
| `first_observed_imv_source` | pandas `string`; `intubation_procedure`, `invasive_ventilation_procedure`, `derived_ventilation_episode`, `multiple_sources_same_time`, or `missing` | Identifies the source of the minimum; use `multiple_sources_same_time` when two or more robust sources share the exact minimum and `missing` only when no robust timestamp exists. |
| `robust_imv_observed` | pandas nullable integer `Int64`; `0` or `1`, nonmissing | `1` when any robust timestamp is nonmissing; otherwise `0`. It must agree with `first_observed_imv_time` availability. |
| `imv_qualifying_gas_order` | pandas `string`; `not_applicable_no_qualifying_gas`, `no_observed_imv`, `qualifying_gas_before_imv`, `imv_before_qualifying_gas`, or `timing_indeterminate`, nonmissing | Uses `qualifying_pco2_time` and strict `<`/`>` comparison with no rounding, tolerance, or buffer. Exact ties, a missing gas time, an official IMV source record without any usable source timestamp, robust evidence with missing time, and legacy-only IMV evidence are indeterminate. ICD-only admissions are not applicable. |
| `imv_preceded_qualifying_gas` | pandas nullable `boolean`; `True`, `False`, or `NA` | `True` only for `imv_before_qualifying_gas`; `False` for `no_observed_imv` or `qualifying_gas_before_imv`; `NA` otherwise. |
| `no_prior_observed_imv` | pandas nullable `boolean`; `True`, `False`, or `NA` | `True` for `no_observed_imv` or `qualifying_gas_before_imv`; `False` for `imv_before_qualifying_gas`; `NA` otherwise. |
| `hours_from_imv_to_qualifying_gas` | `float64` hours; any finite real value or `NaN` | `(qualifying_pco2_time - first_observed_imv_time)` in hours. Negative means gas before observed IMV, positive means observed IMV before gas, and zero is an exact tie classified indeterminate. |
| `legacy_imv_timing_discordant` | pandas nullable integer `Int64`; `0` or `1`, nonmissing | `1` when robust presence differs from `imv_flag`, or when gas-positive admissions with both robust and legacy timestamps have different strict before/after/tie relations to `qualifying_pco2_time`; otherwise `0`. Missing legacy timing alone is not discordance. |

Among gas-positive admissions, the four analysis strata
`no_observed_imv`, `qualifying_gas_before_imv`,
`imv_before_qualifying_gas`, and `timing_indeterminate` are mutually exclusive
and exhaustive. ICD-only admissions are
`not_applicable_no_qualifying_gas` and are excluded from the timing analysis.
The primary new sensitivity cohort, `gas_positive_no_prior_observed_imv`, is the
union of `no_observed_imv` and `qualifying_gas_before_imv`; indeterminate rows
are excluded.

The cohort keeps its untimed-official-source evidence flag only in memory and
asserts it before export. Every optional row-level archive export must pass
through the same nonmutating sanitizer that drops this internal field and
asserts its absence; no archive path may write the in-memory frame directly.
The eleven workbook fields remain unchanged. To preserve independently
checkable untimed-source evidence, the cohort also writes the required private
`MIMIC tabular data/MIMICIV IMV source provenance.json` sidecar and a dated copy
under `prior runs/`. The sidecar is restricted admission-level data, not an
aggregate QA or submission artifact. It must never enter `Results/`,
`submission_assets/`, release assets, or git.

The sidecar records source-derived untimed-evidence membership before the
internal flag is dropped, never membership inferred from the supplied temporal
stratum. Its schema, admission count, member uniqueness, payload digest, and
deterministic source-projection fingerprint are validated at the analysis
boundary. The common projection comprises `subject_id`, `hadm_id`,
`pco2_threshold_any`, `imv_flag`, `qualifying_pco2_time`, the three robust source
timestamps, and `first_imv_time`; it is unchanged by classification. A missing,
malformed, stale, or mismatched sidecar fails closed, as does untimed membership
contradicting reliable source timestamps. The cohort and analysis manifests
record the sidecar path and SHA-256. Digests establish transfer integrity, not
independent source authenticity; approved private-transfer provenance remains
required.

The analysis stage must recompute robust presence, the exact minimum timestamp
and its source/tie label, signed hours, strict temporal order, nullable
indicators, and legacy discordance from the component timestamps, qualifying-gas
and legacy fields, and verified untimed membership. With otherwise absent IMV
evidence, `timing_indeterminate` is required exactly for admissions with verified
untimed-source evidence; it is not a discretionary alternative to
`no_observed_imv`. Both directions of incorrect relabeling must be rejected.
Supplied derived values must never be used as their own validation reference.

The production load/preparation path must parse the six required gas/IMV
timestamp columns directly with the strict parser before permissive coercion or
fallback can turn a malformed nonmissing cell into `NaT`. This includes
`qualifying_pco2_time` and legacy `first_imv_time`, not only the four robust
source/anchor fields. Genuine missing values remain missing. Regression tests
must exercise the actual notebook preparation statements as well as the
standalone validator.

### Aggregate QA and manuscript outputs

- `artifacts/qa/cohort/imv_qualifying_gas_timing_audit.csv` contains aggregate
  totals; the four-stratum and observed-source distributions; exact ties;
  official source-record evidence without a reliable timestamp; robust
  evidence with missing time; legacy-only and robust-only evidence;
  derived episodes beginning exactly at first ICU `intime`; and minimum, p05,
  p25, median, p75, p95, and maximum hours from IMV to the qualifying gas. The
  ICU-entry marker is descriptive only and does not change classification.
- `Results/YYYY-MM-DD/IMV_Qualifying_Gas_Timing_Sensitivity.xlsx` contains
  exactly `Group_Yield`, `Group_Characteristics`, `Grouped_RFV_Prevalence`,
  `Canonical_RFV_Prevalence`, `No_Prior_IMV_Sensitivity`, `IMV_Source_Audit`,
  and `Definitions` sheets.
- `Figure S10.pdf` and `Figure S10.xlsx` show grouped multi-label RFV
  prevalence for the four gas-positive temporal strata with panel denominators
  and patient-cluster bootstrap 95% confidence intervals.
- `imv_timing_manuscript_summary.md` reports prespecified aggregate counts and
  percentages, respiratory and injuries/adverse-effects prevalence, the maximum
  absolute grouped-RFV difference from the all-gas-positive reference, and one
  rule-generated neutral interpretation sentence.
- All four outputs are aggregate-only and exclude patient/encounter identifiers,
  raw chief-complaint text, and other row-level MIMIC data. No null-hypothesis
  tests or causal language are permitted.

### Existing-output parity control

`scripts/imv_ticket_parity.py` provides a private semantic safeguard for the
ticket. `capture` requires an explicit baseline ID/path, results date, and
resolvable source commit. It creates a fresh requested target and stores the
private handoffs plus the 14 enumerated existing result workbooks under that
ignored baseline, without overwriting an existing capture.

New schema-v2 captures require clean, date-matched cohort, classifier, and
analysis producer manifests naming that exact resolved source commit. The
three producer manifests record `results_date` and use that configured date in
their filenames, including for backdated or frozen-date runs; their
`generated_utc` values still record actual execution times. The
classifier records the SHA-256 of the exact cohort bytes parsed; analysis
records the SHA-256 of the exact classified bytes parsed and of every registered
result workbook. Capture checks the full producer/input/output hash chain,
stage order, and all 14 workbook hashes before accepting the baseline. It saves
copies of the producer manifests and byte hashes for all 16 captured artifacts.
The current checkout may differ from the producing commit; an unchecked
`--source-commit` label or checkout identity alone is not provenance. A render
predating these sealed producer manifests cannot create a new schema-v2
capture: preserve an existing capture instead of relabeling old outputs or
manufacturing provenance retrospectively.

Existing schema-v1 captures remain read-only and are explicitly reported as
`legacy_unverified` for producer provenance. Their recorded integrity and
semantic comparisons remain mandatory; this compatibility does not upgrade
their historical provenance. Never recapture a baseline after a parity failure.

Before comparing current outputs,
`compare` must validate the captured copies against the schema-versioned
manifest: exact handoff row counts and semantic hashes, the exact workbook
inventory, every stored per-sheet signature, and schema-v2 byte hashes and
producer manifests. A missing, altered,
incomplete, or unsupported baseline fails closed and current-output comparison
is skipped. Once baseline integrity passes, `compare` checks admission
membership, RFV1-RFV5 codes and labels, and all substantive workbook cells
against that same baseline. Only exact normalized documentation-sheet names in
the per-workbook allowlist (currently `Notes` for the 14-workbook contract) may
produce warning-only changes; a substantive name that merely contains
`note`, `definition`, or `readme` remains failure-eligible. The comparison
writes only an aggregate JSON report, including aggregate baseline-integrity
status (by default under
`debug/pipeline_parity/imv_ticket/<run-id>/semantic_report.json`) and never emits
row-level differences. Baseline copies, current results, and reports are ignored
and must not be committed.

Example contract:

```bash
uv run python scripts/imv_ticket_parity.py capture \
  --baseline <baseline-id-or-absolute-path> \
  --results-date <pre-ticket-results-date> \
  --source-commit <pre-ticket-commit>
uv run python scripts/imv_ticket_parity.py compare \
  --baseline <same-baseline-id-or-absolute-path> \
  --results-date <post-run-results-date>
```

A passing parity report establishes that the ticket did not change those
prespecified existing artifacts. It does not validate the new IMV timing
estimand, replace the denominator/privacy checks, or establish causal validity.

### Split-machine execution

The cohort-extraction machine must have credentialed access to HOSP, ICU, ED,
and the official derived dataset. If later stages run elsewhere, the current
private handoff, required IMV source-provenance sidecar, and producing manifests
must move together through an approved restricted-data channel; git sync does
not transfer these ignored files. Verify source/destination SHA-256 equality.
The receiving machine can run downstream stages from these inputs, but the run
is not an end-to-end render on one machine and must retain both input checksums
and their producing-stage provenance.

## Stage-Owned Invariants

The cohort-stage blood-gas item selection is versioned in `specs/blood_gas_itemids.json`.

Current cohort-stage invariants:

- ABG/VBG/UNKNOWN are the canonical source classes for definitive pCO2 values.
- UNKNOWN remains cohort-eligible for threshold inclusion.
- Gas qualification for enrollment is any-time during stay via `pco2_threshold_any`; `pco2_threshold_0_24h` is retained as a timing marker only.
- The analysis notebook treats gas timing as an inferential safeguard, not a cohort-enrollment gate: it compares RFV distributions across the broad EHR-ascertained cohort, first qualifying gas within 24h, first qualifying gas within 6h, any ICD-positive admissions, and ICD-positive admissions with qualifying gas within 24h.
- The IMV timing sensitivity uses strict timestamp ordering relative to
  `qualifying_pco2_time`; it does not alter cohort enrollment or establish a
  causal effect of ventilation.
- Analysis-stage candidate definition and sensitivity outputs are descriptive supplement-ready summaries only. They do not redefine the primary cohort, add regression/prediction models, or create new chief-concern categories.
- Frozen submission pH bands are `<7.20`, `7.20-7.24`, `7.25-7.29`, `7.30-7.34`, `7.35-7.44`, and `>=7.45`.
- Frozen submission bicarbonate bands are `<22`, `22-27`, `28-33`, and `>=34`.
- POC itemid QC telemetry does not itself gate cohort enrollment logic.
- Gas-source row diagnostics are written to `artifacts/qa/cohort/gas_source_diagnostics_by_ed_stay.csv`.
- Canonical cohort export fields use `qualifying_pco2_mmhg` rather than `first_pco2`.
- Canonical cohort export fields include paired qualifying-gas pH audit fields
  (`qualifying_ph`, `qualifying_ph_time`, `qualifying_ph_source_branch`,
  `qualifying_ph_site`, and `qualifying_ph_pairing_status`) matched to the
  earliest qualifying pCO2 by admission, source branch, site, and panel time.
  These fields support acidemia denominator audits and do not change cohort
  enrollment.
- The cohort stage writes an aggregate-only
  `qualifying_ph_pairing_completeness_audit.csv` covering broad gas-positive,
  24h gas, 6h gas, late gas, ICD+gas, and ICD-only scopes.
- Figure 4 acidemia severity uses paired qualifying-gas pH from the same
  specimen/panel as the earliest qualifying pCO2 when available; the older
  source-priority pH rule is retained only as a denominator/context audit.
- The first-by-site pO2 exports are `first_abg_po2`, `first_vbg_po2`, and `first_other_po2`.
- `first_hco3` is selected by qualifying panel bicarbonate first, then nearest serum bicarbonate/total CO2 fallback.

Anthropometric cleaning invariants:

- BMI `(10, 100]`
- height `[100, 230]` cm
- weight `(25, 400]` kg
- BMI is computed from cleaned height + weight only when recorded BMI is missing
- recorded-vs-computed BMI consistency is audited but does not overwrite a valid recorded BMI

## Acceptance Checks

### Numeric claims contract

`docs/NUMERIC_CLAIMS.yml` binds repeated aggregate claims to the accepted run's
producer SHA and to explicit aggregate workbook/CSV/JSON sources. The static
`make numbers-check` target validates active tracked documentation and notebook
snapshot assertions without requiring private inputs. The live
`make numbers-check-live RESULTS_DATE=<accepted-date>` target additionally
validates aggregate sources, figure/submission denominators, generated IMV
summary text, and extractable `N=` labels in publication PDFs.

Manuscript/preprint files, dated decision history, prior Results directories,
prior-run manifests, and sealed acceptance evidence are excluded from replacement.
They may be inventoried as history but must not be rewritten. A mismatch inside an
accepted Results directory is a producer/run acceptance problem: never patch the
generated artifact in place, and require a new dated full run after correcting its
producer. Numeric-audit reports remain aggregate-only under
`artifacts/qa/numeric_consistency/`.

A valid private pipeline run satisfies all of the following:

- `make quarto-pipeline RESULTS_DATE=<date>` renders the four stage PDFs into `Results/<date>/`
- `make quarto-chart-review RESULTS_DATE=<date>` renders the chart-review HTML bundle into `Results/<date>/`
- private handoff workbooks exist at `MIMIC tabular data/MIMICIV all with CC.xlsx` and `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- both private handoffs contain all eleven IMV timing fields, and every
  gas-positive admission has exactly one of the four temporal strata
- the required private IMV source-provenance sidecar passes schema, membership,
  fingerprint, and digest validation; synthetic nonzero untimed-source cases
  reject incorrect relabeling in both directions
- the IMV timing QA audit, sensitivity workbook, Figure S10 source/display
  files, and prespecified summary exist and contain aggregate data only
- the explicit IMV-ticket parity comparison passes for cohort membership,
  RFV1-RFV5 assignments, and the 14 prespecified existing workbooks after the
  captured baseline itself passes manifest-integrity validation
- an archive-enabled cohort smoke test confirms the in-memory-only untimed IMV
  source flag is absent from every optional row-level workbook export
- QA payloads land under `artifacts/qa/cohort/`, `artifacts/qa/rater_agreement/`, `artifacts/qa/analysis/`, and `artifacts/qa/baselines/`
- no generated outputs are written to the repository root or to `Drafts/`
- each renderable notebook remains self-contained at runtime and free of repo-local runtime imports
- the live numeric claims audit passes for the accepted Results directory

A valid public release branch additionally satisfies:

- no tracked row-level data, generated outputs, debug logs, local metadata, generated Office/PDF/image exports, or generated Excel workbooks
- no tracked `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, or `Legacy Code/`
- release assets, if distributed, are attached outside git with a manifest and checksums
