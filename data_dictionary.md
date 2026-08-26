# Data Dictionary

This dictionary documents the restricted source data, private handoff workbooks,
derived variables, benchmark metrics, and aggregate release assets used by
`Hypercap-CC-NLP`. It is a public documentation surface only; it does not contain
row-level MIMIC data, annotation rows, encounter identifiers, chief complaint text,
or generated manuscript outputs.

The machine-readable version is [`data_dictionary.csv`](data_dictionary.csv).

## Source Data

The pipeline uses credentialed PhysioNet/BigQuery access to MIMIC-IV HOSP/ICU
v3.1, the official MIMIC-IV derived concepts dataset, and MIMIC-IV-ED v2.2.
These sources are restricted under PhysioNet access rules and must not be
exported into git. Current source citations are:

- MIMIC-IV v3.1, DOI `10.13026/kpb9-mt58`
- MIMIC-IV-ED v2.2, DOI `10.13026/5ntk-km72`

The IMV timing analysis uses only the official derived `_metadata` and
`ventilation` tables. The `_metadata` attribute/value record must report
`mimic_version = 3.1`; `ventilation` is filtered to `InvasiveVent`. A derived
`bg` table is not an input to this analysis.

MIMIC-IV-Note is not required for the current release unless future note-based
extensions are added.

## Private Handoffs

The canonical private workbooks are:

- `MIMIC tabular data/MIMICIV all with CC.xlsx`
- `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- optional direct annotation benchmark outputs:
  `MIMIC tabular data/annotation_benchmark_with_NLP.xlsx`,
  `MIMIC tabular data/annotation_visit_candidate_scores.csv`, and
  `MIMIC tabular data/annotation_segment_candidate_scores.csv`

These workbooks include restricted row-level encounter identifiers, chief
complaint text, blood gas variables, and derived NLP outputs. They are ignored by
git and required only for authorized local reproduction.

## Key Derived Fields

- Cohort flags: `pco2_threshold_any`, `pco2_threshold_0_24h`,
  `qualifying_pco2_mmhg`, `gas_source`, and route-specific indicators for ICD,
  ABG, and VBG ascertainment.
- Paired qualifying-gas pH audit fields: `qualifying_ph`,
  `qualifying_ph_time`, `qualifying_ph_source_branch`, `qualifying_ph_site`, and
  `qualifying_ph_pairing_status`, used to assess whether pH from the same
  specimen/panel as the earliest qualifying PCO2 is complete enough for stricter
  acidemia analyses.
- Classifier outputs: `RFV1` through `RFV5`, corresponding `_name`, `_support`,
  and `_sim` columns, plus segment-level prediction payloads.
- Analysis strata: age groups, acidemia severity groups, ascertainment routes,
  and grouped presenting-concern categories.
- Benchmark metrics: mean F1, Cohen's kappa, exact agreement, and partial
  agreement against adjudicated human labels. Direct annotation benchmarking uses
  `annotation_row_id`, a deterministic non-MIMIC row key, to align adjudicated
  labels with classifier predictions across the full private annotation sample.

### Private IMV timing fields

The following eleven ticket-defined fields are authoritative and marked
`reviewed` in the CSV. They exist at admission level only in the ignored private
handoffs.

| Field | Exact type | Allowed and missing values | Derivation and validation |
|---|---|---|---|
| `first_derived_imv_starttime` | `datetime64[ns]` | Timestamp or `NaT` | Minimum official `InvasiveVent.starttime` by `hadm_id` after `stay_id` linkage; must equal the admission minimum. |
| `first_intubation_procedure_time` | `datetime64[ns]` | Timestamp or `NaT` | Minimum ICU procedure `starttime` for item `224385`, after the normalized `Intubation` label is validated. |
| `first_invasive_ventilation_procedure_time` | `datetime64[ns]` | Timestamp or `NaT` | Minimum ICU procedure `starttime` for item `225792`, after the normalized `Invasive Ventilation` label is validated. |
| `first_observed_imv_time` | `datetime64[ns]` | Timestamp or `NaT` | Minimum nonmissing robust source time; missing exactly when all three source times are missing. |
| `first_observed_imv_source` | pandas `string` | `intubation_procedure`, `invasive_ventilation_procedure`, `derived_ventilation_episode`, `multiple_sources_same_time`, or `missing`; never blank | Source of the exact minimum; tied minima use `multiple_sources_same_time`. |
| `robust_imv_observed` | pandas `Int64` | `0` or `1`; never missing | `1` when any robust source time is present and `0` otherwise; must agree with observed-time availability. |
| `imv_qualifying_gas_order` | pandas `string` | `not_applicable_no_qualifying_gas`, `no_observed_imv`, `qualifying_gas_before_imv`, `imv_before_qualifying_gas`, or `timing_indeterminate`; never blank | Strict comparison with `qualifying_pco2_time`, without rounding or a buffer. Exact ties, official IMV source records with no usable timestamp, and other unreliable/missing timing are indeterminate; ICD-only rows are not applicable. |
| `imv_preceded_qualifying_gas` | pandas nullable `boolean` | `True`, `False`, or `NA` | True only for IMV before gas; false for no observed IMV or gas before IMV; otherwise missing. |
| `no_prior_observed_imv` | pandas nullable `boolean` | `True`, `False`, or `NA` | True for no observed IMV or gas before IMV; false for IMV before gas; otherwise missing. |
| `hours_from_imv_to_qualifying_gas` | `float64` | Any finite real number or `NaN`; hours | `(qualifying_pco2_time - first_observed_imv_time)` in hours. Negative means gas first, positive means IMV first, and zero is an indeterminate exact tie. |
| `legacy_imv_timing_discordant` | pandas `Int64` | `0` or `1`; never missing | `1` for robust-versus-legacy presence disagreement or different strict before/after/tie relations when both timestamps are comparable; missing legacy time alone is not discordance. |

Among gas-positive admissions, `no_observed_imv`,
`qualifying_gas_before_imv`, `imv_before_qualifying_gas`, and
`timing_indeterminate` must be mutually exclusive and exhaustive. The first two
form the no-prior-observed-IMV sensitivity cohort. This ordering is descriptive
and cannot establish ventilator-induced or otherwise causal hypercapnia.
The untimed-official-source evidence flag is asserted inside the cohort stage
and is absent from every workbook export. Its source-derived admission
membership is retained separately in the required private
`MIMICIV IMV source provenance.json` sidecar, with a source-projection fingerprint
and payload digest. Analysis verifies that evidence before reconstructing the
temporal stratum; a supplied `timing_indeterminate` label is not itself evidence.
Both incorrect no-observed-to-indeterminate and indeterminate-to-no-observed
relabeling are rejected. The sidecar does not add a twelfth workbook field and
must remain restricted and excluded from public/submission/release artifacts.

## Public Output Boundary

Aggregate figures, tables, and source-data workbooks may be distributed as release
assets only after privacy/small-cell review. Generated PDFs, Excel files, ZIPs,
debug reports, MIMIC-derived row-level data, and annotation workbooks should not
be committed to the repository.

Ticket-defined aggregate outputs are:

- `artifacts/qa/cohort/imv_qualifying_gas_timing_audit.csv` (aggregate cohort
  and source/timing QA);
- `Results/YYYY-MM-DD/IMV_Qualifying_Gas_Timing_Sensitivity.xlsx` (seven
  specified aggregate sheets);
- `Results/YYYY-MM-DD/Figure S10.pdf` and `Figure S10.xlsx` (four temporal
  strata with patient-cluster bootstrap 95% confidence intervals); and
- `Results/YYYY-MM-DD/imv_timing_manuscript_summary.md` (prespecified aggregate
  summary and neutral rule-generated interpretation).

None may contain `subject_id`, `hadm_id`, `ed_stay_id`, raw chief-complaint
text, or other row-level MIMIC data.

## Fields Requiring Review

Rows marked `needs_review` in the CSV are inferred from code and manuscript
mapping rather than from a final journal data dictionary. Confirm exact age bands,
pH/acidemia bands, ICD-code definitions, classifier similarity interpretation,
and small-cell release policy before final public release.
