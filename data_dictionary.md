# Data Dictionary

This dictionary documents the restricted source data, private handoff workbooks,
derived variables, benchmark metrics, and aggregate release assets used by
`Hypercap-CC-NLP`. It is a public documentation surface only; it does not contain
row-level MIMIC data, annotation rows, encounter identifiers, chief complaint text,
or generated manuscript outputs.

The machine-readable version is [`data_dictionary.csv`](data_dictionary.csv).

## Source Data

The pipeline uses credentialed PhysioNet/BigQuery access to MIMIC-IV HOSP/ICU
v3.1 and MIMIC-IV-ED v2.2. These sources are restricted under PhysioNet access
rules and must not be exported into git. Current source citations are:

- MIMIC-IV v3.1, DOI `10.13026/kpb9-mt58`
- MIMIC-IV-ED v2.2, DOI `10.13026/5ntk-km72`

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

## Public Output Boundary

Aggregate figures, tables, and source-data workbooks may be distributed as release
assets only after privacy/small-cell review. Generated PDFs, Excel files, ZIPs,
debug reports, MIMIC-derived row-level data, and annotation workbooks should not
be committed to the repository.

## Fields Requiring Review

Rows marked `needs_review` in the CSV are inferred from code and manuscript
mapping rather than from a final journal data dictionary. Confirm exact age bands,
pH/acidemia bands, ICD-code definitions, classifier similarity interpretation,
and small-cell release policy before final public release.
