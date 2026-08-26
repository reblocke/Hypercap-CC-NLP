# Data Access And Ethics

This repository intentionally excludes row-level data and private study artifacts.

## Restricted Inputs

The pipeline requires credentialed access to MIMIC-IV through PhysioNet/BigQuery:

- MIMIC-IV HOSP and ICU v3.1, DOI `10.13026/kpb9-mt58`
- the official MIMIC-IV derived concepts dataset (default
  `mimiciv_derived`) for `_metadata` and `ventilation`
- MIMIC-IV-ED v2.2, DOI `10.13026/5ntk-km72`
- MIMIC-IV-Note only for future note-based extensions

The IMV timing sensitivity fails closed unless
`{BQ_PHYSIONET_PROJECT}.{BQ_DATASET_DERIVED}._metadata` contains exactly one
attribute/value record with `attribute = mimic_version` and `value = 3.1`, and
the official `ventilation` table is accessible. `BQ_DATASET_DERIVED` identifies
the derived dataset, not a blood-gas table: the pipeline does not query
`mimiciv_derived.bg` for this analysis and does not substitute the legacy broad
ventilation regex when the official source is unavailable.

The restricted MIMIC-IV 3.1 run verified on 2026-08-25 selected
`BQ_DATASET_DERIVED=mimiciv_3_1_derived` explicitly. Set this in the local
environment or `.env` when using that release-specific dataset; the runtime
default remains `mimiciv_derived`. Dataset naming does not replace permission
checks or the required metadata validation. A 403 from the default dataset
calls for checking the configured dataset and access, not a `bg` query or a
legacy timing fallback.

The ICU dataset used for the derived `stay_id` join must also resolve explicitly
to a versioned 3.1 alias; unversioned ICU fallback is rejected. Official derived
or procedure source records without any usable start time are retained only as
aggregate QA evidence and force an indeterminate temporal classification.

Local runs also require private generated handoff workbooks under `MIMIC tabular data/` and, for the rater stage, the private adjudicated annotation workbook.

## Public Repository Boundary

The public repository must not contain:

- MIMIC-derived row-level exports
- patient or encounter identifiers
- annotation workbooks
- generated result folders
- debug logs or run manifests
- credentials or local environment files

Aggregate manuscript figures/tables may be distributed outside git as reviewed release assets with checksums and a manifest.

## Split-Machine Runs

The cohort stage must run where the authorized user can query MIMIC-IV HOSP,
ICU, ED, and the official derived dataset. If later stages run on another
machine, the regenerated private cohort handoff must be transferred through an
institutionally approved restricted-data channel. Repository synchronization is
insufficient because handoff workbooks are ignored by git, and neither the
handoff nor any row-level IMV timestamps may be added to the public repository.

The receiving machine may run the classifier and analysis from the private
handoff without BigQuery access, but that is a split execution rather than an
end-to-end render on one machine. The run record should identify which machine
performed the credentialed cohort extraction and which handoff checksum was
used downstream.

## Reproducibility Limit

Users without restricted data access can inspect code, run static/unit tests, and review the documented pipeline. They cannot reproduce the analytic cohort, the IMV timing sensitivity, or manuscript estimates end to end without approved access to all configured sources.

## Citation

Users should cite the relevant MIMIC-IV resources according to PhysioNet requirements, cite the matching repository release (or the repository and exact commit SHA for unreleased code) when referencing the analysis code, and cite the current medRxiv preprint and/or ATS 2026 abstract as appropriate when referencing the study findings:

- Merdad RH, Ramirez M, Christenson M, Pettine WW, Locke BW. Emergency Department Presenting Concerns Among Admissions With Hypercapnia: A Retrospective NLP Study of MIMIC-IV. medRxiv. 2026. DOI `10.64898/2026.07.03.26357242`.
- Merdad RH, Crawford M, Christenson M, Pettine W, Locke B. C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV. American Journal of Respiratory and Critical Care Medicine. 2026;212(Supplement_1). DOI `10.1093/ajrccm/aamag162.4737`.
