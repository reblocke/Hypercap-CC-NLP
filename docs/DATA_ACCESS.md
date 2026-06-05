# Data Access And Ethics

This repository intentionally excludes row-level data and private study artifacts.

## Restricted Inputs

The pipeline requires credentialed access to MIMIC-IV through PhysioNet/BigQuery:

- MIMIC-IV HOSP and ICU v3.1, DOI `10.13026/kpb9-mt58`
- MIMIC-IV-ED v2.2, DOI `10.13026/5ntk-km72`
- MIMIC-IV-Note only for future note-based extensions

Local runs also require private generated handoff workbooks under `MIMIC tabular data/` and, for the rater stage, the private adjudicated annotation workbook.

## Public Repository Boundary

The public repository must not contain:

- MIMIC-derived row-level exports
- patient or encounter identifiers
- annotation workbooks
- generated result folders
- debug logs or run manifests
- manuscript drafts or cover letters
- credentials or local environment files

Aggregate manuscript figures/tables may be distributed outside git as reviewed release assets with checksums and a manifest.

## Reproducibility Limit

Users without restricted data access can inspect code, run static/unit tests, and review the documented pipeline. They cannot reproduce the analytic cohort or manuscript estimates end to end without approved data access.

## Citation

Users should cite the relevant MIMIC-IV resources according to PhysioNet requirements, cite this repository release when referencing the analysis code, and cite the ATS 2026 abstract when referencing the presented findings:

- Merdad RH, Crawford M, Christenson M, Pettine W, Locke B. C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV. American Journal of Respiratory and Critical Care Medicine. 2026;212(Supplement_1). DOI `10.1093/ajrccm/aamag162.4737`.
