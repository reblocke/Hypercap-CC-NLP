# Data Access And Ethics

This repository intentionally excludes row-level data and private study artifacts.

## Restricted Inputs

The pipeline requires credentialed access to MIMIC-IV through PhysioNet/BigQuery:

- MIMIC-IV HOSP and ICU v3.1, DOI `10.13026/kpb9-mt58`
- the official MIMIC-IV derived concepts dataset (default
  `mimiciv_derived`) for `_metadata` and `ventilation`
- MIMIC-IV-ED v2.2, DOI `10.13026/5ntk-km72`
- MIMIC-IV-Note only for future note-based extensions

### Local credentialed setup

From the repository root, after obtaining PhysioNet/MIMIC training and local
institutional approval, copy `.env.example` to ignored `.env` and set the
local project/auth values. The standard configuration is:

```text
MIMIC_BACKEND=bigquery
WORK_PROJECT=<your-billing-project-id>
BQ_PHYSIONET_PROJECT=physionet-data
BQ_DATASET_HOSP=mimiciv_3_1_hosp
BQ_DATASET_ICU=mimiciv_3_1_icu
BQ_DATASET_ED=mimiciv_ed
BQ_DATASET_DERIVED=mimiciv_derived
```

`BQ_DATASET_DERIVED=mimiciv_derived` is the runtime default; the verified
2026-08-25 MIMIC-IV 3.1 run explicitly used
`BQ_DATASET_DERIVED=mimiciv_3_1_derived`. Select an authorized official
derived dataset in the local environment; naming alone does not pass the
metadata/version and `ventilation` checks described below. For BigQuery
application-default credentials, use the following only on an authorized
machine with the Google Cloud CLI installed and an approved billing project.
They change the machine's active Cloud CLI configuration and store
application-default credentials locally; they do not run the cohort or enable
project services. Successful setup reports an authenticated account. An
authentication or permission error is a stop: resolve it with the data owner
rather than bypassing dataset checks. The project administrator must separately
confirm that the BigQuery API is enabled for the approved billing project.

```bash
gcloud init
gcloud auth application-default login
```

The billing project needs access to HOSP, ICU, ED, and the selected derived
`_metadata` and `ventilation` tables. A successful login or API enablement
does not itself prove that table access or the required source version is
valid. Do not commit `.env` or credentials.

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
or procedure source records without any usable start time force an indeterminate
temporal classification. Source-derived admission membership is preserved in a
required private `MIMICIV IMV source provenance.json` sidecar so analysis can
verify this evidence; only its aggregate counts may enter manuscript outputs.

Local runs also require private generated handoff workbooks under `MIMIC tabular data/` and, for the rater stage, the private adjudicated annotation workbook.

## Public Repository Boundary

The public repository must not contain:

- MIMIC-derived row-level exports or source-provenance sidecars
- patient or encounter identifiers
- annotation workbooks
- generated result folders
- debug logs or run manifests
- credentials or local environment files

Aggregate manuscript figures/tables may be distributed outside git as reviewed release assets with checksums and a manifest.

Author-approved Word drafts in `working-drafts/` are a documented exception for
cross-machine collaboration. These files are public, including their comments
and tracked changes, and must not contain the restricted material listed above
or private reviewer material. Each draft has a status notice and SHA-256 in
`working-drafts/README.md`. This exception does not permit tracking generated
result folders or private handoffs.

## Split-Machine Runs

The cohort stage must run where the authorized user can query MIMIC-IV HOSP,
ICU, ED, and the official derived dataset. If later stages run on another
machine, the regenerated private cohort handoff, required
`MIMICIV IMV source provenance.json` sidecar, and producing manifests must be
transferred together through an institutionally approved restricted-data
channel. Verify source/destination SHA-256 equality. Repository synchronization
is insufficient because these files are ignored by git, and neither the
handoff, sidecar, nor any row-level IMV timestamps may be added to the public
repository or submission/release assets.

The receiving machine may run the classifier and analysis from the private
handoff and sidecar without BigQuery access, but that is a split execution rather than an
end-to-end render on one machine. The run record should identify which machine
performed the credentialed cohort extraction and which handoff and sidecar
checksums were used downstream. Sidecar digests check integrity against the
approved source transfer; they are not independent proof of source authenticity.

## Reproducibility Limit

Users without restricted data access can inspect code, run static/unit tests, and review the documented pipeline. They cannot reproduce the analytic cohort, the IMV timing sensitivity, or manuscript estimates end to end without approved access to all configured sources.

## Citation

Users should cite the relevant MIMIC-IV resources according to PhysioNet requirements, cite the matching repository release (or the repository and exact commit SHA for unreleased code) when referencing the analysis code, and cite the current medRxiv preprint and/or ATS 2026 abstract as appropriate when referencing the study findings:

- Merdad RH, Ramirez M, Christenson M, Pettine WW, Locke BW. Emergency Department Presenting Concerns Among Admissions With Hypercapnia: A Retrospective NLP Study of MIMIC-IV. medRxiv. 2026. DOI `10.64898/2026.07.03.26357242`.
- Merdad RH, Crawford M, Christenson M, Pettine W, Locke B. C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV. American Journal of Respiratory and Critical Care Medicine. 2026;212(Supplement_1). DOI `10.1093/ajrccm/aamag162.4737`.
