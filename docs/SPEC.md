# Pipeline Specification

This document is the canonical current-state contract for the pipeline. Use [`README.md`](../README.md) for public setup/runbook guidance and [`docs/DECISIONS.md`](DECISIONS.md) for dated rationale and superseded decisions.

## Public Release Boundary

The public repository contains source code, specifications, tests, and documentation only. It must not track row-level MIMIC-derived data, annotation workbooks, generated results, draft manuscripts, debug logs, local environment files, or release asset zips.

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
| `MIMICIV_hypercap_EXT_cohort.qmd` | BigQuery-backed MIMIC-IV HOSP/ICU/ED data plus local `.env` settings | `MIMIC tabular data/MIMICIV all with CC.xlsx`; dated manifests/audits under `MIMIC tabular data/prior runs/`; QA payloads under `artifacts/qa/cohort/` |
| `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/MIMICIV all with CC.xlsx` unless overridden by `CLASSIFIER_INPUT_FILENAME` | `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`; dated manifests/audits under `MIMIC tabular data/prior runs/` |
| `Rater Agreement Analysis.qmd` | Canonical NLP workbook plus private annotation workbook (`RATER_ANNOTATION_PATH` when overridden) | Agreement audits and summaries under `artifacts/qa/rater_agreement/` |
| `Hypercap CC NLP Analysis.qmd` | `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx` unless overridden by `ANALYSIS_INPUT_FILENAME` | Figures, tables, PDFs, submission bundle, and analysis exports under `Results/YYYY-MM-DD/`; QA checks under `artifacts/qa/analysis/` |
| `Chart Review Sample Calc.qmd` | Local R package environment plus notebook inputs | `Results/YYYY-MM-DD/Chart Review Sample Calc.html` and `Results/YYYY-MM-DD/Chart Review Sample Calc_files/` |

Pipeline order is `cohort -> classifier -> rater -> analysis`. The analysis stage's statistical estimates come from the canonical NLP workbook. The clean submission bundle copies selected publication-facing assets into `Results/YYYY-MM-DD/submission_assets/` and writes `submission_asset_manifest.csv`. Direct analysis-stage renders must not fail solely because optional upstream supplement workbooks are absent; missing optional upstream assets are recorded under `artifacts/qa/analysis/submission_asset_optional_missing.csv`.

Manual annotation workbook curation remains private and independent. The public `Annotation/` directory contains only classifier resource CSVs and the example resource manifest.

## Output Contract

Generated outputs are local/private by default:

- stage PDFs: `Results/YYYY-MM-DD/MIMICIV_hypercap_EXT_cohort.pdf`, `Hypercap CC NLP Classifier.pdf`, `Rater Agreement Analysis.pdf`, and `Hypercap CC NLP Analysis.pdf`
- chart-review render: `Results/YYYY-MM-DD/Chart Review Sample Calc.html` plus its HTML bundle
- manuscript assets: `Figure 1.pdf`, `Figure 2-4.pdf/.xlsx`, `Table 1-2.xlsx`, and supplement aliases `Figure S1-S8.pdf/.xlsx` as produced by the analysis notebook
- submission bundle: `Results/YYYY-MM-DD/submission_assets/`
- QA/debug/manifests: `artifacts/qa/cohort/`, `artifacts/qa/rater_agreement/`, `artifacts/qa/analysis/`, `artifacts/qa/baselines/`, and `debug/...`
- private handoff workbooks: `MIMIC tabular data/MIMICIV all with CC.xlsx` and `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`

`Drafts/` is manual-only working space and must not be modified by automated render or migration steps. Public release branches must not include `Drafts/`, `Results/`, `MIMIC tabular data/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, or `Legacy Code/`.

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
| `RESULTS_DATE` | Selects the dated output folder under `Results/YYYY-MM-DD/` |
| `RESULTS_DIR` | Overrides the concrete results path; defaults to `Results/<date>` |
| `CLASSIFIER_INPUT_FILENAME` | Overrides the classifier's cohort-workbook handoff input |
| `RATER_NLP_INPUT_FILENAME` | Overrides the rater stage NLP-workbook handoff input |
| `RATER_ANNOTATION_PATH` | Overrides the annotation workbook used by the rater stage |
| `ANALYSIS_INPUT_FILENAME` | Overrides the analysis stage NLP-workbook handoff input |
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

## Stage-Owned Invariants

The cohort-stage blood-gas item selection is versioned in `specs/blood_gas_itemids.json`.

Current cohort-stage invariants:

- ABG/VBG/UNKNOWN are the canonical source classes for definitive pCO2 values.
- UNKNOWN remains cohort-eligible for threshold inclusion.
- Gas qualification for enrollment is any-time during stay via `pco2_threshold_any`; `pco2_threshold_0_24h` is retained as a timing marker only.
- POC itemid QC telemetry does not itself gate cohort enrollment logic.
- Gas-source row diagnostics are written to `artifacts/qa/cohort/gas_source_diagnostics_by_ed_stay.csv`.
- Canonical cohort export fields use `qualifying_pco2_mmhg` rather than `first_pco2`.
- The first-by-site pO2 exports are `first_abg_po2`, `first_vbg_po2`, and `first_other_po2`.
- `first_hco3` is selected by qualifying panel bicarbonate first, then nearest serum bicarbonate/total CO2 fallback.

Anthropometric cleaning invariants:

- BMI `(10, 100]`
- height `[100, 230]` cm
- weight `(25, 400]` kg
- BMI is computed from cleaned height + weight only when recorded BMI is missing
- recorded-vs-computed BMI consistency is audited but does not overwrite a valid recorded BMI

## Acceptance Checks

A valid private pipeline run satisfies all of the following:

- `make quarto-pipeline RESULTS_DATE=<date>` renders the four stage PDFs into `Results/<date>/`
- `make quarto-chart-review RESULTS_DATE=<date>` renders the chart-review HTML bundle into `Results/<date>/`
- private handoff workbooks exist at `MIMIC tabular data/MIMICIV all with CC.xlsx` and `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- QA payloads land under `artifacts/qa/cohort/`, `artifacts/qa/rater_agreement/`, `artifacts/qa/analysis/`, and `artifacts/qa/baselines/`
- no generated outputs are written to the repository root or to `Drafts/`
- each renderable notebook remains self-contained at runtime and free of repo-local runtime imports

A valid public release branch additionally satisfies:

- no tracked row-level data, generated outputs, drafts, debug logs, local metadata, Office/PDF/image exports, or generated Excel workbooks
- no tracked `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, or `Legacy Code/`
- release assets, if distributed, are attached outside git with a manifest and checksums
