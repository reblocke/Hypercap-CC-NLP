# Hypercap-CC-NLP

> Quarto-first code to assemble a MIMIC-IV hypercapnia cohort, classify emergency department presenting concerns with NLP, evaluate annotation/rater agreement, and generate manuscript-facing tables and figures.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Current software release: journal-submission code snapshot `v0.1.1`
- Current manuscript preprint: *Emergency Department Presenting Concerns Among Admissions With Hypercapnia: A Retrospective NLP Study of MIMIC-IV*, medRxiv, 2026, DOI [10.64898/2026.07.03.26357242](https://doi.org/10.64898/2026.07.03.26357242); [repository PDF](preprint/2026.07.03.26357242v1.pdf)
- Related abstract: *C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV*, American Journal of Respiratory and Critical Care Medicine, 2026;212(Supplement_1), DOI [10.1093/ajrccm/aamag162.4737](https://doi.org/10.1093/ajrccm/aamag162.4737)
- Machine-readable index: [`llms.txt`](./llms.txt)
- Statistical environment: Python 3.11, Quarto, BigQuery-backed MIMIC-IV access

## Cite This Work
Please cite the GitHub release matching the code you used, the relevant MIMIC-IV resources, and the current medRxiv preprint and/or ATS abstract as appropriate when referring to the presented findings. Repository citation metadata is provided in [`CITATION.cff`](./CITATION.cff). The manuscript is currently a medRxiv preprint; no final journal article has been accepted or published.

Current related scholarly outputs:

- Merdad RH, Ramirez M, Christenson M, Pettine WW, Locke BW. **Emergency Department Presenting Concerns Among Admissions With Hypercapnia: A Retrospective NLP Study of MIMIC-IV.** medRxiv. 2026. DOI [10.64898/2026.07.03.26357242](https://doi.org/10.64898/2026.07.03.26357242). [Preprint record](https://www.medrxiv.org/content/10.64898/2026.07.03.26357242v1). [Repository PDF](preprint/2026.07.03.26357242v1.pdf).
- Merdad RH, Crawford M, Christenson M, Pettine W, Locke B. **C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV.** American Journal of Respiratory and Critical Care Medicine. 2026;212(Supplement_1). DOI [10.1093/ajrccm/aamag162.4737](https://doi.org/10.1093/ajrccm/aamag162.4737).

Key source-data citations:

- MIMIC-IV v3.1, DOI [10.13026/kpb9-mt58](https://doi.org/10.13026/kpb9-mt58).
- MIMIC-IV-ED v2.2, DOI [10.13026/5ntk-km72](https://doi.org/10.13026/5ntk-km72).

## Data Access And Ethics
This repository does not distribute row-level data, MIMIC-derived workbooks, annotation workbooks, or generated debug logs. The pipeline requires credentialed access to:

- MIMIC-IV HOSP and ICU on BigQuery
- the official MIMIC-IV derived concepts dataset on BigQuery for
  `mimiciv_derived.ventilation`
- MIMIC-IV-ED on BigQuery
- MIMIC-IV-Note only if note-based extensions are added

Researchers must obtain the required PhysioNet/MIMIC training, data-use approval, and any local institutional approvals before running the pipeline. See [`docs/DATA_ACCESS.md`](docs/DATA_ACCESS.md) for the full public data-access statement.

## Docs Map
- `README.md` is the public onboarding/runbook surface.
- [`docs/SPEC.md`](docs/SPEC.md) is the current pipeline contract: stage ownership, private handoffs, output locations, QA surfaces, runtime constraints, and acceptance checks.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) records dated rationale and superseded decisions.
- [`docs/MANUSCRIPT_MAPPING.md`](docs/MANUSCRIPT_MAPPING.md) maps manuscript tables/figures to notebook stages and generated assets.
- [`analysis_manifest.yml`](analysis_manifest.yml) freezes definition-only analysis rules used by the submission revision, including RFV taxonomy, gas thresholds, pH/HCO3 bands, and sensitivity definitions.
- [`data_dictionary.md`](data_dictionary.md) and [`data_dictionary.csv`](data_dictionary.csv) describe restricted source fields, derived NLP/cohort variables, benchmark metrics, and aggregate release assets without exposing row-level data.

## Quick Start

> Without the restricted MIMIC-derived input workbooks and credentials, you can inspect and test the code, but you cannot reproduce the cohort or manuscript results end to end.

### 1. Create The Environment

```bash
uv sync --frozen
```

This installs the locked spaCy English model used by the classifier. Verify the
model is loadable:

```bash
uv run python -c "import spacy; spacy.load('en_core_web_sm')"
```

Install the R packages used by the analysis CONSORT figure and chart-review
notebook:

```bash
make r-packages
```

Install/check Quarto for notebook rendering:

```bash
quarto --version
quarto install tinytex
quarto check
```

### 2. Configure Restricted Data Access

Copy the example environment file and fill in local project/auth settings:

```bash
cp .env.example .env
```

Required BigQuery settings for the default workflow:

```text
MIMIC_BACKEND=bigquery
WORK_PROJECT=<your-billing-project-id>
BQ_PHYSIONET_PROJECT=physionet-data
BQ_DATASET_HOSP=mimiciv_3_1_hosp
BQ_DATASET_ICU=mimiciv_3_1_icu
BQ_DATASET_ED=mimiciv_ed
BQ_DATASET_DERIVED=mimiciv_derived
```

`BQ_DATASET_DERIVED` names the release-managed derived dataset. The cohort
stage reads its `_metadata` attribute/value table and requires exactly one
`mimic_version = 3.1` record before querying `ventilation`; it does not query a
derived `bg` table or silently fall back to legacy regex timing.

Authenticate BigQuery access:

```bash
gcloud init
gcloud auth application-default login
gcloud services enable bigquery.googleapis.com --project <your-billing-project-id>
```

Optional strict classifier resource hashing:

```bash
cp Annotation/resource_manifest.example.json Annotation/resource_manifest.json
# Fill sha256 values, then run:
make check-resources
```

Do not commit `.env`, MIMIC exports, annotation workbooks, or generated outputs.

For split-machine runs, the cohort stage must execute on a machine whose
credentials can read HOSP, ICU, ED, and the official derived dataset. A separate
downstream machine can run the classifier and analysis only after receiving the
new private handoff workbook through an approved restricted-data channel. Git
sync alone does not transfer that ignored workbook and must not be used to do so.

## Pipeline

The canonical pipeline is Quarto-first and notebook-native:

1. `MIMICIV_hypercap_EXT_cohort.qmd` - cohort assembly from BigQuery-backed MIMIC-IV tables
2. `Hypercap CC NLP Classifier.qmd` - presenting-concern normalization/classification
3. `Rater Agreement Analysis.qmd` - adjudicator/rater agreement and NLP benchmark analyses
4. `Hypercap CC NLP Analysis.qmd` - merged manuscript analysis, figures, tables, and submission bundle

Run the full pipeline:

```bash
make quarto-pipeline RESULTS_DATE=$(date +%Y-%m-%d)
```

Run stages individually:

```bash
make quarto-cohort
make quarto-classifier
make quarto-rater
make quarto-analysis
make quarto-chart-review
```

Compatibility alias for older local commands:

```bash
make quarto-reyan-figures
```

Generated outputs are written locally under `Results/YYYY-MM-DD/` and are ignored by git.
QA/debug outputs are written locally under `artifacts/qa/...` and `debug/...` and are ignored by git.

## Expected Outputs

A successful private run produces:

- Four stage PDFs in `Results/YYYY-MM-DD/`
- Canonical private handoff workbooks under `MIMIC tabular data/`
- Manuscript tables and figures under `Results/YYYY-MM-DD/`
- A curated `Results/YYYY-MM-DD/submission_assets/` bundle with main figures `Figure 1`-`Figure 4`, supplement figures `Figure S1`-`Figure S10`, tables, source-data workbooks, and `submission_assets_manifest.csv`
- Run-level reviewer manifests `submission_manifest.xlsx`, `submission_manifest.csv`, and `OUTPUTS_README.md`
- Aggregate supplement-ready acid-base missingness, candidate-definition, and sensitivity-suite workbooks
- The aggregate IMV timing outputs `IMV_Qualifying_Gas_Timing_Sensitivity.xlsx`,
  `Figure S10.pdf`, `Figure S10.xlsx`, and
  `imv_timing_manuscript_summary.md`, plus the aggregate-only cohort QA file
  `artifacts/qa/cohort/imv_qualifying_gas_timing_audit.csv`

The IMV sensitivity compares observed timestamp order only. It does not alter
the primary cohort or existing primary results and does not establish that IMV
caused hypercapnia.

For public release, generated manuscript outputs should be attached as GitHub/Zenodo release assets, not tracked in git.

The `v0.1.1` release refreshes metadata for the verified ATS abstract while preserving the aggregate `v0.1.0` submission-assets bundle. Release asset checksums should validate from the downloaded filename, not from private local paths.

## Paper To Code Mapping

| Manuscript item | Producing notebook | Output |
|---|---|---|
| Cohort construction and NLP workflow | `Hypercap CC NLP Analysis.qmd` | `Figure 1.pdf` |
| Presenting-concern prevalence by overlapping ascertainment indicator | `Hypercap CC NLP Analysis.qmd` | `Figure 2.pdf`, `Figure 2.xlsx` |
| Presenting-concern prevalence by age group | `Hypercap CC NLP Analysis.qmd` | `Figure 3.pdf`, `Figure 3.xlsx` |
| Presenting-concern prevalence by acidemia severity | `Hypercap CC NLP Analysis.qmd` | `Figure 4.pdf`, `Figure 4.xlsx` |
| Main baseline characteristics | `Hypercap CC NLP Analysis.qmd` | `Table 1.xlsx` |
| Common presenting-concern categories | `Hypercap CC NLP Analysis.qmd` | `Table 2.xlsx` |
| IMV timing relative to the qualifying gas | `Hypercap CC NLP Analysis.qmd` | `Figure S10.pdf`, `Figure S10.xlsx`, `IMV_Qualifying_Gas_Timing_Sensitivity.xlsx` |
| Classifier supplement tables | `Hypercap CC NLP Classifier.qmd` | `NLP_Classifier_Supplement_Tables.xlsx` |
| Rater benchmark supplement tables | `Rater Agreement Analysis.qmd` | `Rater_Benchmark_Supplement_Tables.xlsx` |

See [`docs/MANUSCRIPT_MAPPING.md`](docs/MANUSCRIPT_MAPPING.md) for the fuller mapping.

## Quality Checks

Run code checks locally:

```bash
uv run pytest -q
uv run ruff check src tests
```

Equivalent Make targets:

```bash
make test
make lint
```

Dry-run the stage commands without executing restricted-data queries:

```bash
make -n quarto-pipeline
```

Full private reproducibility audit, after restricted inputs are available:

```bash
RUN_MANIFEST_REQUIRE_CLEAN_GIT=1 RESULTS_DATE=2026-04-29 make quarto-pipeline-audit
```

For the IMV ticket, capture a named pre-change private baseline and compare the
post-run outputs against that same explicit baseline:

```bash
uv run python scripts/imv_ticket_parity.py capture \
  --baseline <baseline-id-or-absolute-path> \
  --results-date <pre-ticket-results-date> \
  --source-commit <pre-ticket-commit>
uv run python scripts/imv_ticket_parity.py compare \
  --baseline <same-baseline-id-or-absolute-path> \
  --results-date <post-run-results-date>
```

This private QA control checks unchanged cohort membership, RFV1-RFV5 code and
label assignments, and 14 existing manuscript workbooks. Captured baselines and
results remain under ignored locations; the comparison emits an aggregate-only
JSON status report and does not export row-level differences.

## Repository Layout

```text
├── Annotation/                  # Public classifier resources only; private annotation workbooks are ignored
├── docs/                        # Specification, decisions, data-access, and manuscript mapping docs
├── preprint/                    # Public medRxiv preprint and citation details
├── scripts/                     # QA/reproducibility utility scripts
├── specs/                       # Versioned analysis specs such as blood-gas item IDs
├── src/hypercap_cc_nlp/         # QA/contracts/audit/parity helpers; notebooks remain runtime-self-contained
├── tests/                       # pytest coverage for helpers and static notebook contracts
├── *.qmd                        # Canonical Quarto pipeline notebooks
├── Makefile
├── pyproject.toml
├── uv.lock
├── CITATION.cff
├── LICENSE
└── README.md
```

Not tracked in the public repository: `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, legacy notebooks, local environment files, generated Office/PDF/image exports, and generated Excel workbooks.
Also not tracked: `CONTINUITY.md`, which is a local agent/session ledger rather than a public project artifact.

## Environment

- Python: `>=3.11,<3.12`
- Dependency lock: `pyproject.toml` + `uv.lock`
- Notebook interface: Quarto with Python execution
- External auth: Google Cloud SDK for BigQuery Application Default Credentials
- GPU: not required

## Funding, Conflicts, And Acknowledgements

Funding, conflict-of-interest, and acknowledgement text should follow the submitted manuscript. Keep this repository focused on reproducibility and do not place unpublished private disclosure documents in git.

## License

Code is released under the MIT License. Generated figures/tables are not tracked here and may be subject to author, journal, or repository release terms.

## Contributing, Conduct, And Security

Documentation and portability fixes are welcome. Do not submit PHI/PII, MIMIC-derived row-level data, annotation workbooks, credentials, or generated manuscript files through issues or pull requests. See:

- [`CONTRIBUTING.md`](./CONTRIBUTING.md)
- [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md)
- [`SECURITY.md`](./SECURITY.md)

## Maintainer

Brian W. Locke (`@reblocke`). Open a GitHub Issue for public code/documentation questions only.
