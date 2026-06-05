# Hypercap-CC-NLP

> Quarto-first code to assemble a MIMIC-IV hypercapnia cohort, classify emergency department presenting concerns with NLP, evaluate annotation/rater agreement, and generate manuscript-facing tables and figures.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Release intent: journal-submission code snapshot `v0.1.1`
- Related abstract: *C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV*, American Journal of Respiratory and Critical Care Medicine, 2026;212(Supplement_1), DOI [10.1093/ajrccm/aamag162.4737](https://doi.org/10.1093/ajrccm/aamag162.4737)
- Machine-readable index: [`llms.txt`](./llms.txt)
- Statistical environment: Python 3.11, Quarto, BigQuery-backed MIMIC-IV access

## Cite This Work
Please cite the GitHub release matching the code you used, the relevant MIMIC-IV resources, and the ATS abstract when referring to the presented findings. Repository citation metadata is provided in [`CITATION.cff`](./CITATION.cff). Do not cite this repository as a final journal article until an accepted manuscript record exists.

Current related scholarly output:

- Merdad RH, Crawford M, Christenson M, Pettine W, Locke B. **C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV.** American Journal of Respiratory and Critical Care Medicine. 2026;212(Supplement_1). DOI [10.1093/ajrccm/aamag162.4737](https://doi.org/10.1093/ajrccm/aamag162.4737).

Key source-data citations:

- MIMIC-IV v3.1, DOI [10.13026/kpb9-mt58](https://doi.org/10.13026/kpb9-mt58).
- MIMIC-IV-ED v2.2, DOI [10.13026/5ntk-km72](https://doi.org/10.13026/5ntk-km72).

## Data Access And Ethics
This repository does not distribute row-level data, MIMIC-derived workbooks, annotation workbooks, generated debug logs, or draft manuscript files. The pipeline requires credentialed access to:

- MIMIC-IV HOSP and ICU on BigQuery
- MIMIC-IV-ED on BigQuery
- MIMIC-IV-Note only if note-based extensions are added

Researchers must obtain the required PhysioNet/MIMIC training, data-use approval, and any local institutional approvals before running the pipeline. See [`docs/DATA_ACCESS.md`](docs/DATA_ACCESS.md) for the full public data-access statement.

## Docs Map
- `README.md` is the public onboarding/runbook surface.
- [`docs/SPEC.md`](docs/SPEC.md) is the current pipeline contract: stage ownership, private handoffs, output locations, QA surfaces, runtime constraints, and acceptance checks.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) records dated rationale and superseded decisions.
- [`docs/MANUSCRIPT_MAPPING.md`](docs/MANUSCRIPT_MAPPING.md) maps manuscript tables/figures to notebook stages and generated assets.
- [`data_dictionary.md`](data_dictionary.md) and [`data_dictionary.csv`](data_dictionary.csv) describe restricted source fields, derived NLP/cohort variables, benchmark metrics, and aggregate release assets without exposing row-level data.

## Quick Start

> Without the restricted MIMIC-derived input workbooks and credentials, you can inspect and test the code, but you cannot reproduce the cohort or manuscript results end to end.

### 1. Create The Environment

```bash
uv sync --frozen
```

Install the spaCy English model used by the classifier:

```bash
./.venv/bin/python -m spacy download en_core_web_sm
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
```

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
- A curated `Results/YYYY-MM-DD/submission_assets/` bundle with main/supplement figures from `Figure 1` through `Figure S1` and later supplement items, tables, source-data workbooks, and `submission_asset_manifest.csv`

For public release, generated manuscript outputs should be attached as GitHub/Zenodo release assets, not tracked in git.

The `v0.1.1` release refreshes metadata for the verified ATS abstract while preserving the aggregate `v0.1.0` submission-assets bundle. Release asset checksums should validate from the downloaded filename, not from private local paths.

## Paper To Code Mapping

| Manuscript item | Producing notebook | Output |
|---|---|---|
| Cohort construction and NLP workflow | `Hypercap CC NLP Analysis.qmd` | `Figure 1.pdf` |
| Presenting-concern prevalence by ascertainment route | `Hypercap CC NLP Analysis.qmd` | `Figure 2.pdf`, `Figure 2.xlsx` |
| Presenting-concern prevalence by age group | `Hypercap CC NLP Analysis.qmd` | `Figure 3.pdf`, `Figure 3.xlsx` |
| Presenting-concern prevalence by acidemia severity | `Hypercap CC NLP Analysis.qmd` | `Figure 4.pdf`, `Figure 4.xlsx` |
| Main baseline characteristics | `Hypercap CC NLP Analysis.qmd` | `Table 1.xlsx` |
| Common presenting-concern categories | `Hypercap CC NLP Analysis.qmd` | `Table 2.xlsx` |
| Classifier supplement tables | `Hypercap CC NLP Classifier.qmd` | `NLP_Classifier_Supplement_Tables.xlsx` |
| Rater benchmark supplement tables | `Rater Agreement Analysis.qmd` | `Rater_Benchmark_Supplement_Tables.xlsx` |

See [`docs/MANUSCRIPT_MAPPING.md`](docs/MANUSCRIPT_MAPPING.md) for the fuller mapping.

## Quality Checks

Run code checks locally:

```bash
uv run pytest -q
uv run --with ruff ruff check src tests
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

## Repository Layout

```text
├── Annotation/                  # Public classifier resources only; private annotation workbooks are ignored
├── docs/                        # Specification, decisions, data-access, and manuscript mapping docs
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

Not tracked in the public repository: `MIMIC tabular data/`, `Drafts/`, `Results/`, `artifacts/`, `debug/`, `outputs/`, `tmp/`, legacy notebooks, local environment files, Office/PDF/image exports, and generated Excel workbooks.
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
