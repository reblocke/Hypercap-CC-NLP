# Hypercap-CC-NLP

> Quarto-first code to assemble a MIMIC-IV hypercapnia cohort, classify emergency department presenting concerns with NLP, evaluate annotation/rater agreement, and generate manuscript-facing tables and figures.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Current software release: journal-submission code snapshot `v0.1.1`
- Current manuscript preprint: *Emergency Department Presenting Concerns Among Admissions With Hypercapnia: A Retrospective NLP Study of MIMIC-IV*, medRxiv, 2026, DOI [10.64898/2026.07.03.26357242](https://doi.org/10.64898/2026.07.03.26357242); [repository PDF](preprint/2026.07.03.26357242v1.pdf)
- Related abstract: *C75-09 Chief Complaint Profiles in Hypercapnic Respiratory Failure: A Natural Language Processing Study of MIMIC-IV*, American Journal of Respiratory and Critical Care Medicine, 2026;212(Supplement_1), DOI [10.1093/ajrccm/aamag162.4737](https://doi.org/10.1093/ajrccm/aamag162.4737)
- Machine-readable index: [`llms.txt`](./llms.txt)
- Statistical environment: Python 3.11, Quarto, BigQuery-backed MIMIC-IV access

The IMV timing sensitivity and acceptance safeguards described below are
unreleased changes; the published `v0.1.1` release does not include them.

## Choose a Route

| Task | Access and evidence | Start |
| --- | --- | --- |
| Inspect and test public code | No clinical data or BigQuery credentials; tests use public code and synthetic/static fixtures, not empirical reproduction | [Public quick start](#quick-start) and [quality checks](#quality-checks) |
| Run the four-stage cohort pipeline | Authorized MIMIC/BigQuery access, private handoffs and Quarto/R tools; writes ignored clinical outputs | [Restricted setup](#2-configure-restricted-data-access), [pipeline](#pipeline), [SPEC](docs/SPEC.md) and [data access](docs/DATA_ACCESS.md) |
| Continue from a private split-machine handoff | Institutionally approved transfer of workbook, IMV sidecar and producer manifests with matching SHA-256; Git is insufficient | [Handoff rules](#2-configure-restricted-data-access) and [SPEC](docs/SPEC.md#private-handoffs-and-local-outputs) |
| Locate manuscript and release evidence | Public v0.1.1 and preprint/ATS sources differ from unreleased IMV changes and author drafts | [Citation](#cite-this-work), [exhibit map](docs/MANUSCRIPT_MAPPING.md), [definitions](analysis_manifest.yml), [numeric claims](docs/NUMERIC_CLAIMS.yml) and [working drafts](working-drafts/README.md) |

## Cite This Work

Please cite the GitHub release matching the code you used, or the repository and
exact commit SHA for unreleased code, together with the relevant MIMIC-IV
resources and the current medRxiv preprint and/or ATS abstract as appropriate
when referring to the presented findings. Repository citation metadata is
provided in [`CITATION.cff`](./CITATION.cff); its version and release date describe
the published software release, not subsequent unreleased commits. The manuscript
is currently a medRxiv preprint; no final journal article has been accepted or
published.

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
  the configured dataset's `_metadata` and `ventilation` tables
- MIMIC-IV-ED on BigQuery
- MIMIC-IV-Note only if note-based extensions are added

Researchers must obtain the required PhysioNet/MIMIC training, data-use approval, and any local institutional approvals before running the pipeline. See [`docs/DATA_ACCESS.md`](docs/DATA_ACCESS.md) for the full public data-access statement.

## Docs Map
- `README.md` is the public onboarding/runbook surface.
- [`working-drafts/`](working-drafts/README.md) contains shared author Word drafts
  for cross-machine editing, with status notices and file checksums.
- [`docs/SPEC.md`](docs/SPEC.md) is the current pipeline contract: stage ownership, private handoffs, output locations, QA surfaces, runtime constraints, and acceptance checks.
- [`docs/DECISIONS.md`](docs/DECISIONS.md) records dated rationale and superseded decisions.
- [`docs/MANUSCRIPT_MAPPING.md`](docs/MANUSCRIPT_MAPPING.md) maps manuscript tables/figures to notebook stages and generated assets.
- [`analysis_manifest.yml`](analysis_manifest.yml) freezes definition-only analysis rules used by the submission revision, including RFV taxonomy, gas thresholds, pH/HCO3 bands, and sensitivity definitions.
- [`docs/NUMERIC_CLAIMS.yml`](docs/NUMERIC_CLAIMS.yml) records source-addressed aggregate values from the accepted run and the active nonhistorical surfaces where repeated values are allowed.
- [`data_dictionary.md`](data_dictionary.md) and [`data_dictionary.csv`](data_dictionary.csv) describe restricted source fields, derived NLP/cohort variables, benchmark metrics, and aggregate release assets without exposing row-level data.

## Quick Start

> Without the restricted MIMIC-derived input workbooks and credentials, you can inspect and test the code, but you cannot reproduce the cohort or manuscript results end to end.

### 1. Inspect And Test Public Code

```bash
uv sync --frozen
uv run pytest -q
uv run ruff check src tests
```

Run from the repository root with Python 3.11 and `uv`. The frozen sync may
download packages, including the spaCy English model wheel, and writes a local
environment; these tests use public source and synthetic/static fixtures and
may write ignored caches. They do not query MIMIC or reproduce the cohort,
annotation agreement or manuscript estimates. BigQuery authentication, R
graphics packages, TinyTeX and a user Jupyter kernel are not prerequisites
for these commands. See [Quality Checks](#quality-checks) for the static
numeric-claims check and private-only audit routes.

### Restricted Rendering Prerequisites

For an authorized classifier/pipeline run, verify the installed spaCy model:

```bash
uv run python -c "import spacy; spacy.load('en_core_web_sm')"
```

The analysis and optional chart-review Make targets invoke `make r-packages`
as a prerequisite. Install/check the R packages for those stages only:

```bash
make r-packages
```

Quarto and TinyTeX are rendering prerequisites, not public test requirements:

```bash
quarto --version
quarto install tinytex
quarto check
```

### 2. Configure Restricted Data Access

This route requires authorized MIMIC/BigQuery access on the cohort machine.
Follow [local credentialed setup](docs/DATA_ACCESS.md#local-credentialed-setup)
for the ignored `.env`, dataset settings and application-default credentials;
[the pipeline contract](docs/SPEC.md) defines the stage inputs and outputs.
The runtime derived-dataset default is `mimiciv_derived`. The verified
2026-08-25 MIMIC-IV 3.1 run explicitly used
`BQ_DATASET_DERIVED=mimiciv_3_1_derived` instead. Whichever official
derived dataset is selected, the cohort stage requires exactly one
`mimic_version = 3.1` record in its `_metadata` attribute/value table
and access to `ventilation`; it does not query derived `bg` or fall back
to legacy regex timing. A 403 calls for checking the selected dataset and
permissions, not bypassing validation. Do not commit credentials or data.

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
new private handoff workbook, required `MIMICIV IMV source provenance.json`
sidecar, and producing manifests through an approved restricted-data channel.
Verify source/destination SHA-256 equality for these files. Git sync alone does
not transfer ignored restricted inputs and must not be used to do so.

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

Run this from the repository root only after the restricted setup above, with
authorized BigQuery access and the required local private inputs. The Make
targets enforce the stage workbook/PDF presence checks; inspect the new
`Results/YYYY-MM-DD/` PDFs, private handoff workbooks, IMV provenance sidecar
and producer manifests against [SPEC](docs/SPEC.md#private-handoffs-and-local-outputs)
before treating a run as complete. File presence alone does not establish
source validity, QA acceptance or manuscript reproduction. The cohort stage
must pass the official derived-dataset version/ventilation checks; a 403 or
mismatched `_metadata` record is a stop, not a fallback.

Run stages individually:

```bash
make quarto-cohort
make quarto-classifier
make quarto-rater
make quarto-analysis
```

The four stages must remain in that order. `make quarto-chart-review` is an
optional supporting render with R packages and
`Results/YYYY-MM-DD/Chart Review Sample Calc.html` as its checked local
output, not a fifth core stage:

```bash
make quarto-chart-review
```

Compatibility alias for older local commands:

```bash
make quarto-reyan-figures
```

That alias renders the analysis notebook and checks its PDF plus representative
figure files; it is not another core stage. See [SPEC](docs/SPEC.md) for its
input/output contract.

Generated outputs are written locally under `Results/YYYY-MM-DD/` and are ignored by git.
QA/debug outputs are written locally under `artifacts/qa/...` and `debug/...` and are ignored by git.

## Expected Outputs

A successful private run produces:

- Four stage PDFs in `Results/YYYY-MM-DD/`
- Canonical private handoff workbooks and the required private
  `MIMICIV IMV source provenance.json` sidecar under `MIMIC tabular data/`
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

Check repeated aggregate claims without private results, then optionally reconcile
the accepted local run and publication-figure sample-size labels:

```bash
make numbers-check
make numbers-check-live RESULTS_DATE=2026-08-25
```

The static check excludes manuscript/preprint files and historical evidence. The
live check reads aggregate workbooks, QA summaries, manifests, and publication
PDF text only; it never opens row-level MIMIC or annotation handoffs. A mismatch
in a preserved Results directory requires producer investigation and a new dated
run rather than hand-editing the accepted output.

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

Capture creates a fresh requested baseline target; do not overwrite or recapture
an existing baseline. New captures require clean producer manifests at the
resolved `--source-commit`, with linked input/output hashes for both handoffs
and all 14 workbooks. The checkout may differ from that producing commit, but
the declared SHA alone is insufficient. Outputs from revisions without those
sealed manifests cannot be newly captured; retain the original capture instead
of retrospectively labeling outputs. Existing schema-v1 captures remain usable
with `legacy_unverified` producer provenance reported explicitly.

This private QA control checks unchanged cohort membership, RFV1-RFV5 code and
label assignments, and 14 existing manuscript workbooks. Before current outputs
are compared, every captured baseline copy is checked against its recorded
manifest row counts, semantic hashes, workbook inventory, and sheet signatures;
altered or incomplete baselines fail closed. Only exact allowlisted
documentation sheets are warning-eligible. Captured baselines and results remain
under ignored locations; the comparison emits an aggregate-only JSON status
report and does not export row-level differences.

Analysis also requires the cohort-produced private IMV provenance sidecar, even
when its untimed-source membership is empty. It verifies the source fingerprint
and evidence membership before accepting timing strata; an absent or mismatched
sidecar is an input failure, not permission to infer evidence from a supplied
stratum. See [`docs/SPEC.md`](docs/SPEC.md) for the normative contracts.

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
