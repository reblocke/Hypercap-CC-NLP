# Hypercap-CC-NLP

> Notebooks and resources to assemble a hypercapnia cohort from MIMIC-IV, annotate clinical notes, quantify inter‑rater agreement, and train/evaluate an NLP classifier for chief complaint identification in critical care.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Status: research code (Quarto-first pipeline); no archived release yet
- Preprint / paper: _pending_ (submitted to ATS 2026 conference as abstract)

## Cite this work
Until a manuscript/preprint is available, please cite the repository (and MIMIC resources you rely on). A `CITATION.cff` will be added once a preprint is posted.

## Quick start

> You need credentialed access to MIMIC‑IV (structured) and, if using notes, MIMIC‑IV‑Note (see **Data access**). The main pipeline is **Quarto-first** (`.qmd` + Python execution). Core stage logic is embedded directly in the 4 main `.qmd` notebooks; `src/hypercap_cc_nlp/` is used for QA/contracts/audit/parity utilities.

### 1) Create the environment (uv)
```bash
uv sync
```

### 1a) Configure BigQuery CLI auth (one-time per machine)
The cohort notebook expects Google Application Default Credentials.

```bash
gcloud init
gcloud auth application-default login
gcloud services enable bigquery.googleapis.com --project <your-billing-project-id>
```

### 1b) Install the required spaCy English model (`en_core_web_sm`)
`Hypercap CC NLP Classifier.qmd` now fails fast if this model is missing, to avoid silent NLP quality drift.

```bash
./.venv/bin/python -m spacy download en_core_web_sm
```

If your venv reports `No module named pip`, bootstrap pip first:
```bash
./.venv/bin/python -m ensurepip --upgrade
./.venv/bin/python -m pip --version
./.venv/bin/python -m spacy download en_core_web_sm
```

If `spacy download` fails due network/DNS restrictions, install the wheel directly (after downloading it from a machine with internet access):
```bash
./.venv/bin/python -m pip install /path/to/en_core_web_sm-3.8.0-py3-none-any.whl
```

Quick verification:
```bash
./.venv/bin/python - <<'PY'
import spacy
nlp = spacy.load("en_core_web_sm")
print("Loaded spaCy model:", nlp.meta.get("name"), nlp.meta.get("version"))
PY
```

### 1c) Install Quarto + TinyTeX (PDF rendering)
Quarto is required for the pipeline notebooks and distributable PDF outputs.

```bash
quarto --version
quarto install tinytex
quarto check
```

### 2) Configure paths
Copy the example env file, then paste your own local keys/paths:

```bash
cp .env.example .env
```

Then edit `.env` and replace placeholders (especially `WORK_PROJECT` and any auth keys).

Required keys for the default BigQuery workflow are documented in `.env.example`:

```
MIMIC_BACKEND=bigquery
WORK_PROJECT=<your-billing-project-id>
BQ_PHYSIONET_PROJECT=physionet-data
BQ_DATASET_HOSP=mimiciv_3_1_hosp
BQ_DATASET_ICU=mimiciv_3_1_icu
BQ_DATASET_ED=mimiciv_ed
HF_TOKEN=hf_<your_token_here>  # optional
```

`HF_TOKEN` is optional and only needed for authenticated Hugging Face model downloads.
`WORK_DIR` is optional but recommended for reproducibility manifests:

```
WORK_DIR=/path/to/Hypercap-CC-NLP
```

Do not commit your `.env` file.

Optional strict resource hashing for classifier reproducibility:

```bash
cp Annotation/resource_manifest.example.json Annotation/resource_manifest.json
# Fill sha256 values in Annotation/resource_manifest.json, then run:
make check-resources
```

Verify from Python by explicitly loading `.env`:

```bash
./.venv/bin/python - <<'PY'
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
print("HF_TOKEN loaded:", bool(os.getenv("HF_TOKEN")))
PY
```

Note: `./.venv/bin/python -c "import os; print(bool(os.getenv('HF_TOKEN')))"` returns `False` unless `HF_TOKEN` is already exported in your shell environment.

Optional workflow overrides (defaults use canonical handoff files):

```bash
CLASSIFIER_INPUT_FILENAME="MIMICIV all with CC.xlsx"      # optional override for classifier input
RATER_NLP_INPUT_FILENAME="MIMICIV all with CC_with_NLP.xlsx"  # optional override for rater NLP input
RATER_ANNOTATION_PATH="Annotation/Final 2025-10-14 Annotation Sample.xlsx"  # optional annotation workbook override
ANALYSIS_INPUT_FILENAME="MIMICIV all with CC_with_NLP.xlsx"  # optional override for analysis input
BQ_QUERY_TIMEOUT_SECS=1800  # optional BigQuery timeout for cohort query execution
WRITE_ARCHIVE_XLSX_EXPORTS=0  # set 1 to write additional legacy/timestamped XLSX archive exports
COHORT_FAIL_ON_ALL_OTHER_SOURCE=1  # fail cohort run if gas source attribution collapses to all other/unknown
COHORT_WARN_OTHER_RATE=0.50  # warn when mean gas_source_other_rate is high
COHORT_FAIL_OTHER_RATE=      # optional hard fail threshold for gas_source_other_rate (unset = disabled)
COHORT_GAS_SOURCE_INFERENCE_MODE=metadata_only  # enforce metadata/text-only source inference
COHORT_OTHER_RELATIVE_REDUCTION_MIN=0.10  # require >=10% relative OTHER-rate reduction vs latest baseline audit (strict mode)
COHORT_FAIL_ON_OMR_ATTACH_INCONSISTENCY=1  # fail when OMR +/-365-day candidates exist but attached outputs are all null
COHORT_ALLOW_EMPTY_OMR=0  # set 1 to bypass strict OMR attach guard for intentional reruns
COHORT_ALLOW_OMR_QUERY_FAILURE=0  # set 1 to continue with empty OMR when OMR query fails
COHORT_ANTHRO_CHARTED_FALLBACK=1  # enable charted anthropometric fallback extraction
COHORT_ANTHRO_NEAREST_ANYTIME=1  # use nearest-anytime charted fallback selection
COHORT_ANTHRO_MIN_BMI_COVERAGE=0.30  # contract minimum BMI coverage floor
CLASSIFIER_STRICT_RESOURCE_HASH=1  # fail on resource-manifest hash mismatch (set 0 to warn)
PIPELINE_CONTRACT_MODE=fail  # fail|warn contract enforcement mode
RUN_MANIFEST_STAGE_SCOPE=all  # per-stage run manifests are written for cohort/classifier/rater/analysis
```

Hard-coded QA-only POC sanity contract (not a runtime notebook env setting):
- `COHORT_POC_PCO2_MEDIAN_MIN = 45`
- `COHORT_POC_PCO2_MEDIAN_MAX = 80`
- `COHORT_POC_PCO2_FAIL_ENABLED = 1`
- Enforced by `make contracts-check` / pipeline audit checks against exported cohort artifacts.

`GOOGLE_APPLICATION_CREDENTIALS` is also supported (optional) when you prefer service-account auth over ADC login.

### 3) Run the notebooks in order (interactive)
Canonical Quarto pipeline:
1. `MIMICIV_hypercap_EXT_cohort.qmd` – cohort assembly using BigQuery
2. `Hypercap CC NLP Classifier.qmd` – consumes canonical cohort workbook and writes canonical NLP workbook
3. `Rater Agreement Analysis.qmd` – compares adjudicated labels to classifier output and writes agreement artifacts
4. `Hypercap CC NLP Analysis.qmd` – consumes canonical NLP workbook and produces analysis exports

Render one notebook manually (if needed):

```bash
QUARTO_PYTHON="$PWD/.venv/bin/python" quarto render "Hypercap CC NLP Classifier.qmd" --to pdf
```

Legacy compatibility note:
- The `.ipynb` versions are retained as transition-era references and `make notebook-*` compatibility targets.
- The legacy `.ipynb` files intentionally remain in the repository root (same level as `.qmd`) so existing notebook targets and collaborators’ links continue to work.
- New feature work should be authored in `.qmd`.
- Archiving/moving `.ipynb` files is deferred until `make notebook-*` targets are retired.

Related (independent) workflows:
- `Annotation/` – manual annotation workflow

### 4) Run the pipeline headlessly (reproducible CLI)
Preferred deterministic Quarto entrypoint:

```bash
make quarto-pipeline
```

To force all four stage PDFs into one folder in a single invocation:

```bash
make quarto-pipeline REPORT_RUN_ID=$(date +%Y%m%d_%H%M%S)
```

Run Quarto stages individually:

```bash
make quarto-cohort
make quarto-classifier
make quarto-rater
make quarto-analysis
```

Preflight and reproducibility checks:

```bash
make check-resources
```

Post-run QA checks (explicitly separate from `make quarto-pipeline`):

```bash
make contracts-check STAGE=all
make quarto-parity-check BASELINE=latest
make quarto-pipeline-audit BASELINE=latest
```

Execution policy:
- `make quarto-pipeline` is the primary render/data-generation workflow.
- QA contracts/parity/audit are run explicitly with the commands above.
- This separation keeps generation and QA stages reproducible but independently invokable.

PDF outputs from Quarto targets are written to:
- `artifacts/reports/<run_id>/MIMICIV_hypercap_EXT_cohort.pdf`
- `artifacts/reports/<run_id>/Hypercap CC NLP Classifier.pdf`
- `artifacts/reports/<run_id>/Rater Agreement Analysis.pdf`
- `artifacts/reports/<run_id>/Hypercap CC NLP Analysis.pdf`

Each successful Quarto stage also publishes a latest copy to the repository root:
- `MIMICIV_hypercap_EXT_cohort.pdf`
- `Hypercap CC NLP Classifier.pdf`
- `Rater Agreement Analysis.pdf`
- `Hypercap CC NLP Analysis.pdf`

PDF naming policy:
- Stage targets keep a single canonical filename (space-separated titles) per report.
- Dashed duplicate filenames are no longer retained by the Make targets.
- `artifacts/reports/<run_id>/` remains the run archive; root PDFs are latest-stage convenience copies.
- Wide manuscript tables are rendered with LaTeX `longtable` (+ landscape wrappers where needed) so columns are fully visible in exported PDFs.

Optional legacy compatibility path:

```bash
make notebook-pipeline
```

Capture baseline + parity check + Quarto audit:

```bash
make baseline-capture-jupyter
make quarto-parity-check BASELINE=latest
make quarto-pipeline-audit BASELINE=latest
```

Note on `quarto-pipeline-audit` report directories:
- It executes stage targets sequentially and may produce multiple timestamped `artifacts/reports/<run_id>/` folders.
- This is expected; if you need one consolidated report folder, run `make quarto-pipeline REPORT_RUN_ID=<id>` directly.

Cleanup transient generated outputs:

```bash
make clean-generated
```

### 5) Install a user-level kernel (optional, VS Code selection)
If you prefer a user-level kernel for editor selection, run:
```bash
./.venv/bin/python -m ipykernel install --user --name hypercap-cc-nlp --display-name "Python (hypercap-cc-nlp)"
```

## Data access
Requires credentialed access to **MIMIC‑IV on BigQuery** (HOSP + ICU) and **MIMIC‑IV‑ED** through PhysioNet. **MIMIC‑IV‑Note** is optional and only needed for note‑based analyses.

## Environment
- Environment files: `pyproject.toml` + `uv.lock`
- Python: `>=3.11,<3.12`
- OS: macOS / Linux
- External tools: Google Cloud SDK (`gcloud`) for BigQuery auth
- GPU not required unless deep models are added.

## Repository layout
```
├── src/hypercap_cc_nlp/         # QA/contracts/audit/parity helpers (core stage logic stays in qmd)
├── tests/                       # pytest coverage for QA + notebook-output contracts
├── Annotation/
├── annotation_agreement_outputs_nlp/
├── debug/abg_vbg_capture/       # ABG/VBG capture forensics (debug-only)
├── Drafts/                     # manuscript/abstract drafts
├── Legacy Code/                # prior versions / deprecated experiments
├── MIMIC tabular data/         # raw/derived data exports (not shared; request access)
├── .jupyter/                   # repo-local kernelspec (hypercap-cc-nlp)
├── _quarto.yml
├── MIMICIV_hypercap_EXT_cohort.qmd
├── Hypercap CC NLP Classifier.qmd
├── Rater Agreement Analysis.qmd
├── Hypercap CC NLP Analysis.qmd
├── MIMICIV_hypercap_EXT_cohort.ipynb
├── Rater Agreement Analysis.ipynb
├── Hypercap CC NLP Classifier.ipynb
├── Hypercap CC NLP Analysis.ipynb
├── Chart Review Sample Calc.qmd
├── Makefile
├── LICENSE
└── README.md
```

## Workflow overview
Canonical execution chain:
- `MIMICIV_hypercap_EXT_cohort.qmd` → `MIMIC tabular data/MIMICIV all with CC.xlsx`
- `Hypercap CC NLP Classifier.qmd` → `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- `Rater Agreement Analysis.qmd` reads the canonical NLP workbook above and writes join-audit artifacts plus agreement outputs.
- `Hypercap CC NLP Analysis.qmd` reads the canonical NLP workbook above (ordered after rater in `make quarto-pipeline`, but does not consume rater artifacts).

Anthropometric timing policy:
- OMR anthropometrics use a tiered fallback: pre-ED (within 365 days) first, then post-ED (within 365 days).
- Charted fallback can fill remaining missing values with nearest-anytime records when enabled.
- Output provenance columns indicate uncertainty: `anthro_timing_tier`, `anthro_days_offset`, `anthro_chartdate`, `anthro_timing_uncertain`, `anthro_source`, `anthro_obstime`, `anthro_hours_offset`, `anthro_timing_basis`.

Classifier CC missingness policy:
- Pseudo-missing CC tokens (e.g., `-`, `--`, `N`, `UNKNOWN`, redaction underscores) are retained but flagged via `cc_pseudomissing_flag` and `cc_missing_flag`.
- Pseudo-missing rows are forced to uncodable primary output (`RFV1="uncodable"`, `RFV1_name="Uncodable/Unknown"`).
- Exported missingness audit columns are `cc_missing_flag`, `cc_missing_reason`, `cc_pseudomissing_flag`, and `cc_text_for_nlp`.

Annotation workbook curation remains an independent manual workflow.

## Methods summary (BigQuery pipeline)
- Query MIMIC‑IV HOSP/ICU/ED in BigQuery and assemble an **ED‑stay** cohort anchored to the first ED visit per hospitalization.
- Define hypercapnia via ICD codes and blood‑gas thresholds (ABG/VBG), then take the union.
- Join ED triage data and ED vitals; derive ED chief‑complaint subsets and time‑anchored features.
- Manually annotate ED chief complaints to NHAMCS 17 top‑level RVC groups.
- Quantify agreement with set‑based metrics and chance‑corrected scores (e.g., Gwet’s AC1).
- Train an NLP classifier to predict RVC groups and evaluate against adjudicated labels.

ED vitals cleaning policy (cohort stage):
- Temperatures in the 20–50 range are treated as Celsius-like and converted to Fahrenheit, then range-validated (50–120 °F).
- Pain score `13` is treated as sentinel/unknown and set missing; valid pain range is 0–10.
- SBP/DBP are range-validated to 20–300 / 10–200; SpO2 values `>100` or `<0` are dropped while `0` is retained and flagged.
- Per-run ED vitals audits are written to `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_*.csv`.

## Results mapping
| Artifact | Notebook | Output path |
|---|---|---|
| Cohort export (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_bq_abg_vbg_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`) |
| ED‑CC‑only cohort (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_EDcc_only_bq_abg_vbg_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`) |
| ED‑CC sample (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_EDcc_sample<N>_bq_abg_vbg_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`, `N` defaults to 160) |
| ED‑stay workbook (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_EDstay_bq_gas_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`) |
| All-encounters workbook (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_all_encounters_bq_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`) |
| ED‑CC ED-stay workbook (optional archive) | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/mimic_hypercap_EXT_EDcc_only_edstay_bq_<timestamp>.xlsx` (`WRITE_ARCHIVE_XLSX_EXPORTS=1`) |
| Full CC workbook (no NLP) | `MIMICIV_hypercap_EXT_cohort.qmd` | Canonical: `MIMIC tabular data/MIMICIV all with CC.xlsx`; archive: `MIMIC tabular data/prior runs/YYYY-MM-DD MIMICIV all with CC.xlsx` |
| Cohort run manifest | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD cohort_run_manifest.json` |
| Data dictionary | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD MIMICIV all with CC_data_dictionary.xlsx` and `.csv` |
| QA summary | `MIMICIV_hypercap_EXT_cohort.qmd` | `qa_summary.json` |
| OMR diagnostics | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD omr_diagnostics.json` |
| Anthropometric coverage audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD anthropometrics_coverage_audit.json` |
| Gas source audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD gas_source_audit.json` |
| Gas source overlap summary | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD gas_source_overlap_summary.csv` |
| First other-pCO2 audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD first_other_pco2_audit.csv` |
| ED vitals distribution audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_distribution_summary.csv` |
| ED vitals extremes audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_extreme_examples.csv` |
| ED vitals raw→clean delta audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD ed_vitals_model_delta.csv` |
| Vitals outlier audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD vitals_outlier_audit.csv` |
| Combined outlier audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD outliers_audit.csv` |
| Lab item map | `MIMICIV_hypercap_EXT_cohort.qmd` | `lab_item_map.json` |
| Lab unit audit | `MIMICIV_hypercap_EXT_cohort.qmd` | `lab_unit_audit.csv` |
| Current admission columns | `MIMICIV_hypercap_EXT_cohort.qmd` | `current_columns.json` |
| Current ED columns (if `ed_df` exists) | `MIMICIV_hypercap_EXT_cohort.qmd` | `ed_columns.json` |
| ED-stay cohort parquet snapshot | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/cohort_ed_stay_<timestamp>.parquet` |
| ED vitals (long) parquet snapshot | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/ed_vitals_long_<timestamp>.parquet` |
| Labs (long) parquet snapshot | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/labs_long_<timestamp>.parquet` |
| Gas panels (HOSP) parquet snapshot | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/gas_panels_<timestamp>.parquet` |
| Gas panels (ICU POC) parquet snapshot | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/gas_panels_poc_<timestamp>.parquet` |
| ICU POC matched itemid map | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD icu_poc_itemid_map.csv` |
| ICU POC matched itemid usage | `MIMICIV_hypercap_EXT_cohort.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD icu_poc_itemid_usage.csv` |
| Manual agreement metrics | `Rater Agreement Analysis.qmd` | `Annotation/Full Annotations/Agreement Metrics/` (pairwise set metrics, binary stats, AC1 summary) |
| NLP vs R3 agreement outputs | `Rater Agreement Analysis.qmd` | `annotation_agreement_outputs_nlp/R3_vs_NLP_set_metrics_by_visit.csv`, `annotation_agreement_outputs_nlp/R3_vs_NLP_binary_stats_by_category.csv`, `annotation_agreement_outputs_nlp/R3_vs_NLP_summary.txt` |
| R3/NLP join audit | `Rater Agreement Analysis.qmd` | `annotation_agreement_outputs_nlp/R3_vs_NLP_join_audit.json`, `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_adjudicated_keys.csv`, `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_nlp_keys.csv` |
| NLP‑augmented workbook | `Hypercap CC NLP Classifier.qmd` | Canonical: `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`; optional archive (`WRITE_ARCHIVE_XLSX_EXPORTS=1`): `MIMIC tabular data/prior runs/YYYY-MM-DD MIMICIV all with CC_with_NLP.xlsx` |
| Classifier CC missingness audit | `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD classifier_cc_missing_audit.csv` |
| Classifier phrase regression audit | `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD classifier_phrase_audit.csv` |
| Classifier contract report | `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD classifier_contract_report.json` |
| Classifier run manifest | `Hypercap CC NLP Classifier.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD classifier_run_manifest.json` |
| Pipeline contract report | `make contracts-check` | `debug/contracts/<run_id>/contract_report.json` (+ `FAILED_CONTRACT.json` on failure) |
| Analysis exports | `Hypercap CC NLP Analysis.qmd` | `Symptom_Composition_by_Hypercapnia_Definition.xlsx`, `Symptom_Composition_Pivot_ChartReady.xlsx`, `Symptom_Composition_by_ABG_VBG_Overlap.xlsx`, `Symptom_Composition_by_ICD_Gas_Overlap.xlsx` |
| Analysis gas-source overlap export | `Hypercap CC NLP Analysis.qmd` | `Symptom_Composition_by_Gas_Source_Overlap.xlsx` |
| ICD diagnostic performance export | `Hypercap CC NLP Analysis.qmd` | `ICD_vs_Gas_Performance.xlsx` |
| ICD-positive subset breakdown export | `Hypercap CC NLP Analysis.qmd` | `ICD_Positive_Subset_Breakdown.xlsx` |
| Ascertainment overlap UpSet outputs | `Hypercap CC NLP Analysis.qmd` | `Ascertainment_Overlap_UpSet.png`, `Ascertainment_Overlap_Intersections.xlsx` |
| Analysis run manifest | `Hypercap CC NLP Analysis.qmd` | `MIMIC tabular data/prior runs/YYYY-MM-DD analysis_run_manifest.json` |
| Rater input manifest | `Rater Agreement Analysis.qmd` | `annotation_agreement_outputs_nlp/R3_vs_NLP_input_manifest.json` |
| Rater matched key hashes | `Rater Agreement Analysis.qmd` | `annotation_agreement_outputs_nlp/R3_vs_NLP_matched_key_hashes.csv` |
| Rater run manifest | `Rater Agreement Analysis.qmd` | `annotation_agreement_outputs_nlp/R3_vs_NLP_run_manifest.json` |

Rater join policy:
- Rater/NLP agreement is computed on matched `(hadm_id, subject_id)` rows.
- Unmatched adjudicated and unmatched NLP keys are exported for audit, and the notebook continues when at least one matched row exists.
- `R3_vs_NLP_join_audit.json` now includes `join_interpretation` and `severity`:
  - `severity=info` when adjudicated rows are fully covered (`matched_rate_vs_adjudicated == 1.0`) and NLP has expected extras.
  - `severity=warning` for partial adjudicated overlap.

Schema transition note:
- Classifier intake now applies transitional aliases (`age`, `hr`, `rr`, `sbp`, `dbp`, `temp`, `spo2`, `race`) from canonical source columns when alias columns are absent.
- Source columns are preserved; aliases are compatibility scaffolding and may be deprecated after full downstream migration.

Anthropometric provenance note:
- `bmi_closest_pre_ed`, `height_closest_pre_ed`, and `weight_closest_pre_ed` may come from either pre-ED or bounded post-ED OMR fallback windows.
- Use `anthro_timing_uncertain` (and `anthro_timing_tier`) for sensitivity analyses when restricting to temporally cleaner baseline measurements.

## Quality checks / tests
Run repo checks locally after dependency sync:

```bash
uv run pytest -q
uv run --with ruff ruff check src tests
```

Equivalent Make targets:

```bash
make test
make lint
```

Optional formatting:

```bash
uv run --with ruff ruff format src tests
```

## Pipeline deep-dive audit
Use `make quarto-pipeline-audit` to run:
1. `make lint` and `make test`
2. full 4-stage Quarto pipeline (`cohort -> classifier -> rater -> analysis`)
3. artifact contract checks, hard-fail QA checks, log scanning, and drift checks vs latest baseline

Audit outputs are written to:
- `debug/pipeline_audit/<run_id>/run_manifest.json`
- `debug/pipeline_audit/<run_id>/logs/01_cohort.log` ... `04_analysis.log`
- `debug/pipeline_audit/<run_id>/audit_report.json`
- `debug/pipeline_audit/<run_id>/audit_summary.md`
- `debug/pipeline_audit/<run_id>/metric_drift.csv`
- `debug/pipeline_audit/<run_id>/warnings_index.csv`

Interpretation:
- `status=fail`: one or more P0/P1 findings (blocking)
- `status=warning`: no P0/P1 findings, but at least one P2 finding
- `status=pass`: no findings

Current expected non-blocking warning:
- `missing_work_dir_env_key` when `.env` omits `WORK_DIR`; pipeline remains valid, but manifest portability is reduced.

Reproducibility checklist:
- Rebuild environment from lockfile: `uv sync --frozen`
- Keep `.env` populated for BigQuery datasets/project and (recommended) `WORK_DIR`
- Keep ADC valid: `gcloud auth application-default login`
- Ensure spaCy model exists: `make spacy-model`
- Capture a fresh Jupyter baseline when needed: `make baseline-capture-jupyter`
- Run `make quarto-pipeline-audit` and archive the generated `debug/pipeline_audit/<run_id>/` folder with results.
- Run `make quarto-parity-check BASELINE=latest` and archive `debug/pipeline_parity/<run_id>/`.

Legacy compatibility:
- `make notebook-pipeline` and `make notebook-pipeline-audit` remain available during transition.
- Root-level `.ipynb` notebook files are intentionally preserved for that compatibility path.
- Main pipeline development and PDF distribution should use the Quarto targets.

## Debug workflows
`debug/abg_vbg_capture/` contains debug-only forensics notebooks for ABG/VBG capture deltas, including run order and generated artifacts:

- `01_extract_current_vs_legacy.ipynb`
- `02_reason_attribution.ipynb`
- `03_fix_validation.ipynb`

See `debug/abg_vbg_capture/README.md` for scope, inputs, and outputs.

## Quarto / R note
`Chart Review Sample Calc.qmd` uses R packages (`presize`, `kappaSize`, `irr`) for reference-standard power calculations.

## License
Released under the **MIT License**.

## Contributing
Issues and PRs welcome.

## Maintainer
**@reblocke** – open a GitHub Issue for questions.

## Archival
No DOI yet. 
