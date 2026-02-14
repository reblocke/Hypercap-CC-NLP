# Hypercap-CC-NLP

> Notebooks and resources to assemble a hypercapnia cohort from MIMIC-IV, annotate clinical notes, quantify inter‑rater agreement, and train/evaluate an NLP classifier for chief complaint identification in critical care.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Status: research code (notebook‑first); no archived release yet
- Preprint / paper: _pending_ (submitted to ATS 2026 conference as abstract)

## Cite this work
Until a manuscript/preprint is available, please cite the repository (and MIMIC resources you rely on). A `CITATION.cff` will be added once a preprint is posted.

## Quick start

> You need credentialed access to MIMIC‑IV (structured) and, if using notes, MIMIC‑IV‑Note (see **Data access**). This repository is notebook-first; shared reusable logic lives under `src/hypercap_cc_nlp/`.

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
`Hypercap CC NLP Classifier.ipynb` now fails fast if this model is missing, to avoid silent NLP quality drift.

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

### 2) Configure paths
Copy the example env file, then paste your own local keys/paths:

```bash
cp .env.example .env
```

Then edit `.env` and replace placeholders (especially `WORK_PROJECT`, `WORK_DIR`, and any auth keys).

Required keys are documented in `.env.example`:

```
MIMIC_BACKEND=bigquery
WORK_PROJECT=<your-billing-project-id>
BQ_PHYSIONET_PROJECT=physionet-data
BQ_DATASET_HOSP=mimiciv_3_1_hosp
BQ_DATASET_ICU=mimiciv_3_1_icu
BQ_DATASET_ED=mimiciv_ed
WORK_DIR=/path/to/Hypercap-CC-NLP
HF_TOKEN=hf_<your_token_here>  # optional
```

`HF_TOKEN` is optional and only needed for authenticated Hugging Face model downloads.
Do not commit your `.env` file.

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
COHORT_FAIL_ON_ALL_OTHER_SOURCE=1  # fail cohort run if gas source attribution collapses to all other/unknown
COHORT_FAIL_ON_OMR_ATTACH_INCONSISTENCY=1  # fail when OMR +/-365-day candidates exist but attached outputs are all null
COHORT_ALLOW_EMPTY_OMR=0  # set 1 to bypass strict OMR attach guard for intentional reruns
```

### 3) Run the notebooks in order (interactive)
Core pipeline:
1. `MIMICIV_hypercap_EXT_cohort.ipynb` – cohort assembly using BigQuery
2. `Hypercap CC NLP Classifier.ipynb` – consumes canonical cohort workbook and writes canonical NLP workbook
3. `Rater Agreement Analysis.ipynb` – compares adjudicated labels to classifier output and writes agreement artifacts
4. `Hypercap CC NLP Analysis.ipynb` – consumes canonical NLP workbook and produces analysis exports

Related (independent) workflows:
- `Annotation/` – manual annotation workflow

### 4) Run the pipeline headlessly (reproducible CLI)
Preferred deterministic entrypoint:

```bash
make notebook-pipeline
```

Run stages individually:

```bash
make notebook-cohort
make notebook-classifier
make notebook-rater
make notebook-analysis
```

Direct `nbconvert` invocation is also supported. We ship a **repo‑local kernelspec** that points to `.venv/bin/python` to avoid kernel/venv drift:

```bash
JUPYTER_PATH="$PWD/.jupyter" ./.venv/bin/python -m jupyter nbconvert \
  --to notebook --execute --inplace \
  --ClearOutputPreprocessor.enabled=True \
  --ExecutePreprocessor.timeout=0 \
  --ExecutePreprocessor.kernel_name=hypercap-cc-nlp \
  "Hypercap CC NLP Classifier.ipynb"
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
├── src/hypercap_cc_nlp/         # importable analysis helpers
├── tests/                       # pytest coverage for core helpers
├── Annotation/
├── annotation_agreement_outputs_nlp/
├── debug/abg_vbg_capture/       # ABG/VBG capture forensics (debug-only)
├── Drafts/                     # manuscript/abstract drafts
├── Legacy Code/                # prior versions / deprecated experiments
├── MIMIC tabular data/         # raw/derived data exports (not shared; request access)
├── .jupyter/                   # repo-local kernelspec (hypercap-cc-nlp)
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
- `MIMICIV_hypercap_EXT_cohort.ipynb` → `MIMIC tabular data/MIMICIV all with CC.xlsx`
- `Hypercap CC NLP Classifier.ipynb` → `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`
- `Rater Agreement Analysis.ipynb` reads the canonical NLP workbook above and writes join-audit artifacts plus agreement outputs.
- `Hypercap CC NLP Analysis.ipynb` reads the canonical NLP workbook above.

Anthropometric timing policy:
- OMR anthropometrics use a tiered fallback: pre-ED (within 365 days) first, then post-ED (within 365 days).
- Output provenance columns indicate uncertainty: `anthro_timing_tier`, `anthro_days_offset`, `anthro_chartdate`, `anthro_timing_uncertain`.

Annotation workbook curation remains an independent manual workflow.

## Methods summary (BigQuery pipeline)
- Query MIMIC‑IV HOSP/ICU/ED in BigQuery and assemble an **ED‑stay** cohort anchored to the first ED visit per hospitalization.
- Define hypercapnia via ICD codes and blood‑gas thresholds (ABG/VBG), then take the union.
- Join ED triage data and ED vitals; derive ED chief‑complaint subsets and time‑anchored features.
- Manually annotate ED chief complaints to NHAMCS 17 top‑level RVC groups.
- Quantify agreement with set‑based metrics and chance‑corrected scores (e.g., Gwet’s AC1).
- Train an NLP classifier to predict RVC groups and evaluate against adjudicated labels.

## Results mapping
| Artifact | Notebook | Output path |
|---|---|---|
| Cohort export | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_bq_abg_vbg_<timestamp>.xlsx` |
| ED‑CC‑only cohort | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_EDcc_only_bq_abg_vbg_<timestamp>.xlsx` |
| ED‑CC sample | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_EDcc_sample160_bq_abg_vbg_<timestamp>.xlsx` |
| Full CC workbook (no NLP) | `MIMICIV_hypercap_EXT_cohort.ipynb` | Canonical: `MIMIC tabular data/MIMICIV all with CC.xlsx`; archive: `MIMIC tabular data/prior runs/YYYY-MM-DD MIMICIV all with CC.xlsx` |
| Data dictionary | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/YYYY-MM-DD MIMICIV all with CC_data_dictionary.xlsx` |
| QA summary | `MIMICIV_hypercap_EXT_cohort.ipynb` | `qa_summary.json` |
| Gas source audit | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/prior runs/YYYY-MM-DD gas_source_audit.json` |
| First other-pCO2 audit | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/prior runs/YYYY-MM-DD first_other_pco2_audit.csv` |
| Vitals outlier audit | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/prior runs/YYYY-MM-DD vitals_outlier_audit.csv` |
| Lab item map | `MIMICIV_hypercap_EXT_cohort.ipynb` | `lab_item_map.json` |
| Lab unit audit | `MIMICIV_hypercap_EXT_cohort.ipynb` | `lab_unit_audit.csv` |
| Current admission columns | `MIMICIV_hypercap_EXT_cohort.ipynb` | `current_columns.json` |
| Current ED columns (if `ed_df` exists) | `MIMICIV_hypercap_EXT_cohort.ipynb` | `ed_columns.json` |
| ED vitals (long) | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/ed_vitals_long.parquet` |
| Labs (long) | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/labs_long.parquet` |
| Gas panels (HOSP) | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/gas_panels.parquet` |
| Gas panels (ICU POC) | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/gas_panels_poc.parquet` |
| Manual agreement metrics | `Rater Agreement Analysis.ipynb` | `Annotation/Full Annotations/Agreement Metrics/` |
| NLP vs R3 agreement | `Rater Agreement Analysis.ipynb` | `annotation_agreement_outputs_nlp/` |
| R3/NLP join audit | `Rater Agreement Analysis.ipynb` | `annotation_agreement_outputs_nlp/R3_vs_NLP_join_audit.json`, `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_adjudicated_keys.csv`, `annotation_agreement_outputs_nlp/R3_vs_NLP_unmatched_nlp_keys.csv` |
| NLP‑augmented workbook | `Hypercap CC NLP Classifier.ipynb` | Canonical: `MIMIC tabular data/MIMICIV all with CC_with_NLP.xlsx`; archive: `MIMIC tabular data/prior runs/YYYY-MM-DD MIMICIV all with CC_with_NLP.xlsx` |
| Analysis exports | `Hypercap CC NLP Analysis.ipynb` | `Symptom_Composition_by_Hypercapnia_Definition.xlsx`, `Symptom_Composition_Pivot_ChartReady.xlsx` |

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

Optional formatting:

```bash
uv run --with ruff ruff format src tests
```

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
