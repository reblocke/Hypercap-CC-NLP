# Hypercap-CC-NLP

> Notebooks and resources to assemble a hypercapnia cohort from MIMIC-IV, annotate clinical notes, quantify inter‑rater agreement, and train/evaluate an NLP classifier for chief complaint identification in critical care.

**Links & IDs**
- Repository: https://github.com/reblocke/Hypercap-CC-NLP
- Status: research code (notebook‑first); no archived release yet
- Preprint / paper: _pending_ (submitted to ATS 2026 conference as abstract)

## Cite this work
Until a manuscript/preprint is available, please cite the repository (and MIMIC resources you rely on). A `CITATION.cff` will be added once a preprint is posted.

## Quick start

> You need credentialed access to MIMIC‑IV (structured) and, if using notes, MIMIC‑IV‑Note (see **Data access**). This repository is notebook‑driven; it does not install as a Python package.

### 1) Create the environment (uv)
```bash
uv venv
source .venv/bin/activate
uv sync
python -m ipykernel install --user --name hypercap-cc-nlp --display-name "Python (hypercap-cc-nlp)"
```

### 2) Configure paths
Create a `.env` file at the repo root:

```
MIMIC_BACKEND=bigquery
WORK_PROJECT=<your-billing-project-id>
BQ_PHYSIONET_PROJECT=physionet-data
BQ_DATASET_HOSP=mimiciv_3_1_hosp
BQ_DATASET_ICU=mimiciv_3_1_icu
BQ_DATASET_ED=mimiciv_ed
WORK_DIR=/path/to/Hypercap-CC-NLP
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### 3) Run the notebooks in order
1. `MIMICIV_hypercap_EXT_cohort.ipynb` – cohort assembly using BigQuery 
2. `Annotation/` – manual annotation workflow  
3. `Rater Agreement Analysis.ipynb` – inter‑rater reliability  
4. `Hypercap CC NLP Classifier.ipynb` – model training
5. `Hypercap CC NLP Analysis.ipynb` – model evaluation and figure generation  

## Data access
Requires credentialed access to **MIMIC‑IV on BigQuery** (HOSP + ICU) and **MIMIC‑IV‑ED** through PhysioNet. **MIMIC‑IV‑Note** is optional and only needed for note‑based analyses.

## Environment
- Environment files: `pyproject.toml` + `uv.lock`
- OS: Linux/macOS x86_64
- GPU not required unless deep models are added.

## Repository layout
```
├── Annotation/
├── annotation_agreement_outputs_nlp/
├── Drafts/                     # manuscript/abstract drafts
├── Legacy Code/                # prior versions / deprecated experiments
├── MIMIC tabular data/         # raw/derived data exports (not shared; request access)
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
MIMIC‑IV (BigQuery) → cohort assembly → manual annotation → agreement analysis → NLP classifier → evaluation artifacts.

## Methods summary (BigQuery pipeline)
- Query MIMIC‑IV HOSP/ICU/ED in BigQuery and assemble an admission‑level cohort.
- Define hypercapnia via ICD codes and blood‑gas thresholds (ABG/VBG), then take the union.
- Join ED triage data and first ED vitals when available; derive an ED chief‑complaint subset.
- Manually annotate ED chief complaints to NHAMCS 17 top‑level RVC groups.
- Quantify agreement with set‑based metrics and chance‑corrected scores (e.g., Gwet’s AC1).
- Train an NLP classifier to predict RVC groups and evaluate against adjudicated labels.

## Results mapping
| Artifact | Notebook | Output path |
|---|---|---|
| Cohort export | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_bq_abg_vbg_<timestamp>.xlsx` |
| ED‑CC‑only cohort | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_EDcc_only_bq_abg_vbg_<timestamp>.xlsx` |
| ED‑CC sample | `MIMICIV_hypercap_EXT_cohort.ipynb` | `MIMIC tabular data/mimic_hypercap_EXT_EDcc_sample160_bq_abg_vbg_<timestamp>.xlsx` |
| Manual agreement metrics | `Rater Agreement Analysis.ipynb` | `Annotation/Full Annotations/Agreement Metrics/` |
| NLP vs R3 agreement | `Rater Agreement Analysis.ipynb` | `annotation_agreement_outputs_nlp/` |
| NLP‑augmented workbook | `Hypercap CC NLP Classifier.ipynb` | `MIMIC tabular data/*_with_NLP.xlsx` |
| Analysis exports | `Hypercap CC NLP Analysis.ipynb` | `Symptom_Composition_by_Hypercapnia_Definition.xlsx`, `Symptom_Composition_Pivot_ChartReady.xlsx` |

## Quality checks / tests
A synthetic small-note smoke test is recommended.

## License
Released under the **MIT License**.

## Contributing
Issues and PRs welcome.

## Maintainer
**@reblocke** – open a GitHub Issue for questions.

## Archival
No DOI yet. 
