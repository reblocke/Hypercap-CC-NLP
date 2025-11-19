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

### 1) Create the conda environment
```bash
conda env create -f environment.yml
conda activate hypercap-cc-nlp
python -m ipykernel install --user --name hypercap-cc-nlp --display-name "Python (hypercap-cc-nlp)"
```

### 2) Configure paths
Create a `.env` file at the repo root:

```
MIMIC_IV_DIR=/path/to/mimic-iv
MIMIC_IV_NOTE_DIR=/path/to/mimic-iv-note
WORK_DIR=/path/to/project/workdir
```

### 3) Run the notebooks in order
1. `MIMICIV_hypercap_EXT_cohort.ipynb` – cohort assembly using BigQuery 
2. `Annotation/` – manual annotation workflow  
3. `Rater Agreement Analysis.ipynb` – inter‑rater reliability  
4. `Hypercap CC NLP Classifier.ipynb` – model training/evaluation  

[ ] TODO: analysis codebook to be included

## Data access
Requires credentialed access to **MIMIC‑IV** and optionally **MIMIC‑IV‑Note** through PhysioNet.

## Environment
- Environment file: `environment.yml`
- OS: Linux/macOS/Windows x86_64
- GPU not required unless deep models are added.

## Repository layout
```
├── Annotation/
├── annotation_agreement_outputs_nlp/
├── MIMIC tabular data/
├── MIMICIV_hypercap_EXT_cohort.ipynb
├── Rater Agreement Analysis.ipynb
├── Hypercap CC NLP Classifier.ipynb
├── Chart Review Sample Calc.qmd
├── environment.yml
├── Makefile
├── LICENSE
└── README.md
```

## Workflow overview
MIMIC‑IV → cohort assembly → manual annotation → agreement analysis → NLP classifier → evaluation artifacts.

## Results mapping
| Artifact | Notebook | Output path |
|---|---|---|
| Cohort tables | `MIMICIV_hypercap_EXT_cohort.ipynb` | `data/cohort/` |
| Label CSV | `Annotation/` | `labels/annotations.csv` |
| Agreement plots/tables | `Rater Agreement Analysis.ipynb` | `annotation_agreement_outputs_nlp/` |
| Classifier metrics | `Hypercap CC NLP Classifier.ipynb` | `outputs/` |

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
