# Manuscript Mapping

This document maps journal-submission outputs to their producing pipeline stages. Generated outputs are local/private by default and should be distributed as release assets rather than tracked in git.

## Main Manuscript

| Manuscript item | Producing stage | Primary output |
|---|---|---|
| Figure 1: analytic cohort construction, source-specific overlap, ascertainment definitions, and NLP workflow | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Figure 1.pdf` |
| Figure 2: presenting-concern prevalence by overlapping ascertainment indicator | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Figure 2.pdf`, `Figure 2.xlsx` |
| Figure 3: presenting-concern prevalence by age group | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Figure 3.pdf`, `Figure 3.xlsx` |
| Figure 4: presenting-concern prevalence by acidemia severity | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Figure 4.pdf`, `Figure 4.xlsx` |
| Table 1: baseline characteristics | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Table 1.xlsx` |
| Table 2: common presenting-concern categories | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Table 2.xlsx` |

## Supplement

| Supplement item | Producing stage | Primary output |
|---|---|---|
| Figure S1-S9 | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Figure S*.pdf`, selected `Figure S*.xlsx` |
| NLP classifier methods tables | `Hypercap CC NLP Classifier.qmd` | `Results/YYYY-MM-DD/NLP_Classifier_Supplement_Tables.xlsx` |
| Rater benchmark methods tables | `Rater Agreement Analysis.qmd` | `Results/YYYY-MM-DD/Rater_Benchmark_Supplement_Tables.xlsx` |
| Run-level submission manifest and output README | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/submission_manifest.xlsx`, `submission_manifest.csv`, `OUTPUTS_README.md` |
| Aggregate acid-base missingness, candidate definitions, and sensitivity suite | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/Supplementary_Table_Acid_Base_Source_Missingness.xlsx`, `Candidate_Definition_Yield_Composition.xlsx`, `Sensitivity_Analysis_Suite.xlsx` |
| Submission asset manifest | `Hypercap CC NLP Analysis.qmd` | `Results/YYYY-MM-DD/submission_assets/submission_assets_manifest.csv` |

## Release Asset Policy

For a public release, zip `Results/YYYY-MM-DD/submission_assets/` after a private audited run, generate a SHA-256 checksum, and attach both files to the GitHub/Zenodo release. Do not commit the zip or generated outputs to git.
