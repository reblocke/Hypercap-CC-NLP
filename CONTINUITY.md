Goal (incl. success criteria):
- Add brief rationale markdown for each section across all notebooks in the repository.

Constraints/Assumptions:
- Follow AGENTS.md instructions; Python-first; prioritize accuracy and reproducibility.
- Use evidence from notebooks; avoid speculation.

Key decisions:
- Use existing code/notebooks to determine cohort flow counts and definitions; flag gaps explicitly.

State:
- Done: Added overlap counts and symptom-by-overlap outputs.
- Now: Insert rationale markdown cells across notebooks.
- Next: Summarize changes and point to notebooks for review.

Done:
- Updated ledger for cohort-construction review request.
- Located cohort inclusion logic, thresholds, and printed counts in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added cohort flow count cell (ED/ICU/blood gas/hypercapnia/CC) to MIMICIV_hypercap_EXT_cohort.ipynb.
- Added ascertainment overlap counts cell (ABG-only/VBG-only/both; ICD+gas) to MIMICIV_hypercap_EXT_cohort.ipynb.
- Verified ICD code handling (any diagnosis; ED + hospital sources combined) and code list in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added ICD source flags and categorical source variable (ED vs HOSP vs BOTH vs NONE) in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added missingness summary cell (chief complaint, race/ethnicity, ED triage/vitals) in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added symptom distribution by overlap cell and exports in Hypercap CC NLP Analysis.ipynb.
- Added rationale markdown cells and section headings across all notebooks (cohort, NLP classifier/analysis, rater agreement, legacy notebooks).

Now:
- Verify overlap counts and whether symptom distributions by overlap are already computed.

Next:
- Provide answers on overlap counts and symptom distributions; offer to implement.

Open questions (UNCONFIRMED if needed):
- Whether you want an overlap summary table added to the cohort notebook and/or symptom distribution pivots by overlap.

Working set (files/ids/commands):
- MIMICIV_hypercap_EXT_cohort.ipynb
- README.md
- CONTINUITY.md
