Goal (incl. success criteria):
- Determine whether other scripts need path updates for new ED‑CC outputs and propose automation options.

Constraints/Assumptions:
- Follow AGENTS.md instructions; Python-first; prioritize accuracy and reproducibility.
- Use evidence from notebooks; avoid speculation.

Key decisions:
- Use existing code/notebooks to determine cohort flow counts and definitions; flag gaps explicitly.

State:
- Done: Prior ICD-handling updates; missingness summary cell added.
- Now: Check downstream notebooks for hardcoded ED‑CC output paths.
- Next: Answer whether updates are needed and suggest automation.

Done:
- Updated ledger for cohort-construction review request.
- Located cohort inclusion logic, thresholds, and printed counts in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added cohort flow count cell (ED/ICU/blood gas/hypercapnia/CC) to MIMICIV_hypercap_EXT_cohort.ipynb.
- Verified ICD code handling (any diagnosis; ED + hospital sources combined) and code list in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added ICD source flags and categorical source variable (ED vs HOSP vs BOTH vs NONE) in MIMICIV_hypercap_EXT_cohort.ipynb.
- Added missingness summary cell (chief complaint, race/ethnicity, ED triage/vitals) in MIMICIV_hypercap_EXT_cohort.ipynb.

Now:
- Verify references to ED‑CC outputs in other notebooks and advise on automation.

Next:
- Provide answers on path updates and automation options.

Open questions (UNCONFIRMED if needed):
- Whether you want an automated “latest file” resolver or explicit .env variables for outputs.

Working set (files/ids/commands):
- MIMICIV_hypercap_EXT_cohort.ipynb
- README.md
- CONTINUITY.md
