# Fix Candidates (Debug Evidence)

Recommended candidate: `D_relaxed_site_threshold`

## Candidate Summary

| candidate                |   hadm_any_n |   delta_any_vs_current |   abg_n |   vbg_n |   dropped_old_recovered_n |   dropped_old_recovered_pct | runtime_impact_estimate   |
|:-------------------------|-------------:|-----------------------:|--------:|--------:|--------------------------:|----------------------------:|:--------------------------|
| D_relaxed_site_threshold |        14476 |                   4275 |    6056 |   13032 |                      4237 |                    0.422222 | 1.02x                     |
| B_add_unknown_to_any     |        13104 |                   2903 |    6050 |    6423 |                      4206 |                    0.419133 | 1.00x                     |
| A_current_strict         |        10201 |                      0 |    6050 |    6423 |                      2672 |                    0.266268 | 1.00x                     |
| C_union_with_legacy      |        10201 |                      0 |    6050 |    6423 |                      2672 |                    0.266268 | 1.05x                     |

## Back-port QA Checks

- ABG capture non-zero sanity check (`abg_hypercap_threshold` > 0 expected).
- VBG capture non-zero sanity check (`vbg_hypercap_threshold` > 0 expected).
- Unknown source burden report (`gas_source_unknown_rate`).
- Label/unit drift alerts from top unmapped labels and non-mmhg units.