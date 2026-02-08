"""SQL registry for ABG/VBG capture forensics (debug-only)."""

from __future__ import annotations

from textwrap import dedent


def _fmt(sql_template: str, *, PHYS: str, HOSP: str, ICU: str) -> str:
    return dedent(sql_template).format(PHYS=PHYS, HOSP=HOSP, ICU=ICU).strip()


CURRENT_STRICT_SQL = """
WITH hadms AS (
  SELECT hadm_id FROM UNNEST(@hadms) AS hadm_id
),
hosp_cand AS (
  SELECT
    le.hadm_id,
    le.charttime,
    le.specimen_id,
    le.itemid,
    SAFE_CAST(le.valuenum AS FLOAT64) AS val,
    LOWER(REPLACE(COALESCE(le.valueuom, ''), ' ', '')) AS uom_nospace,
    LOWER(COALESCE(di.label, '')) AS lbl,
    LOWER(COALESCE(di.fluid, '')) AS fl
  FROM `{PHYS}.{HOSP}.labevents` le
  JOIN hadms h ON h.hadm_id = le.hadm_id
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.valuenum IS NOT NULL
    AND (
      REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b')
      OR (
        LOWER(COALESCE(di.category, '')) LIKE '%blood gas%'
        AND REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bco\\s*2\\b')
      )
    )
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
),
hosp_spec AS (
  SELECT
    le.specimen_id,
    LOWER(COALESCE(le.value, '')) AS spec_val
  FROM `{PHYS}.{HOSP}.labevents` le
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.specimen_id IS NOT NULL
    AND REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(specimen|sample)')
),
hosp_std AS (
  SELECT
    c.hadm_id,
    c.itemid,
    c.lbl AS label,
    c.charttime,
    CASE
      WHEN c.fl LIKE '%arterial%' OR REGEXP_CONTAINS(c.lbl, r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN c.fl LIKE '%ven%' OR REGEXP_CONTAINS(c.lbl, r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'arter') THEN 'arterial'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'ven') THEN 'venous'
      ELSE 'unknown'
    END AS site,
    c.uom_nospace,
    CASE WHEN c.uom_nospace = 'kpa' THEN c.val * 7.50062 ELSE c.val END AS pco2_mmhg
  FROM hosp_cand c
  LEFT JOIN hosp_spec s USING (specimen_id)
  WHERE c.val IS NOT NULL
),
icu_cand AS (
  SELECT
    ce.hadm_id,
    ce.itemid,
    LOWER(COALESCE(di.label, '')) AS lbl,
    ce.charttime,
    SAFE_CAST(ce.valuenum AS FLOAT64) AS val,
    LOWER(REPLACE(COALESCE(ce.valueuom, ''), ' ', '')) AS uom_nospace,
    LOWER(COALESCE(CAST(ce.value AS STRING), '')) AS valstr
  FROM `{PHYS}.{ICU}.chartevents` ce
  JOIN hadms h ON h.hadm_id = ce.hadm_id
  LEFT JOIN `{PHYS}.{ICU}.d_items` di
    ON di.itemid = ce.itemid
  WHERE ce.valuenum IS NOT NULL
    AND (
      REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b')
      OR (
        REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bco\\s*2\\b')
        AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
      )
    )
),
icu_std AS (
  SELECT
    hadm_id,
    itemid,
    lbl AS label,
    charttime,
    CASE
      WHEN REGEXP_CONTAINS(lbl, r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN REGEXP_CONTAINS(lbl, r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      ELSE 'unknown'
    END AS site,
    uom_nospace,
    CASE WHEN uom_nospace = 'kpa' OR REGEXP_CONTAINS(valstr, r'\\bkpa\\b') THEN val * 7.50062 ELSE val END AS pco2_mmhg
  FROM icu_cand
  WHERE val IS NOT NULL
),
all_std AS (
  SELECT hadm_id, itemid, label, charttime, site, uom_nospace, pco2_mmhg FROM hosp_std
  UNION ALL
  SELECT hadm_id, itemid, label, charttime, site, uom_nospace, pco2_mmhg FROM icu_std
),
filtered AS (
  SELECT *
  FROM all_std
  WHERE site IN ('arterial', 'venous')
    AND pco2_mmhg BETWEEN 5 AND 200
)
SELECT
  hadm_id,
  MAX(IF(site = 'arterial' AND pco2_mmhg >= 45.0, 1, 0)) AS current_abg_hypercap,
  MAX(IF(site = 'venous' AND pco2_mmhg >= 50.0, 1, 0)) AS current_vbg_hypercap,
  COUNT(*) AS current_candidate_n
FROM filtered
GROUP BY hadm_id
"""


LEGACY_REPLAY_SQL = """
WITH hadms AS (
  SELECT hadm_id FROM UNNEST(@hadms) AS hadm_id
),
hosp_cand AS (
  SELECT
    le.hadm_id,
    le.charttime,
    le.specimen_id,
    le.itemid,
    LOWER(COALESCE(di.label, '')) AS lbl,
    LOWER(COALESCE(di.fluid, '')) AS fl,
    LOWER(REPLACE(COALESCE(le.valueuom, ''), ' ', '')) AS uom_nospace,
    SAFE_CAST(le.valuenum AS FLOAT64) AS val
  FROM `{PHYS}.{HOSP}.labevents` le
  JOIN hadms h ON h.hadm_id = le.hadm_id
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.valuenum IS NOT NULL
    AND (
      LOWER(COALESCE(di.category, '')) LIKE '%blood gas%'
      OR REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b|\\bph\\b')
    )
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
),
hosp_spec AS (
  SELECT
    le.specimen_id,
    LOWER(COALESCE(le.value, '')) AS spec_val
  FROM `{PHYS}.{HOSP}.labevents` le
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.specimen_id IS NOT NULL
    AND REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(specimen|sample)')
),
hosp_tag AS (
  SELECT
    c.hadm_id,
    c.specimen_id,
    c.charttime,
    c.itemid,
    c.lbl,
    c.uom_nospace,
    c.val,
    CASE
      WHEN REGEXP_CONTAINS(c.lbl, r'\\bph\\b') THEN 'ph'
      WHEN REGEXP_CONTAINS(c.lbl, r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b') THEN 'pco2'
      ELSE NULL
    END AS analyte,
    CASE
      WHEN c.fl LIKE '%arterial%' OR REGEXP_CONTAINS(c.lbl, r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN c.fl LIKE '%ven%' OR REGEXP_CONTAINS(c.lbl, r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'arter') THEN 'arterial'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'ven') THEN 'venous'
      ELSE 'unknown'
    END AS site
  FROM hosp_cand c
  LEFT JOIN hosp_spec s USING (specimen_id)
),
hosp_panel AS (
  SELECT
    hadm_id,
    specimen_id,
    MIN(charttime) AS sample_time,
    ANY_VALUE(site) AS site,
    MAX(IF(analyte = 'ph', val, NULL)) AS ph,
    MAX(IF(analyte = 'pco2', val, NULL)) AS pco2_raw,
    ANY_VALUE(IF(analyte = 'pco2', uom_nospace, NULL)) AS pco2_uom
  FROM hosp_tag
  WHERE analyte IN ('ph', 'pco2')
  GROUP BY hadm_id, specimen_id
),
hosp_std AS (
  SELECT
    hadm_id,
    sample_time AS charttime,
    site,
    CASE WHEN pco2_uom = 'kpa' THEN pco2_raw * 7.50062 ELSE pco2_raw END AS pco2_mmhg
  FROM hosp_panel
  WHERE site IN ('arterial', 'venous')
    AND (ph IS NOT NULL OR pco2_raw IS NOT NULL)
    AND (pco2_raw IS NULL OR (CASE WHEN pco2_uom = 'kpa' THEN pco2_raw * 7.50062 ELSE pco2_raw END) BETWEEN 5 AND 200)
),
icu_cand AS (
  SELECT
    ce.hadm_id,
    ce.charttime,
    ce.itemid,
    LOWER(COALESCE(di.label, '')) AS lbl,
    SAFE_CAST(ce.valuenum AS FLOAT64) AS val,
    LOWER(REPLACE(COALESCE(ce.valueuom, ''), ' ', '')) AS uom_nospace,
    LOWER(COALESCE(CAST(ce.value AS STRING), '')) AS valstr,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bph\\b') THEN 'ph'
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b') THEN 'pco2'
      ELSE NULL
    END AS analyte,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      ELSE 'unknown'
    END AS site
  FROM `{PHYS}.{ICU}.chartevents` ce
  JOIN hadms h ON h.hadm_id = ce.hadm_id
  LEFT JOIN `{PHYS}.{ICU}.d_items` di
    ON di.itemid = ce.itemid
  WHERE ce.valuenum IS NOT NULL
    AND (
      REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b')
      OR REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bph\\b')
    )
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
),
icu_pairs AS (
  SELECT
    p.hadm_id,
    LEAST(p.charttime, c.charttime) AS charttime,
    COALESCE(NULLIF(p.site, 'unknown'), c.site) AS site,
    CASE
      WHEN c.uom_nospace = 'kpa' OR REGEXP_CONTAINS(c.valstr, r'\\bkpa\\b') THEN c.val * 7.50062
      ELSE c.val
    END AS pco2_mmhg
  FROM icu_cand p
  JOIN icu_cand c
    ON p.hadm_id = c.hadm_id
   AND p.analyte = 'ph'
   AND c.analyte = 'pco2'
   AND ABS(TIMESTAMP_DIFF(p.charttime, c.charttime, MINUTE)) <= 120
   AND COALESCE(NULLIF(p.site, 'unknown'), c.site) IN ('arterial', 'venous')
),
icu_solo AS (
  SELECT
    hadm_id,
    charttime,
    site,
    CASE
      WHEN uom_nospace = 'kpa' OR REGEXP_CONTAINS(valstr, r'\\bkpa\\b') THEN val * 7.50062
      ELSE val
    END AS pco2_mmhg
  FROM icu_cand
  WHERE analyte = 'pco2'
    AND site IN ('arterial', 'venous')
),
all_std AS (
  SELECT hadm_id, charttime, site, pco2_mmhg FROM hosp_std
  UNION ALL
  SELECT hadm_id, charttime, site, pco2_mmhg FROM icu_pairs
  UNION ALL
  SELECT hadm_id, charttime, site, pco2_mmhg FROM icu_solo
),
filtered AS (
  SELECT *
  FROM all_std
  WHERE pco2_mmhg BETWEEN 5 AND 200
)
SELECT
  hadm_id,
  MAX(IF(site = 'arterial' AND pco2_mmhg >= 45.0, 1, 0)) AS legacy_abg_hypercap,
  MAX(IF(site = 'venous' AND pco2_mmhg >= 50.0, 1, 0)) AS legacy_vbg_hypercap,
  COUNT(*) AS legacy_candidate_n
FROM filtered
GROUP BY hadm_id
"""


PERMISSIVE_ENVELOPE_LONG_SQL = """
WITH hadms AS (
  SELECT hadm_id FROM UNNEST(@hadms) AS hadm_id
),
hosp_raw AS (
  SELECT
    'HOSP' AS source_system,
    le.hadm_id,
    le.itemid,
    LOWER(COALESCE(di.label, '')) AS label,
    LOWER(COALESCE(di.fluid, '')) AS fluid,
    le.specimen_id,
    le.charttime,
    SAFE_CAST(le.valuenum AS FLOAT64) AS value_raw,
    LOWER(REPLACE(COALESCE(le.valueuom, ''), ' ', '')) AS unit_raw
  FROM `{PHYS}.{HOSP}.labevents` le
  JOIN hadms h ON h.hadm_id = le.hadm_id
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.valuenum IS NOT NULL
    AND (
      REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b')
      OR (
        REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bco\\s*2\\b')
        AND LOWER(COALESCE(di.category, '')) LIKE '%blood%'
      )
    )
    AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
),
hosp_spec AS (
  SELECT
    le.specimen_id,
    LOWER(COALESCE(le.value, '')) AS spec_val
  FROM `{PHYS}.{HOSP}.labevents` le
  LEFT JOIN `{PHYS}.{HOSP}.d_labitems` di
    ON di.itemid = le.itemid
  WHERE le.specimen_id IS NOT NULL
    AND REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(specimen|sample)')
),
hosp_std AS (
  SELECT
    source_system,
    r.hadm_id,
    r.itemid,
    r.label,
    r.charttime,
    r.unit_raw AS unit_norm,
    CASE WHEN r.unit_raw = 'kpa' THEN r.value_raw * 7.50062 ELSE r.value_raw END AS pco2_mmhg,
    CASE
      WHEN r.fluid LIKE '%arterial%' OR REGEXP_CONTAINS(r.label, r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN r.fluid LIKE '%ven%' OR REGEXP_CONTAINS(r.label, r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'arter') THEN 'arterial'
      WHEN REGEXP_CONTAINS(COALESCE(s.spec_val, ''), r'ven') THEN 'venous'
      ELSE 'unknown'
    END AS site
  FROM hosp_raw r
  LEFT JOIN hosp_spec s USING (specimen_id)
),
icu_raw AS (
  SELECT
    'ICU' AS source_system,
    ce.hadm_id,
    ce.itemid,
    LOWER(COALESCE(di.label, '')) AS label,
    ce.charttime,
    SAFE_CAST(ce.valuenum AS FLOAT64) AS value_raw,
    LOWER(REPLACE(COALESCE(ce.valueuom, ''), ' ', '')) AS unit_raw,
    LOWER(COALESCE(CAST(ce.value AS STRING), '')) AS value_str
  FROM `{PHYS}.{ICU}.chartevents` ce
  JOIN hadms h ON h.hadm_id = ce.hadm_id
  LEFT JOIN `{PHYS}.{ICU}.d_items` di
    ON di.itemid = ce.itemid
  WHERE ce.valuenum IS NOT NULL
    AND (
      REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bpco2\\b|\\bpaco2\\b|\\bp\\s*co(?:2|₂)\\b')
      OR (
        REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'\\bco\\s*2\\b')
        AND NOT REGEXP_CONTAINS(LOWER(COALESCE(di.label, '')), r'(total\\s*co2|tco2|etco2|end[- ]?tidal|hco3|bicar)')
      )
    )
),
icu_std AS (
  SELECT
    source_system,
    hadm_id,
    itemid,
    label,
    charttime,
    unit_raw AS unit_norm,
    CASE WHEN unit_raw = 'kpa' OR REGEXP_CONTAINS(value_str, r'\\bkpa\\b') THEN value_raw * 7.50062 ELSE value_raw END AS pco2_mmhg,
    CASE
      WHEN REGEXP_CONTAINS(label, r'\\b(abg|art|arterial|a[- ]?line)\\b') THEN 'arterial'
      WHEN REGEXP_CONTAINS(label, r'\\b(vbg|ven|venous|mixed|central)\\b') THEN 'venous'
      ELSE 'unknown'
    END AS site
  FROM icu_raw
),
all_std AS (
  SELECT source_system, hadm_id, itemid, label, charttime, unit_norm, pco2_mmhg, site FROM hosp_std
  UNION ALL
  SELECT source_system, hadm_id, itemid, label, charttime, unit_norm, pco2_mmhg, site FROM icu_std
)
SELECT
  source_system,
  hadm_id,
  itemid,
  label,
  charttime,
  unit_norm,
  pco2_mmhg,
  site,
  CAST(pco2_mmhg BETWEEN 5 AND 200 AS INT64) AS in_range,
  CAST(site = 'unknown' AS INT64) AS is_unknown_site,
  CAST(site IN ('arterial', 'venous') AS INT64) AS is_known_site,
  CAST(pco2_mmhg >= 45 AS INT64) AS ge_45,
  CAST(site = 'arterial' AND pco2_mmhg >= 45 AS INT64) AS arterial_ge_45,
  CAST(site = 'venous' AND pco2_mmhg >= 50 AS INT64) AS venous_ge_50
FROM all_std
"""


PERMISSIVE_ENVELOPE_HADM_SQL = """
WITH long AS (
  {permissive_long_sql}
)
SELECT
  hadm_id,
  COUNT(*) AS env_candidate_n,
  SUM(in_range) AS env_in_range_n,
  SUM(is_known_site) AS env_known_site_n,
  SUM(is_unknown_site) AS env_unknown_site_n,
  SUM(arterial_ge_45) AS env_abg_ge45_n,
  SUM(venous_ge_50) AS env_vbg_ge50_n,
  SUM(ge_45) AS env_any_ge45_n,
  SUM(CASE WHEN unit_norm = 'kpa' THEN 1 ELSE 0 END) AS env_kpa_n
FROM long
GROUP BY hadm_id
"""


def build_sql_registry(PHYS: str, HOSP: str, ICU: str) -> dict[str, str]:
    """Return fully rendered SQL strings for debug notebooks."""
    permissive_long = _fmt(PERMISSIVE_ENVELOPE_LONG_SQL, PHYS=PHYS, HOSP=HOSP, ICU=ICU)
    permissive_hadm = dedent(PERMISSIVE_ENVELOPE_HADM_SQL).format(
        permissive_long_sql=permissive_long
    ).strip()
    return {
        "current_strict": _fmt(CURRENT_STRICT_SQL, PHYS=PHYS, HOSP=HOSP, ICU=ICU),
        "legacy_replay": _fmt(LEGACY_REPLAY_SQL, PHYS=PHYS, HOSP=HOSP, ICU=ICU),
        "permissive_envelope_long": permissive_long,
        "permissive_envelope_hadm": permissive_hadm,
    }
