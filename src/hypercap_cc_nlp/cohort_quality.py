"""Quality helpers for cohort notebook data contracts and diagnostics."""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

import pandas as pd

OMR_RESULT_NAMES = ("bmi", "height", "weight")
OMR_OUTPUT_COLUMNS = (
    "bmi_closest_pre_ed",
    "height_closest_pre_ed",
    "weight_closest_pre_ed",
)

OMR_PROVENANCE_COLUMNS = (
    "anthro_timing_tier",
    "anthro_days_offset",
    "anthro_chartdate",
    "anthro_timing_uncertain",
    "anthro_source",
    "anthro_obstime",
    "anthro_hours_offset",
    "anthro_timing_basis",
)

OMR_TIMING_TIERS = ("pre_ed_365", "post_ed_365", "missing")

EXPECTED_STRUCTURAL_NULL_FIELDS = (
    "poc_abg_ph_uom",
    "poc_vbg_ph_uom",
    "poc_other_ph_uom",
)

PACO2_VALUE_UOM_PAIRS = (
    ("poc_abg_paco2", "poc_abg_paco2_uom"),
    ("poc_vbg_paco2", "poc_vbg_paco2_uom"),
    ("poc_other_paco2", "poc_other_paco2_uom"),
)

DEFAULT_VITALS_MODEL_RANGES: dict[str, tuple[float, float]] = {
    "ed_triage_hr": (20.0, 250.0),
    "ed_first_hr": (20.0, 250.0),
    "ed_triage_rr": (4.0, 80.0),
    "ed_first_rr": (4.0, 80.0),
    "ed_triage_sbp": (40.0, 300.0),
    "ed_first_sbp": (40.0, 300.0),
    "ed_triage_dbp": (20.0, 200.0),
    "ed_first_dbp": (20.0, 200.0),
    "ed_triage_o2sat": (40.0, 100.0),
    "ed_first_o2sat": (40.0, 100.0),
    "ed_triage_temp": (90.0, 110.0),
    "ed_first_temp": (90.0, 110.0),
}

DEFAULT_GAS_MODEL_RANGES: dict[str, tuple[float, float]] = {
    "first_ph": (6.8, 7.8),
    "first_pco2": (10.0, 200.0),
    "first_other_pco2": (10.0, 200.0),
    "first_lactate": (0.0, 30.0),
}

_NUMERIC_TOKEN_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)")
_ARTERIAL_HINT_PATTERN = re.compile(
    r"(arterial|abg|a[- ]?line|art line|\bart\b|\bartery\b)",
    re.I,
)
_VENOUS_HINT_PATTERN = re.compile(
    r"(venous|vbg|cvbg|mixed venous|central venous|\bven\b)",
    re.I,
)


def _to_int64(series: pd.Series) -> pd.Series:
    """Return pandas nullable integer series for key columns."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def prepare_omr_records(omr_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize OMR records into a deterministic schema.

    Required source columns: ``subject_id``, ``chartdate``, ``result_name``,
    ``result_value``.

    Returns:
        DataFrame with columns:
        ``subject_id`` (Int64), ``chartdate_dt`` (datetime64),
        ``result_name`` (lowercase), and ``result_value_num`` (float).
    """
    required = {"subject_id", "chartdate", "result_name", "result_value"}
    missing = sorted(required.difference(omr_df.columns))
    if missing:
        raise KeyError(f"prepare_omr_records missing required columns: {missing}")

    prepared = omr_df.copy()
    prepared["subject_id"] = _to_int64(prepared["subject_id"])
    prepared["chartdate_dt"] = pd.to_datetime(prepared["chartdate"], errors="coerce")
    prepared["result_name"] = (
        prepared["result_name"].astype(str).str.strip().str.lower()
    )
    prepared["result_value_num"] = pd.to_numeric(
        prepared["result_value"]
        .astype(str)
        .str.extract(_NUMERIC_TOKEN_PATTERN, expand=False),
        errors="coerce",
    )

    prepared = prepared.loc[prepared["result_name"].isin(OMR_RESULT_NAMES)].copy()
    prepared = prepared.loc[
        prepared["subject_id"].notna() & prepared["chartdate_dt"].notna()
    ].copy()

    return prepared[["subject_id", "chartdate_dt", "result_name", "result_value_num"]]


def attach_closest_pre_ed_omr(
    ed_df: pd.DataFrame,
    omr_df: pd.DataFrame,
    window_days: int = 365,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Attach closest anthropometrics with tiered timing fallback.

    Args:
        ed_df: ED-stay grain dataframe with ``ed_stay_id``, ``subject_id``,
            and ``ed_intime``.
        omr_df: Prepared OMR records from ``prepare_omr_records``.
        window_days: Inclusion window in days before/after ED arrival.
    """

    def _with_default_anthro_columns(frame: pd.DataFrame) -> pd.DataFrame:
        updated = frame.copy()
        for column_name in OMR_OUTPUT_COLUMNS:
            if column_name not in updated.columns:
                updated[column_name] = pd.NA
        if "anthro_timing_tier" not in updated.columns:
            updated["anthro_timing_tier"] = "missing"
        else:
            updated["anthro_timing_tier"] = updated["anthro_timing_tier"].fillna("missing")
        if "anthro_days_offset" not in updated.columns:
            updated["anthro_days_offset"] = pd.Series([pd.NA] * len(updated), dtype="Int64")
        else:
            updated["anthro_days_offset"] = pd.to_numeric(
                updated["anthro_days_offset"], errors="coerce"
            ).astype("Int64")
        if "anthro_chartdate" not in updated.columns:
            updated["anthro_chartdate"] = pd.NaT
        else:
            updated["anthro_chartdate"] = pd.to_datetime(
                updated["anthro_chartdate"], errors="coerce"
            )
        if "anthro_timing_uncertain" not in updated.columns:
            updated["anthro_timing_uncertain"] = pd.Series(
                [pd.NA] * len(updated), dtype="boolean"
            )
        else:
            updated["anthro_timing_uncertain"] = updated[
                "anthro_timing_uncertain"
            ].astype("boolean")
        if "anthro_source" not in updated.columns:
            updated["anthro_source"] = "missing"
        else:
            updated["anthro_source"] = (
                updated["anthro_source"].astype(str).replace({"": "missing"}).fillna("missing")
            )
        if "anthro_obstime" not in updated.columns:
            updated["anthro_obstime"] = pd.NaT
        else:
            updated["anthro_obstime"] = pd.to_datetime(updated["anthro_obstime"], errors="coerce")
        if "anthro_hours_offset" not in updated.columns:
            updated["anthro_hours_offset"] = pd.NA
        updated["anthro_hours_offset"] = pd.to_numeric(
            updated["anthro_hours_offset"], errors="coerce"
        )
        if "anthro_timing_basis" not in updated.columns:
            updated["anthro_timing_basis"] = "missing"
        else:
            updated["anthro_timing_basis"] = (
                updated["anthro_timing_basis"].astype(str).replace({"": "missing"}).fillna("missing")
            )

        missing_mask = updated["anthro_timing_tier"].eq("missing")
        pre_mask = updated["anthro_timing_tier"].eq("pre_ed_365")
        post_mask = updated["anthro_timing_tier"].eq("post_ed_365")
        updated.loc[pre_mask, "anthro_timing_uncertain"] = False
        updated.loc[post_mask, "anthro_timing_uncertain"] = True
        updated.loc[missing_mask, "anthro_timing_uncertain"] = pd.NA
        updated.loc[pre_mask | post_mask, "anthro_source"] = "omr"
        updated.loc[missing_mask, "anthro_source"] = "missing"
        updated.loc[pre_mask, "anthro_timing_basis"] = "pre"
        updated.loc[post_mask, "anthro_timing_basis"] = "post"
        updated.loc[missing_mask, "anthro_timing_basis"] = "missing"
        updated.loc[missing_mask, "anthro_obstime"] = pd.NaT
        updated.loc[missing_mask, "anthro_hours_offset"] = pd.NA

        return updated

    def _tier_counts(frame: pd.DataFrame) -> dict[str, int]:
        tier_series = frame["anthro_timing_tier"].astype(str).fillna("missing")
        return {
            "pre_ed_365": int((tier_series == "pre_ed_365").sum()),
            "post_ed_365": int((tier_series == "post_ed_365").sum()),
            "missing": int((tier_series == "missing").sum()),
        }

    required_ed = {"ed_stay_id", "subject_id", "ed_intime"}
    missing_ed = sorted(required_ed.difference(ed_df.columns))
    if missing_ed:
        raise KeyError(f"attach_closest_pre_ed_omr missing ED columns: {missing_ed}")

    required_omr = {"subject_id", "chartdate_dt", "result_name", "result_value_num"}
    missing_omr = sorted(required_omr.difference(omr_df.columns))
    if missing_omr:
        raise KeyError(f"attach_closest_pre_ed_omr missing OMR columns: {missing_omr}")

    diagnostics: dict[str, Any] = {
        "window_days": int(window_days),
        "source_rows_prepared": int(len(omr_df)),
        "parsed_value_rows": int(omr_df["result_value_num"].notna().sum()),
        "ed_rows": int(len(ed_df)),
    }

    empty_window_diagnostics = {
        "nonnegative_candidate_rows": 0,
        "pre_window_candidate_rows": 0,
        "post_window_candidate_rows": 0,
        "closest_absolute_candidate_rows": 0,
        "within_window_candidate_rows": 0,
    }

    ed_norm = ed_df[["ed_stay_id", "subject_id", "ed_intime"]].copy()
    ed_norm["subject_id"] = _to_int64(ed_norm["subject_id"])
    ed_norm["ed_intime_dt"] = pd.to_datetime(ed_norm["ed_intime"], errors="coerce")
    ed_norm["ed_date_dt"] = ed_norm["ed_intime_dt"].dt.floor("D")
    ed_norm = ed_norm.loc[ed_norm["subject_id"].notna() & ed_norm["ed_date_dt"].notna()]

    if omr_df.empty or ed_norm.empty:
        updated = _with_default_anthro_columns(ed_df)
        tier_counts = _tier_counts(updated)
        total_rows = max(int(len(updated)), 1)
        diagnostics.update(
            {
                "ed_rows_eligible_for_join": int(len(ed_norm)),
                "subject_overlap_count": 0,
                "candidate_rows_after_subject_join": 0,
                **empty_window_diagnostics,
                "within_window_candidate_rows": 0,
                "eligible_ed_stays_with_candidates": 0,
                "attached_non_null_counts": {
                    column_name: int(updated[column_name].notna().sum())
                    for column_name in OMR_OUTPUT_COLUMNS
                },
                "attached_any_non_null_rows": int(
                    updated[list(OMR_OUTPUT_COLUMNS)].notna().any(axis=1).sum()
                ),
                "selected_tier_counts": tier_counts,
                "selected_tier_rates": {
                    key: float(value / total_rows) for key, value in tier_counts.items()
                },
                "timing_uncertain_count": int(
                    updated["anthro_timing_uncertain"].fillna(False).sum()
                ),
            }
        )
        return updated, diagnostics

    omr_pivot = (
        omr_df.pivot_table(
            index=["subject_id", "chartdate_dt"],
            columns="result_name",
            values="result_value_num",
            aggfunc="first",
        )
        .reset_index()
        .copy()
    )

    shared_subjects = set(ed_norm["subject_id"].dropna().astype(int)).intersection(
        set(omr_pivot["subject_id"].dropna().astype(int))
    )
    diagnostics["ed_rows_eligible_for_join"] = int(len(ed_norm))
    diagnostics["subject_overlap_count"] = int(len(shared_subjects))

    merged = ed_norm.merge(omr_pivot, on="subject_id", how="left")
    diagnostics["candidate_rows_after_subject_join"] = int(
        merged["chartdate_dt"].notna().sum()
    )

    merged["days_before"] = (
        merged["ed_date_dt"] - pd.to_datetime(merged["chartdate_dt"], errors="coerce")
    ).dt.days
    valid_days = merged["days_before"].dropna()
    diagnostics["days_before_min"] = (
        int(valid_days.min()) if not valid_days.empty else None
    )
    diagnostics["days_before_max"] = (
        int(valid_days.max()) if not valid_days.empty else None
    )
    pre_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].ge(0)
        & merged["days_before"].le(window_days)
    )
    post_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].lt(0)
        & merged["days_before"].ge(-window_days)
    )
    abs_window_mask = (
        merged["days_before"].notna()
        & merged["days_before"].abs().le(window_days)
    )

    diagnostics["nonnegative_candidate_rows"] = int((merged["days_before"] >= 0).sum())
    diagnostics["pre_window_candidate_rows"] = int(pre_window_mask.sum())
    diagnostics["post_window_candidate_rows"] = int(post_window_mask.sum())
    diagnostics["closest_absolute_candidate_rows"] = int(abs_window_mask.sum())

    pre_candidates = merged.loc[pre_window_mask].copy()
    post_candidates = merged.loc[post_window_mask].copy()
    diagnostics["within_window_candidate_rows"] = int(len(pre_candidates))

    selected_parts: list[pd.DataFrame] = []
    selected_stays: set[Any] = set()

    if not pre_candidates.empty:
        pre_selected = (
            pre_candidates.sort_values(
                ["ed_stay_id", "days_before", "chartdate_dt"],
                ascending=[True, True, False],
            )
            .groupby("ed_stay_id", as_index=False)
            .first()
        )
        pre_selected["anthro_timing_tier"] = "pre_ed_365"
        selected_parts.append(pre_selected)
        selected_stays.update(pre_selected["ed_stay_id"].tolist())

    if not post_candidates.empty:
        post_candidates["days_after_abs"] = post_candidates["days_before"].abs()
        post_selected = (
            post_candidates.sort_values(
                ["ed_stay_id", "days_after_abs", "chartdate_dt"],
                ascending=[True, True, True],
            )
            .groupby("ed_stay_id", as_index=False)
            .first()
        )
        post_selected = post_selected.loc[
            ~post_selected["ed_stay_id"].isin(selected_stays)
        ].copy()
        if not post_selected.empty:
            post_selected["anthro_timing_tier"] = "post_ed_365"
            selected_parts.append(post_selected)

    if selected_parts:
        selected = pd.concat(selected_parts, ignore_index=True)
        selected = selected.rename(
            columns={
                "bmi": "bmi_closest_pre_ed",
                "height": "height_closest_pre_ed",
                "weight": "weight_closest_pre_ed",
                "chartdate_dt": "anthro_chartdate",
                "days_before": "anthro_days_offset",
            }
        )
        selected["anthro_chartdate"] = pd.to_datetime(
            selected["anthro_chartdate"], errors="coerce"
        )
        selected["anthro_days_offset"] = pd.to_numeric(
            selected["anthro_days_offset"], errors="coerce"
        ).astype("Int64")
        selected["anthro_timing_uncertain"] = selected["anthro_timing_tier"].eq(
            "post_ed_365"
        )
        selected["anthro_source"] = "omr"
        selected["anthro_obstime"] = pd.to_datetime(
            selected["anthro_chartdate"], errors="coerce"
        )
        selected["anthro_hours_offset"] = pd.to_numeric(
            selected["anthro_days_offset"], errors="coerce"
        ) * 24.0
        selected["anthro_timing_basis"] = selected["anthro_timing_tier"].map(
            {"pre_ed_365": "pre", "post_ed_365": "post"}
        ).fillna("missing")

        updates = selected[
            [
                "ed_stay_id",
                *OMR_OUTPUT_COLUMNS,
                *OMR_PROVENANCE_COLUMNS,
            ]
        ].copy()
        updated = ed_df.merge(updates, on="ed_stay_id", how="left")
    else:
        updated = ed_df.copy()

    updated = _with_default_anthro_columns(updated)

    diagnostics["eligible_ed_stays_with_candidates"] = int(
        updated["anthro_timing_tier"].isin({"pre_ed_365", "post_ed_365"}).sum()
    )
    diagnostics["attached_non_null_counts"] = {
        column_name: int(updated[column_name].notna().sum())
        for column_name in OMR_OUTPUT_COLUMNS
    }
    diagnostics["attached_any_non_null_rows"] = int(
        updated[list(OMR_OUTPUT_COLUMNS)].notna().any(axis=1).sum()
    )
    tier_counts = _tier_counts(updated)
    diagnostics["selected_tier_counts"] = tier_counts
    total_rows = max(int(len(updated)), 1)
    diagnostics["selected_tier_rates"] = {
        key: float(value / total_rows) for key, value in tier_counts.items()
    }
    diagnostics["timing_uncertain_count"] = int(
        updated["anthro_timing_uncertain"].fillna(False).sum()
    )
    diagnostics["anthro_source_counts"] = {
        str(key): int(value)
        for key, value in updated["anthro_source"].fillna("missing")
        .astype(str)
        .value_counts(dropna=False)
        .items()
    }

    return updated, diagnostics


def _infer_route_hint_text(value: object) -> str | None:
    text = "" if pd.isna(value) else str(value).strip().lower()
    if not text or text in {"nan", "none"}:
        return None
    if _ARTERIAL_HINT_PATTERN.search(text):
        return "arterial"
    if _VENOUS_HINT_PATTERN.search(text):
        return "venous"
    return None


def _resolve_route_hints(values: Sequence[str]) -> tuple[str | None, bool, int]:
    hints = [str(value).strip().lower() for value in values if str(value).strip()]
    arterial_n = sum(1 for value in hints if value == "arterial")
    venous_n = sum(1 for value in hints if value == "venous")
    conflict = arterial_n > 0 and venous_n > 0
    if conflict:
        return None, True, int(len(hints))
    if arterial_n > 0:
        return "arterial", False, int(len(hints))
    if venous_n > 0:
        return "venous", False, int(len(hints))
    return None, False, int(len(hints))


def infer_panel_gas_source_metadata(
    panel_df: pd.DataFrame,
    labs_df: pd.DataFrame,
    labitems_df: pd.DataFrame,
    *,
    specimen_source_itemids: Sequence[int] | None = None,
    pco2_itemids: Sequence[int] | None = None,
    mode: str = "metadata_only",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Infer gas panel source from metadata/text hints with tier diagnostics.

    Tier precedence:
      1. Specimen/source row text (``value_text`` from specimen/source itemids),
      2. ``d_labitems`` label/fluid hints,
      3. Panel co-occurrence hints (pCO2-labeled rows + free-text),
      4. Fallback ``other``.
    """
    if mode != "metadata_only":
        raise ValueError(
            "infer_panel_gas_source_metadata supports only mode='metadata_only'."
        )

    required_panel = {"ed_stay_id", "specimen_id"}
    missing_panel = sorted(required_panel.difference(panel_df.columns))
    if missing_panel:
        raise KeyError(
            "infer_panel_gas_source_metadata missing panel columns: "
            f"{missing_panel}"
        )
    required_labs = {"ed_stay_id", "specimen_id", "itemid"}
    missing_labs = sorted(required_labs.difference(labs_df.columns))
    if missing_labs:
        raise KeyError(
            "infer_panel_gas_source_metadata missing labs columns: "
            f"{missing_labs}"
        )
    required_labitems = {"itemid"}
    missing_labitems = sorted(required_labitems.difference(labitems_df.columns))
    if missing_labitems:
        raise KeyError(
            "infer_panel_gas_source_metadata missing labitems columns: "
            f"{missing_labitems}"
        )

    key_cols = ["ed_stay_id", "specimen_id"]
    panel = panel_df.copy()
    panel["source"] = pd.Series(pd.NA, index=panel.index, dtype="string")
    panel["source_inference_tier"] = "fallback_other"
    panel["source_hint_conflict"] = pd.Series(False, index=panel.index, dtype="boolean")
    panel["source_hint_count"] = pd.Series(pd.NA, index=panel.index, dtype="Int64")

    labs = labs_df.copy()
    labs["itemid"] = pd.to_numeric(labs["itemid"], errors="coerce").astype("Int64")
    labs["specimen_id"] = pd.to_numeric(labs["specimen_id"], errors="coerce").astype(
        "Int64"
    )
    labs = labs.loc[labs["itemid"].notna() & labs["specimen_id"].notna()].copy()
    if labs.empty:
        panel["source"] = "other"
        diagnostics = summarize_gas_source(panel)
        diagnostics.update(
            {
                "mode": mode,
                "resolved_rows": 0,
                "resolved_rate": 0.0,
                "unresolved_specimen_id_count": 0,
                "unresolved_specimen_id_examples": [],
                "unresolved_value_text_top": {},
                "unresolved_label_top": {},
            }
        )
        return panel, diagnostics

    if "value_text" in labs.columns:
        labs["value_text_norm"] = (
            labs["value_text"]
            .astype("string")
            .fillna("")
            .str.strip()
            .str.lower()
        )
    else:
        labs["value_text_norm"] = ""

    labitems = labitems_df.copy()
    labitems["itemid"] = pd.to_numeric(labitems["itemid"], errors="coerce").astype(
        "Int64"
    )
    for text_col in ("label", "fluid"):
        if text_col not in labitems.columns:
            labitems[text_col] = ""
    labitems["label_norm"] = labitems["label"].astype("string").fillna("").str.strip()
    labitems["fluid_norm"] = labitems["fluid"].astype("string").fillna("").str.strip()
    labitems["item_route_hint"] = (
        (labitems["label_norm"] + " " + labitems["fluid_norm"])
        .str.strip()
        .map(_infer_route_hint_text)
    )
    item_meta = (
        labitems.dropna(subset=["itemid"])
        .drop_duplicates(subset=["itemid"])
        .set_index("itemid")[["label_norm", "fluid_norm", "item_route_hint"]]
    )
    labs = labs.merge(item_meta, left_on="itemid", right_index=True, how="left")
    labs["item_route_hint"] = labs["item_route_hint"].astype("string")
    labs["text_route_hint"] = labs["value_text_norm"].map(_infer_route_hint_text).astype(
        "string"
    )
    labs["label_fluid_text"] = (
        labs["label_norm"].fillna("").astype(str).str.strip()
        + " | "
        + labs["fluid_norm"].fillna("").astype(str).str.strip()
    ).str.strip(" |")

    def _build_tier_mapping(
        frame: pd.DataFrame,
        *,
        hint_column: str,
        tier_name: str,
    ) -> pd.DataFrame:
        subset = frame[key_cols + [hint_column]].copy()
        subset = subset.loc[subset[hint_column].notna()].copy()
        if subset.empty:
            return pd.DataFrame(
                columns=[
                    *key_cols,
                    "tier_source",
                    "tier_conflict",
                    "tier_hint_count",
                    "tier_name",
                ]
            )

        grouped = (
            subset.groupby(key_cols, dropna=False)[hint_column]
            .agg(list)
            .reset_index(name="hints")
        )
        rows: list[dict[str, Any]] = []
        for _, grouped_row in grouped.iterrows():
            source, conflict, hint_count = _resolve_route_hints(grouped_row["hints"])
            rows.append(
                {
                    "ed_stay_id": grouped_row["ed_stay_id"],
                    "specimen_id": grouped_row["specimen_id"],
                    "tier_source": source,
                    "tier_conflict": bool(conflict),
                    "tier_hint_count": int(hint_count),
                    "tier_name": tier_name,
                }
            )
        resolved = pd.DataFrame(rows)
        return resolved.loc[resolved["tier_source"].notna()].reset_index(drop=True)

    specimen_source_itemid_set = {
        int(item) for item in (specimen_source_itemids or []) if pd.notna(item)
    }
    pco2_itemid_set = {int(item) for item in (pco2_itemids or []) if pd.notna(item)}

    tier1 = _build_tier_mapping(
        labs.loc[labs["itemid"].astype("Int64").isin(specimen_source_itemid_set)],
        hint_column="text_route_hint",
        tier_name="specimen_text",
    )
    tier2 = _build_tier_mapping(
        labs,
        hint_column="item_route_hint",
        tier_name="label_fluid",
    )
    tier3_candidates = labs.copy()
    if pco2_itemid_set:
        tier3_candidates = tier3_candidates.loc[
            tier3_candidates["itemid"].astype("Int64").isin(pco2_itemid_set)
            | tier3_candidates["text_route_hint"].notna()
        ].copy()
    tier3_candidates["panel_route_hint"] = tier3_candidates["item_route_hint"].fillna(
        tier3_candidates["text_route_hint"]
    )
    tier3 = _build_tier_mapping(
        tier3_candidates,
        hint_column="panel_route_hint",
        tier_name="panel_cooccurrence",
    )

    for tier_frame in (tier1, tier2, tier3):
        if tier_frame.empty:
            continue
        panel = panel.merge(
            tier_frame[
                key_cols
                + ["tier_source", "tier_conflict", "tier_hint_count", "tier_name"]
            ],
            on=key_cols,
            how="left",
        )
        assign_mask = panel["source"].isna() & panel["tier_source"].notna()
        panel.loc[assign_mask, "source"] = panel.loc[assign_mask, "tier_source"]
        panel.loc[assign_mask, "source_inference_tier"] = panel.loc[
            assign_mask, "tier_name"
        ]
        selected_conflict = panel.loc[assign_mask, "tier_conflict"].astype("boolean")
        panel.loc[assign_mask, "source_hint_conflict"] = selected_conflict.fillna(False)
        panel.loc[assign_mask, "source_hint_count"] = pd.to_numeric(
            panel.loc[assign_mask, "tier_hint_count"], errors="coerce"
        ).astype("Int64")
        panel = panel.drop(
            columns=["tier_source", "tier_conflict", "tier_hint_count", "tier_name"]
        )

    panel["source"] = panel["source"].fillna("other").astype("string")
    panel["source"] = panel["source"].replace({"unknown": "other"})
    panel.loc[panel["source"].eq("other"), "source_inference_tier"] = panel.loc[
        panel["source"].eq("other"), "source_inference_tier"
    ].replace({"": "fallback_other"}).fillna("fallback_other")
    panel["source_inference_tier"] = panel["source_inference_tier"].astype("string")
    panel["source_hint_conflict"] = (
        panel["source_hint_conflict"].fillna(False).astype("boolean")
    )

    diagnostics = summarize_gas_source(panel)
    total = max(int(len(panel)), 1)
    diagnostics["mode"] = mode
    diagnostics["resolved_rows"] = int(
        panel["source"].isin({"arterial", "venous"}).sum()
    )
    diagnostics["resolved_rate"] = float(diagnostics["resolved_rows"] / total)

    unresolved = panel.loc[panel["source"].eq("other"), key_cols].drop_duplicates()
    diagnostics["unresolved_specimen_id_count"] = int(unresolved["specimen_id"].nunique())
    diagnostics["unresolved_specimen_id_examples"] = (
        unresolved["specimen_id"].dropna().astype(int).head(20).tolist()
    )
    if unresolved.empty:
        diagnostics["unresolved_value_text_top"] = {}
        diagnostics["unresolved_label_top"] = {}
        return panel, diagnostics

    unresolved_labs = labs.merge(unresolved, on=key_cols, how="inner")
    unresolved_value_text = (
        unresolved_labs["value_text_norm"]
        .replace({"": pd.NA})
        .dropna()
        .astype(str)
        .value_counts()
        .head(15)
    )
    unresolved_label_text = (
        unresolved_labs["label_fluid_text"]
        .replace({"": pd.NA})
        .dropna()
        .astype(str)
        .value_counts()
        .head(15)
    )
    diagnostics["unresolved_value_text_top"] = {
        str(key): int(value) for key, value in unresolved_value_text.items()
    }
    diagnostics["unresolved_label_top"] = {
        str(key): int(value) for key, value in unresolved_label_text.items()
    }
    return panel, diagnostics


def summarize_gas_source(panel_df: pd.DataFrame) -> dict[str, Any]:
    """Summarize gas source composition from panel-level records."""
    total_rows = int(len(panel_df))
    if total_rows == 0:
        return {
            "panel_rows": 0,
            "source_present": bool("source" in panel_df.columns),
            "source_counts": {},
            "source_rates": {},
            "all_other_or_unknown": False,
            "tier_counts": {},
            "tier_rates": {},
        }

    if "source" not in panel_df.columns:
        return {
            "panel_rows": total_rows,
            "source_present": False,
            "source_counts": {},
            "source_rates": {},
            "all_other_or_unknown": True,
            "tier_counts": {},
            "tier_rates": {},
        }

    source = (
        panel_df["source"]
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": "other", "nan": "other", "none": "other"})
        .fillna("other")
    )
    source_counts = {str(key): int(value) for key, value in source.value_counts(dropna=False).items()}
    source_rates = {key: float(value / total_rows) for key, value in source_counts.items()}
    all_other_or_unknown = bool(
        total_rows > 0 and set(source_counts.keys()).issubset({"other", "unknown"})
    )
    tier_counts: dict[str, int] = {}
    tier_rates: dict[str, float] = {}
    if "source_inference_tier" in panel_df.columns:
        tier = (
            panel_df["source_inference_tier"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({"": "unknown", "nan": "unknown", "none": "unknown"})
            .fillna("unknown")
        )
        tier_counts = {
            str(key): int(value) for key, value in tier.value_counts(dropna=False).items()
        }
        tier_rates = {key: float(value / total_rows) for key, value in tier_counts.items()}

    return {
        "panel_rows": total_rows,
        "source_present": True,
        "source_counts": source_counts,
        "source_rates": source_rates,
        "all_other_or_unknown": all_other_or_unknown,
        "tier_counts": tier_counts,
        "tier_rates": tier_rates,
    }


def assert_gas_source_coverage(
    gas_source_audit: Mapping[str, Any],
    *,
    fail_on_all_other_source: bool = True,
) -> None:
    """Fail fast when source attribution collapses to all other/unknown."""
    if not fail_on_all_other_source:
        return
    if int(gas_source_audit.get("panel_rows", 0)) <= 0:
        return
    if bool(gas_source_audit.get("all_other_or_unknown", False)):
        raise ValueError(
            "Gas source attribution classified all panel rows as other/unknown. "
            "Set COHORT_FAIL_ON_ALL_OTHER_SOURCE=0 to bypass this guard."
        )


def build_gas_source_overlap_summary(ed_df: pd.DataFrame) -> pd.DataFrame:
    """Build ABG/VBG/UNKNOWN overlap counts and percentages."""
    frame = ed_df.copy()
    abg = pd.to_numeric(frame.get("abg_hypercap_threshold", 0), errors="coerce").fillna(0).astype(int)
    vbg = pd.to_numeric(frame.get("vbg_hypercap_threshold", 0), errors="coerce").fillna(0).astype(int)
    unknown = (
        pd.to_numeric(frame.get("unknown_hypercap_threshold", 0), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    labels = []
    for a, v, u in zip(abg.tolist(), vbg.tolist(), unknown.tolist(), strict=False):
        parts: list[str] = []
        if a == 1:
            parts.append("ABG")
        if v == 1:
            parts.append("VBG")
        if u == 1:
            parts.append("UNKNOWN")
        labels.append("+".join(parts) if parts else "NO_GAS")

    counts = pd.Series(labels, dtype="string").value_counts(dropna=False).rename_axis("gas_overlap").reset_index(name="count")
    total = max(int(counts["count"].sum()), 1)
    counts["percent"] = counts["count"].astype(float) / total * 100.0
    counts["percent"] = counts["percent"].round(2)
    return counts.sort_values(["count", "gas_overlap"], ascending=[False, True]).reset_index(drop=True)


def add_gas_model_fields(
    ed_df: pd.DataFrame,
    *,
    ranges: Mapping[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create cleaned gas model fields and a field-level outlier audit."""
    resolved_ranges = dict(ranges or DEFAULT_GAS_MODEL_RANGES)
    updated = ed_df.copy()
    audit_rows: list[dict[str, Any]] = []

    for raw_column, (lower_bound, upper_bound) in resolved_ranges.items():
        if raw_column not in updated.columns:
            continue
        numeric = pd.to_numeric(updated[raw_column], errors="coerce")
        out_of_range = numeric.notna() & ((numeric < lower_bound) | (numeric > upper_bound))
        model_column = f"{raw_column}_model"
        outlier_flag_column = f"{raw_column}_outlier_flag"
        updated[model_column] = numeric.where(~out_of_range)
        updated[outlier_flag_column] = out_of_range.astype("boolean")
        nonnull_n = int(numeric.notna().sum())
        outlier_n = int(out_of_range.sum())
        examples = (
            numeric.loc[out_of_range]
            .round(4)
            .value_counts()
            .head(5)
            .index.astype(str)
            .tolist()
        )
        audit_rows.append(
            {
                "domain": "gas",
                "raw_column": raw_column,
                "model_column": model_column,
                "outlier_flag_column": outlier_flag_column,
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "nonnull_n": nonnull_n,
                "out_of_range_n": outlier_n,
                "out_of_range_pct": float(outlier_n / nonnull_n) if nonnull_n else 0.0,
                "example_outlier_values": "; ".join(examples),
            }
        )

    audit = pd.DataFrame(audit_rows).sort_values("raw_column").reset_index(drop=True)
    return updated, audit


def attach_charted_anthro_fallback(
    ed_df: pd.DataFrame,
    charted_df: pd.DataFrame,
    *,
    nearest_anytime: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Fill missing anthropometrics from charted records with nearest-time selection."""
    required_ed = {"ed_stay_id", "subject_id", "ed_intime"}
    missing_ed = sorted(required_ed.difference(ed_df.columns))
    if missing_ed:
        raise KeyError(f"attach_charted_anthro_fallback missing ED columns: {missing_ed}")

    required_charted = {"subject_id", "obs_time", "result_name", "result_value_num"}
    missing_charted = sorted(required_charted.difference(charted_df.columns))
    if missing_charted:
        raise KeyError(
            "attach_charted_anthro_fallback missing charted columns: "
            f"{missing_charted}"
        )

    updated = ed_df.copy()
    for output_column in OMR_OUTPUT_COLUMNS:
        if output_column not in updated.columns:
            updated[output_column] = pd.NA
    if "anthro_source" not in updated.columns:
        updated["anthro_source"] = "missing"
    else:
        updated["anthro_source"] = (
            updated["anthro_source"].astype(str).replace({"": "missing"}).fillna("missing")
        )
    if "anthro_obstime" not in updated.columns:
        updated["anthro_obstime"] = pd.NaT
    else:
        updated["anthro_obstime"] = pd.to_datetime(updated["anthro_obstime"], errors="coerce")
    if "anthro_hours_offset" not in updated.columns:
        updated["anthro_hours_offset"] = pd.NA
    updated["anthro_hours_offset"] = pd.to_numeric(updated["anthro_hours_offset"], errors="coerce")
    if "anthro_timing_basis" not in updated.columns:
        updated["anthro_timing_basis"] = "missing"
    if "anthro_timing_uncertain" not in updated.columns:
        updated["anthro_timing_uncertain"] = pd.Series([pd.NA] * len(updated), dtype="boolean")
    else:
        updated["anthro_timing_uncertain"] = updated["anthro_timing_uncertain"].astype("boolean")

    charted = charted_df.copy()
    charted["subject_id"] = _to_int64(charted["subject_id"])
    charted["obs_time"] = pd.to_datetime(charted["obs_time"], errors="coerce")
    charted["result_name"] = charted["result_name"].astype(str).str.strip().str.lower()
    charted["result_value_num"] = pd.to_numeric(charted["result_value_num"], errors="coerce")
    if "source" not in charted.columns:
        charted["source"] = "icu_charted"
    charted["source"] = charted["source"].astype(str).replace({"": "icu_charted"}).fillna("icu_charted")
    charted = charted.loc[
        charted["subject_id"].notna()
        & charted["obs_time"].notna()
        & charted["result_name"].isin(OMR_RESULT_NAMES)
        & charted["result_value_num"].notna()
    ].copy()

    diagnostics: dict[str, Any] = {
        "charted_rows_input": int(len(charted_df)),
        "charted_rows_usable": int(len(charted)),
        "nearest_anytime": bool(nearest_anytime),
        "filled_counts": {column: 0 for column in OMR_OUTPUT_COLUMNS},
        "rows_with_any_charted_fill": 0,
    }
    if charted.empty:
        return updated, diagnostics

    ed_norm = updated[["ed_stay_id", "subject_id", "ed_intime"]].copy()
    ed_norm["subject_id"] = _to_int64(ed_norm["subject_id"])
    ed_norm["ed_intime_dt"] = pd.to_datetime(ed_norm["ed_intime"], errors="coerce")
    ed_norm = ed_norm.loc[ed_norm["subject_id"].notna() & ed_norm["ed_intime_dt"].notna()].copy()
    if ed_norm.empty:
        return updated, diagnostics

    merged = ed_norm.merge(charted, on="subject_id", how="inner")
    if merged.empty:
        diagnostics["subject_overlap_count"] = 0
        return updated, diagnostics
    diagnostics["subject_overlap_count"] = int(merged["subject_id"].nunique())
    merged["hours_offset"] = (
        (merged["obs_time"] - merged["ed_intime_dt"]).dt.total_seconds() / 3600.0
    )
    merged["abs_hours_offset"] = merged["hours_offset"].abs()
    if not nearest_anytime:
        merged = merged.loc[merged["abs_hours_offset"] <= 24.0].copy()

    output_to_name = {
        "bmi_closest_pre_ed": "bmi",
        "height_closest_pre_ed": "height",
        "weight_closest_pre_ed": "weight",
    }
    any_fill_mask = pd.Series(False, index=updated.index)
    for output_column, result_name in output_to_name.items():
        subset = merged.loc[merged["result_name"] == result_name].copy()
        if subset.empty:
            continue
        selected = (
            subset.sort_values(
                ["ed_stay_id", "abs_hours_offset", "obs_time"],
                ascending=[True, True, True],
            )
            .groupby("ed_stay_id", as_index=False)
            .first()
        )
        selected = selected.rename(
            columns={
                "result_value_num": f"{output_column}__candidate",
                "obs_time": f"{output_column}__obs_time",
                "hours_offset": f"{output_column}__hours_offset",
                "source": f"{output_column}__source",
            }
        )
        keep_columns = [
            "ed_stay_id",
            f"{output_column}__candidate",
            f"{output_column}__obs_time",
            f"{output_column}__hours_offset",
            f"{output_column}__source",
        ]
        updated = updated.merge(selected[keep_columns], on="ed_stay_id", how="left")

        fill_mask = updated[output_column].isna() & updated[f"{output_column}__candidate"].notna()
        updated.loc[fill_mask, output_column] = updated.loc[fill_mask, f"{output_column}__candidate"]

        provenance_mask = fill_mask & updated["anthro_source"].isin({"missing", "nan"})
        updated.loc[provenance_mask, "anthro_source"] = updated.loc[
            provenance_mask, f"{output_column}__source"
        ]
        updated.loc[provenance_mask, "anthro_obstime"] = pd.to_datetime(
            updated.loc[provenance_mask, f"{output_column}__obs_time"], errors="coerce"
        )
        updated.loc[provenance_mask, "anthro_hours_offset"] = pd.to_numeric(
            updated.loc[provenance_mask, f"{output_column}__hours_offset"], errors="coerce"
        )
        updated.loc[provenance_mask, "anthro_timing_basis"] = "nearest_anytime"
        updated.loc[provenance_mask, "anthro_timing_uncertain"] = True

        any_fill_mask = any_fill_mask | fill_mask
        diagnostics["filled_counts"][output_column] = int(fill_mask.sum())

        drop_columns = [
            f"{output_column}__candidate",
            f"{output_column}__obs_time",
            f"{output_column}__hours_offset",
            f"{output_column}__source",
        ]
        updated = updated.drop(columns=drop_columns)

    diagnostics["rows_with_any_charted_fill"] = int(any_fill_mask.sum())
    diagnostics["fallback_source_counts"] = {
        str(key): int(value)
        for key, value in updated.loc[any_fill_mask, "anthro_source"]
        .fillna("missing")
        .astype(str)
        .value_counts(dropna=False)
        .items()
    }
    return updated, diagnostics


def build_anthro_coverage_audit(ed_df: pd.DataFrame) -> dict[str, Any]:
    """Summarize anthropometric coverage and provenance rates."""
    total_rows = max(int(len(ed_df)), 1)
    field_counts = {
        column: int(pd.to_numeric(ed_df[column], errors="coerce").notna().sum())
        for column in OMR_OUTPUT_COLUMNS
        if column in ed_df.columns
    }
    field_rates = {column: float(count / total_rows) for column, count in field_counts.items()}

    source_counts: dict[str, int] = {}
    source_rates: dict[str, float] = {}
    if "anthro_source" in ed_df.columns:
        source_counts = {
            str(key): int(value)
            for key, value in ed_df["anthro_source"]
            .fillna("missing")
            .astype(str)
            .value_counts(dropna=False)
            .items()
        }
        source_rates = {key: float(value / total_rows) for key, value in source_counts.items()}

    timing_basis_counts: dict[str, int] = {}
    timing_basis_rates: dict[str, float] = {}
    if "anthro_timing_basis" in ed_df.columns:
        timing_basis_counts = {
            str(key): int(value)
            for key, value in ed_df["anthro_timing_basis"]
            .fillna("missing")
            .astype(str)
            .value_counts(dropna=False)
            .items()
        }
        timing_basis_rates = {
            key: float(value / total_rows) for key, value in timing_basis_counts.items()
        }

    return {
        "row_count": int(len(ed_df)),
        "field_nonnull_counts": field_counts,
        "field_nonnull_rates": field_rates,
        "source_counts": source_counts,
        "source_rates": source_rates,
        "timing_basis_counts": timing_basis_counts,
        "timing_basis_rates": timing_basis_rates,
    }


def build_first_other_pco2_audit(ed_df: pd.DataFrame) -> pd.DataFrame:
    """Build route-stratified audit summary for first_other_pco2 values.

    This helper is intentionally tolerant for notebook QA execution: if the
    required fields are not present, it returns a sentinel audit row instead of
    raising.
    """
    columns = [
        "source",
        "count_nonnull",
        "mean",
        "median",
        "q25",
        "q75",
        "p95",
        "max",
        "pct_ge_80",
        "pct_ge_100",
        "pct_ge_150",
        "pct_eq_160",
        "top_values",
        "status",
        "missing_columns",
    ]
    required = {"first_other_pco2", "first_other_src"}
    missing = sorted(required.difference(ed_df.columns))
    if missing:
        return pd.DataFrame(
            [
                {
                    "source": "UNAVAILABLE",
                    "count_nonnull": 0,
                    "mean": None,
                    "median": None,
                    "q25": None,
                    "q75": None,
                    "p95": None,
                    "max": None,
                    "pct_ge_80": 0.0,
                    "pct_ge_100": 0.0,
                    "pct_ge_150": 0.0,
                    "pct_eq_160": 0.0,
                    "top_values": {},
                    "status": "missing_columns",
                    "missing_columns": ",".join(missing),
                }
            ],
            columns=columns,
        )

    frame = ed_df[["first_other_src", "first_other_pco2"]].copy()
    frame["first_other_src"] = (
        frame["first_other_src"].astype(str).str.strip().str.upper().replace({"": "UNKNOWN", "NAN": "UNKNOWN"})
    )
    frame["first_other_pco2"] = pd.to_numeric(frame["first_other_pco2"], errors="coerce")
    frame = frame.loc[frame["first_other_pco2"].notna()].copy()

    if frame.empty:
        return pd.DataFrame(
            [
                {
                    "source": "UNAVAILABLE",
                    "count_nonnull": 0,
                    "mean": None,
                    "median": None,
                    "q25": None,
                    "q75": None,
                    "p95": None,
                    "max": None,
                    "pct_ge_80": 0.0,
                    "pct_ge_100": 0.0,
                    "pct_ge_150": 0.0,
                    "pct_eq_160": 0.0,
                    "top_values": {},
                    "status": "no_nonnull_values",
                    "missing_columns": "",
                }
            ],
            columns=columns,
        )

    rows: list[dict[str, Any]] = []
    for source_name, group in frame.groupby("first_other_src"):
        values = group["first_other_pco2"]
        rows.append(
            {
                "source": source_name,
                "count_nonnull": int(values.shape[0]),
                "mean": float(values.mean()),
                "median": float(values.median()),
                "q25": float(values.quantile(0.25)),
                "q75": float(values.quantile(0.75)),
                "p95": float(values.quantile(0.95)),
                "max": float(values.max()),
                "pct_ge_80": float((values >= 80).mean()),
                "pct_ge_100": float((values >= 100).mean()),
                "pct_ge_150": float((values >= 150).mean()),
                "pct_eq_160": float((values == 160).mean()),
                "top_values": {
                    str(key): int(value)
                    for key, value in values.value_counts().head(10).items()
                },
                "status": "ok",
                "missing_columns": "",
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("source").reset_index(drop=True)


def add_vitals_model_fields(
    ed_df: pd.DataFrame,
    *,
    ranges: Mapping[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create cleaned vitals model fields and return field-level outlier audit.

    The function preserves original raw columns and writes cleaned values to
    ``<raw_column>_model``. For temperature columns, it also writes explicit
    Fahrenheit/Celsius variants:
    - ``<raw_column>_f_model`` (native Fahrenheit cleaned values)
    - ``<raw_column>_c_model`` (derived Celsius)
    Out-of-range values are nulled in model fields.
    """
    resolved_ranges = dict(ranges or DEFAULT_VITALS_MODEL_RANGES)
    updated = ed_df.copy()
    audit_rows: list[dict[str, Any]] = []

    for raw_column, (lower_bound, upper_bound) in resolved_ranges.items():
        if raw_column not in updated.columns:
            continue
        numeric = pd.to_numeric(updated[raw_column], errors="coerce")
        out_of_range = numeric.notna() & (
            (numeric < lower_bound) | (numeric > upper_bound)
        )
        model_column = f"{raw_column}_model"
        outlier_flag_column = f"{raw_column}_outlier_flag"
        cleaned = numeric.where(~out_of_range)
        if raw_column.endswith("_temp"):
            temp_f_column = f"{raw_column}_f_model"
            temp_c_column = f"{raw_column}_c_model"
            updated[temp_f_column] = cleaned
            updated[temp_c_column] = (updated[temp_f_column] - 32.0) * (5.0 / 9.0)
            updated[model_column] = updated[temp_f_column]
        else:
            updated[model_column] = cleaned
        updated[outlier_flag_column] = out_of_range.astype("boolean")
        nonnull_n = int(numeric.notna().sum())
        outlier_n = int(out_of_range.sum())
        examples = (
            numeric.loc[out_of_range]
            .round(4)
            .value_counts()
            .head(5)
            .index.astype(str)
            .tolist()
        )
        audit_rows.append(
            {
                "domain": "vitals",
                "raw_column": raw_column,
                "model_column": model_column,
                "outlier_flag_column": outlier_flag_column,
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "nonnull_n": nonnull_n,
                "out_of_range_n": outlier_n,
                "out_of_range_pct": float(outlier_n / nonnull_n) if nonnull_n else 0.0,
                "example_outlier_values": "; ".join(examples),
            }
        )

    audit = pd.DataFrame(audit_rows).sort_values("raw_column").reset_index(drop=True)
    return updated, audit


def evaluate_uom_expectations(ed_df: pd.DataFrame) -> dict[str, Any]:
    """Evaluate expected-null and value/uom consistency rules."""
    structural_nulls: dict[str, dict[str, Any]] = {}
    for field_name in EXPECTED_STRUCTURAL_NULL_FIELDS:
        if field_name not in ed_df.columns:
            continue
        structural_nulls[field_name] = {
            "present": True,
            "missing_n": int(ed_df[field_name].isna().sum()),
            "missing_pct": float(ed_df[field_name].isna().mean()),
            "all_null": bool(ed_df[field_name].isna().all()),
        }

    paco2_checks: dict[str, dict[str, Any]] = {}
    for value_column, uom_column in PACO2_VALUE_UOM_PAIRS:
        if value_column not in ed_df.columns or uom_column not in ed_df.columns:
            paco2_checks[uom_column] = {
                "present": False,
                "reason": "missing_value_or_uom_column",
            }
            continue

        value_present = ed_df[value_column].notna()
        uom_lower = ed_df[uom_column].astype(str).str.strip().str.lower()
        missing_uom_with_value = int((value_present & ed_df[uom_column].isna()).sum())
        non_mmhg_uom_with_value = int(
            (value_present & ed_df[uom_column].notna() & uom_lower.ne("mmhg")).sum()
        )

        paco2_checks[uom_column] = {
            "present": True,
            "paired_value_column": value_column,
            "value_rows": int(value_present.sum()),
            "missing_uom_when_value_present": missing_uom_with_value,
            "non_mmhg_uom_when_value_present": non_mmhg_uom_with_value,
            "passes": bool(
                missing_uom_with_value == 0 and non_mmhg_uom_with_value == 0
            ),
        }

    return {
        "expected_structural_null_fields": list(EXPECTED_STRUCTURAL_NULL_FIELDS),
        "structural_null_checks": structural_nulls,
        "paco2_uom_checks": paco2_checks,
    }


def classify_missingness_expectations(
    ed_df: pd.DataFrame,
    target_fields: list[str],
    *,
    expected_sparse_fields: set[str] | None = None,
) -> pd.DataFrame:
    """Classify field-level missingness into expected and unexpected categories."""
    rows: list[dict[str, Any]] = []
    total_rows = max(int(len(ed_df)), 1)
    expected_structural = set(EXPECTED_STRUCTURAL_NULL_FIELDS)
    expected_sparse = set(expected_sparse_fields or set())

    for field_name in target_fields:
        if field_name not in ed_df.columns:
            rows.append(
                {
                    "field": field_name,
                    "missing_n": int(len(ed_df)),
                    "missing_pct": 1.0,
                    "expectation": "missing_column",
                }
            )
            continue

        missing_n = int(ed_df[field_name].isna().sum())
        missing_pct = float(missing_n / total_rows)

        if field_name in expected_structural:
            expectation = "expected_structural_null"
        elif field_name in expected_sparse and missing_pct >= 1.0:
            expectation = "expected_sparse"
        elif missing_pct >= 1.0:
            expectation = "unexpected_full_null"
        elif missing_pct > 0.0:
            expectation = "conditional_sparse"
        else:
            expectation = "complete"

        rows.append(
            {
                "field": field_name,
                "missing_n": missing_n,
                "missing_pct": missing_pct,
                "expectation": expectation,
            }
        )

    return pd.DataFrame(rows)
