"""Core helpers for rater-to-NLP overlap auditing."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .workflow_contracts import ensure_required_columns


def normalize_join_keys(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """Normalize join key columns to pandas nullable integers."""
    ensure_required_columns(df, key_cols, context="rater join input")
    normalized = df.copy()
    for key in key_cols:
        normalized[key] = pd.to_numeric(normalized[key], errors="coerce").astype("Int64")
    return normalized


def _validate_unique_keys(df: pd.DataFrame, key_cols: list[str], *, context: str) -> None:
    """Validate key uniqueness after normalization."""
    duplicates = int(df.duplicated(subset=key_cols).sum())
    if duplicates:
        raise ValueError(
            f"{context} has {duplicates} duplicate rows for keys {key_cols}. "
            "Expected unique keys before merge."
        )


def _anti_join_keys(left_df: pd.DataFrame, right_df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """Return unique key rows in ``left_df`` absent from ``right_df``."""
    right_keys = right_df[key_cols].drop_duplicates()
    only_left = (
        left_df[key_cols]
        .drop_duplicates()
        .merge(right_keys, on=key_cols, how="left", indicator=True)
        .loc[lambda frame: frame["_merge"].eq("left_only"), key_cols]
        .sort_values(key_cols)
        .reset_index(drop=True)
    )
    return only_left


def build_r3_nlp_join_audit(
    df_r3: pd.DataFrame,
    df_nlp: pd.DataFrame,
    key_cols: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build a matched rater/NLP frame plus unmatched key audits.

    Returns:
        - Matched inner-join frame.
        - Unmatched adjudicated key rows.
        - Unmatched NLP key rows.
        - Audit summary dictionary.

    Raises:
        ValueError: If normalized keys are non-unique or if join yields zero matches.
    """
    normalized_r3 = normalize_join_keys(df_r3, key_cols)
    normalized_nlp = normalize_join_keys(df_nlp, key_cols)

    _validate_unique_keys(normalized_r3, key_cols, context="R3 source")
    _validate_unique_keys(normalized_nlp, key_cols, context="NLP source")

    matched = normalized_r3.merge(normalized_nlp, on=key_cols, how="inner")
    _validate_unique_keys(matched, key_cols, context="R3/NLP joined output")

    unmatched_adjudicated = _anti_join_keys(normalized_r3, normalized_nlp, key_cols)
    unmatched_nlp = _anti_join_keys(normalized_nlp, normalized_r3, key_cols)

    r3_rows = int(len(normalized_r3))
    nlp_rows = int(len(normalized_nlp))
    matched_rows = int(len(matched))
    unmatched_adjudicated_rows = int(len(unmatched_adjudicated))
    unmatched_nlp_rows = int(len(unmatched_nlp))

    audit = {
        "key_columns": key_cols,
        "r3_rows": r3_rows,
        "nlp_rows": nlp_rows,
        "matched_rows": matched_rows,
        "unmatched_adjudicated_rows": unmatched_adjudicated_rows,
        "unmatched_nlp_rows": unmatched_nlp_rows,
        "matched_rate_vs_adjudicated": float(matched_rows / r3_rows) if r3_rows else None,
        "unmatched_rate_vs_adjudicated": (
            float(unmatched_adjudicated_rows / r3_rows) if r3_rows else None
        ),
        "unmatched_rate_vs_nlp": float(unmatched_nlp_rows / nlp_rows) if nlp_rows else None,
    }

    if matched_rows == 0:
        raise ValueError(
            "R3/NLP join produced zero matched rows after key normalization. "
            "Check key consistency between annotation and NLP workbooks."
        )

    return matched, unmatched_adjudicated, unmatched_nlp, audit
