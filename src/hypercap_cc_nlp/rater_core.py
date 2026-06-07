"""Core helpers for rater-to-NLP overlap auditing."""

from __future__ import annotations

import hashlib
import re
from typing import Any

import numpy as np
import pandas as pd

from .workflow_contracts import ensure_required_columns

BOOTSTRAP_SEED = 20260607


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


def build_join_key_diagnostics(
    df_r3: pd.DataFrame,
    df_nlp: pd.DataFrame,
) -> pd.DataFrame:
    """Profile supported join-key strategies before selecting one."""
    preferred_candidates: list[tuple[list[str], str]] = [
        (["ed_stay_id"], "ed_stay_id"),
        (["hadm_id", "subject_id"], "hadm_id_subject_id"),
        (["hadm_id"], "hadm_id"),
    ]
    rows: list[dict[str, Any]] = []
    for key_cols, strategy in preferred_candidates:
        row: dict[str, Any] = {
            "join_key_strategy": strategy,
            "join_key_columns": "|".join(key_cols),
            "columns_present_r3": bool(set(key_cols).issubset(df_r3.columns)),
            "columns_present_nlp": bool(set(key_cols).issubset(df_nlp.columns)),
            "selected": False,
            "eligible_for_join": False,
            "failure_reason": "",
            "r3_rows": int(len(df_r3)),
            "nlp_rows": int(len(df_nlp)),
        }
        if not row["columns_present_r3"] or not row["columns_present_nlp"]:
            row["failure_reason"] = "missing_columns"
            rows.append(row)
            continue

        normalized_r3 = normalize_join_keys(df_r3, key_cols)
        normalized_nlp = normalize_join_keys(df_nlp, key_cols)
        r3_nonnull = normalized_r3[key_cols].notna().all(axis=1)
        nlp_nonnull = normalized_nlp[key_cols].notna().all(axis=1)
        r3_valid = normalized_r3.loc[r3_nonnull, key_cols].copy()
        nlp_valid = normalized_nlp.loc[nlp_nonnull, key_cols].copy()
        row["r3_missing_key_rows"] = int((~r3_nonnull).sum())
        row["nlp_missing_key_rows"] = int((~nlp_nonnull).sum())
        row["r3_nonnull_rows"] = int(r3_nonnull.sum())
        row["nlp_nonnull_rows"] = int(nlp_nonnull.sum())
        row["r3_duplicate_key_rows"] = int(r3_valid.duplicated(subset=key_cols).sum())
        row["nlp_duplicate_key_rows"] = int(nlp_valid.duplicated(subset=key_cols).sum())
        r3_keys = r3_valid.drop_duplicates()
        nlp_keys = nlp_valid.drop_duplicates()
        row["overlapping_unique_keys"] = int(
            len(r3_keys.merge(nlp_keys, on=key_cols, how="inner"))
        )
        if int(r3_nonnull.sum()) == 0 or int(nlp_nonnull.sum()) == 0:
            row["failure_reason"] = "no_shared_nonnull_rows"
        elif row["r3_duplicate_key_rows"] or row["nlp_duplicate_key_rows"]:
            row["failure_reason"] = "duplicate_keys"
        else:
            row["eligible_for_join"] = True
        rows.append(row)

    diagnostics = pd.DataFrame(rows)
    eligible = diagnostics.loc[diagnostics["eligible_for_join"]].copy()
    if not eligible.empty:
        diagnostics.loc[
            diagnostics["join_key_strategy"].eq(eligible.iloc[0]["join_key_strategy"]),
            "selected",
        ] = True
    return diagnostics


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
    r3_missing_key_rows = int((~normalized_r3[key_cols].notna().all(axis=1)).sum())
    nlp_missing_key_rows = int((~normalized_nlp[key_cols].notna().all(axis=1)).sum())
    r3_duplicate_key_rows = int(
        normalized_r3.loc[normalized_r3[key_cols].notna().all(axis=1), key_cols]
        .duplicated(subset=key_cols)
        .sum()
    )
    nlp_duplicate_key_rows = int(
        normalized_nlp.loc[normalized_nlp[key_cols].notna().all(axis=1), key_cols]
        .duplicated(subset=key_cols)
        .sum()
    )

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
        "human_human_denominator_n": r3_rows,
        "nlp_benchmark_denominator_n": matched_rows,
        "r3_missing_key_rows": r3_missing_key_rows,
        "nlp_missing_key_rows": nlp_missing_key_rows,
        "r3_duplicate_key_rows": r3_duplicate_key_rows,
        "nlp_duplicate_key_rows": nlp_duplicate_key_rows,
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

    matched_rate_vs_adjudicated = audit["matched_rate_vs_adjudicated"]
    if matched_rate_vs_adjudicated == 1.0:
        audit["join_interpretation"] = "adjudicated_fully_covered_subset"
        audit["severity"] = "info"
    else:
        audit["join_interpretation"] = "partial_adjudicated_overlap"
        audit["severity"] = "warning"

    return matched, unmatched_adjudicated, unmatched_nlp, audit


def hash_join_keys(
    df: pd.DataFrame,
    *,
    key_cols: list[str],
    hash_col: str = "key_hash",
) -> pd.DataFrame:
    """Return deterministic SHA256 hashes for normalized join keys only."""
    normalized = normalize_join_keys(df, key_cols)
    _validate_unique_keys(normalized, key_cols, context="join-key hash input")

    key_frame = normalized[key_cols].drop_duplicates().copy()

    def _hash_row(row: pd.Series) -> str:
        material = "|".join(
            "" if pd.isna(row[column]) else str(int(row[column])) for column in key_cols
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    key_frame[hash_col] = key_frame.apply(_hash_row, axis=1)
    return key_frame[[hash_col]].sort_values(hash_col).reset_index(drop=True)


def assign_annotation_row_id(
    df: pd.DataFrame,
    *,
    id_col: str = "annotation_row_id",
    start: int = 1,
) -> pd.DataFrame:
    """Add a deterministic one-based row key for direct annotation benchmarking."""
    if start < 0:
        raise ValueError("start must be nonnegative")
    out = df.copy()
    out[id_col] = np.arange(start, start + len(out), dtype=int)
    return out


def build_annotation_direct_join(
    df_r3: pd.DataFrame,
    df_nlp: pd.DataFrame,
    *,
    id_col: str = "annotation_row_id",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Join adjudicated labels to direct annotation NLP output by row id."""
    ensure_required_columns(df_r3, [id_col], context="direct annotation R3 source")
    ensure_required_columns(df_nlp, [id_col], context="direct annotation NLP source")
    normalized_r3 = normalize_join_keys(df_r3, [id_col])
    normalized_nlp = normalize_join_keys(df_nlp, [id_col])
    _validate_unique_keys(normalized_r3, [id_col], context="direct annotation R3 source")
    _validate_unique_keys(normalized_nlp, [id_col], context="direct annotation NLP source")

    matched = normalized_r3.merge(normalized_nlp, on=[id_col], how="inner")
    _validate_unique_keys(matched, [id_col], context="direct annotation joined output")
    unmatched_adjudicated = _anti_join_keys(normalized_r3, normalized_nlp, [id_col])
    unmatched_nlp = _anti_join_keys(normalized_nlp, normalized_r3, [id_col])

    r3_rows = int(len(normalized_r3))
    nlp_rows = int(len(normalized_nlp))
    matched_rows = int(len(matched))
    if matched_rows == 0:
        raise ValueError(
            "Direct annotation benchmark join produced zero matched rows. "
            "Check annotation_row_id creation in classifier and rater notebooks."
        )

    audit = {
        "benchmark_source": "annotation_direct",
        "key_columns": [id_col],
        "key_strategy": "annotation_row_id",
        "r3_rows": r3_rows,
        "nlp_rows": nlp_rows,
        "matched_rows": matched_rows,
        "unmatched_adjudicated_rows": int(len(unmatched_adjudicated)),
        "unmatched_nlp_rows": int(len(unmatched_nlp)),
        "human_human_denominator_n": r3_rows,
        "nlp_benchmark_denominator_n": matched_rows,
        "matched_rate_vs_adjudicated": float(matched_rows / r3_rows) if r3_rows else None,
        "unmatched_rate_vs_adjudicated": (
            float(len(unmatched_adjudicated) / r3_rows) if r3_rows else None
        ),
        "unmatched_rate_vs_nlp": float(len(unmatched_nlp) / nlp_rows) if nlp_rows else None,
        "join_interpretation": (
            "direct_annotation_fully_covered"
            if matched_rows == r3_rows
            else "direct_annotation_partial_coverage"
        ),
        "severity": "info" if matched_rows == r3_rows else "warning",
    }
    return matched, unmatched_adjudicated, unmatched_nlp, audit


def _as_label_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        if not value.strip():
            return set()
        return {item.strip() for item in value.split(";") if item.strip()}
    return {str(item).strip() for item in value if str(item).strip()}


def agreement_class(gold: Any, pred: Any) -> str:
    """Classify multi-label agreement as exact, partial, or none."""
    gold_set = _as_label_set(gold)
    pred_set = _as_label_set(pred)
    if gold_set == pred_set:
        return "exact"
    if gold_set & pred_set:
        return "partial"
    return "none"


def per_category_prf(
    gold_sets: list[set[str]],
    pred_sets: list[set[str]],
    categories: list[str],
) -> pd.DataFrame:
    """Compute one-vs-rest support, counts, precision, recall, and F1 by category."""
    if len(gold_sets) != len(pred_sets):
        raise ValueError("gold_sets and pred_sets must have the same length")
    rows: list[dict[str, Any]] = []
    n = len(gold_sets)
    for category in categories:
        tp = fp = fn = tn = 0
        for gold, pred in zip(gold_sets, pred_sets):
            gold_has = category in gold
            pred_has = category in pred
            if gold_has and pred_has:
                tp += 1
            elif not gold_has and pred_has:
                fp += 1
            elif gold_has and not pred_has:
                fn += 1
            else:
                tn += 1
        precision = tp / (tp + fp) if (tp + fp) else np.nan
        recall = tp / (tp + fn) if (tp + fn) else np.nan
        f1 = 2 * precision * recall / (precision + recall) if precision + recall > 0 else np.nan
        rows.append(
            {
                "category_code": category,
                "N": n,
                "adjudicated_support": tp + fn,
                "nlp_positives": tp + fp,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "tn": tn,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            }
        )
    return pd.DataFrame(rows)


def _nan_quantiles(values: list[float], probs: tuple[float, float]) -> tuple[float, float]:
    array = np.asarray(values, dtype=float)
    if array.size == 0 or np.isnan(array).all():
        return np.nan, np.nan
    lo, hi = np.nanquantile(array, probs)
    return float(lo), float(hi)


def bootstrap_per_category_prf_cis(
    gold_sets: list[set[str]],
    pred_sets: list[set[str]],
    categories: list[str],
    *,
    n_boot: int = 2000,
    seed: int = BOOTSTRAP_SEED,
    ci: float = 0.95,
) -> pd.DataFrame:
    """Bootstrap visit-level CIs for per-category precision, recall, and F1."""
    if len(gold_sets) != len(pred_sets):
        raise ValueError("gold_sets and pred_sets must have the same length")
    if n_boot <= 0:
        raise ValueError("n_boot must be positive")
    if not 0 < ci < 1:
        raise ValueError("ci must be between 0 and 1")
    n = len(gold_sets)
    if n == 0:
        return pd.DataFrame(
            columns=["category_code", "metric", "estimate", "ci_low", "ci_high", "n_boot", "seed"]
        )

    estimates = per_category_prf(gold_sets, pred_sets, categories).set_index("category_code")
    alpha = (1 - ci) / 2
    probs = (alpha, 1 - alpha)
    rng = np.random.default_rng(seed)
    samples: dict[tuple[str, str], list[float]] = {
        (category, metric): []
        for category in categories
        for metric in ("precision", "recall", "f1")
    }
    for _ in range(n_boot):
        indices = rng.integers(0, n, size=n)
        sampled_gold = [gold_sets[int(index)] for index in indices]
        sampled_pred = [pred_sets[int(index)] for index in indices]
        sampled = per_category_prf(sampled_gold, sampled_pred, categories).set_index("category_code")
        for category in categories:
            for metric in ("precision", "recall", "f1"):
                samples[(category, metric)].append(float(sampled.loc[category, metric]))

    rows: list[dict[str, Any]] = []
    for category in categories:
        for metric in ("precision", "recall", "f1"):
            ci_low, ci_high = _nan_quantiles(samples[(category, metric)], probs)
            rows.append(
                {
                    "category_code": category,
                    "metric": metric,
                    "estimate": float(estimates.loc[category, metric]),
                    "ci_low": ci_low,
                    "ci_high": ci_high,
                    "n_boot": n_boot,
                    "seed": seed,
                }
            )
    return pd.DataFrame(rows)


def bootstrap_set_agreement_cis(
    gold_sets: list[set[str]],
    pred_sets: list[set[str]],
    *,
    n_boot: int = 2000,
    seed: int = BOOTSTRAP_SEED,
    ci: float = 0.95,
) -> pd.DataFrame:
    """Bootstrap visit-level CIs for exact, partial, and none agreement rates."""
    if len(gold_sets) != len(pred_sets):
        raise ValueError("gold_sets and pred_sets must have the same length")
    if n_boot <= 0:
        raise ValueError("n_boot must be positive")
    n = len(gold_sets)
    classes = [agreement_class(gold, pred) for gold, pred in zip(gold_sets, pred_sets)]
    if n == 0:
        return pd.DataFrame(columns=["metric", "estimate", "ci_low", "ci_high", "n_boot", "seed"])
    rng = np.random.default_rng(seed)
    alpha = (1 - ci) / 2
    probs = (alpha, 1 - alpha)
    values = {metric: [] for metric in ("exact_rate", "partial_rate", "none_rate")}
    for _ in range(n_boot):
        indices = rng.integers(0, n, size=n)
        sampled = [classes[int(index)] for index in indices]
        values["exact_rate"].append(sampled.count("exact") / n)
        values["partial_rate"].append(sampled.count("partial") / n)
        values["none_rate"].append(sampled.count("none") / n)

    rows: list[dict[str, Any]] = []
    for class_name, metric in (
        ("exact", "exact_rate"),
        ("partial", "partial_rate"),
        ("none", "none_rate"),
    ):
        ci_low, ci_high = _nan_quantiles(values[metric], probs)
        rows.append(
            {
                "metric": metric,
                "estimate": classes.count(class_name) / n,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "n_boot": n_boot,
                "seed": seed,
            }
        )
    return pd.DataFrame(rows)


def multilabel_confusion_matrix(
    gold_sets: list[set[str]],
    pred_sets: list[set[str]],
    categories: list[str],
    *,
    none_label: str = "__none__",
) -> pd.DataFrame:
    """Build category co-occurrence counts for adjudicated vs NLP label sets."""
    if len(gold_sets) != len(pred_sets):
        raise ValueError("gold_sets and pred_sets must have the same length")
    labels = [*categories, none_label]
    matrix = pd.DataFrame(0, index=labels, columns=labels, dtype=int)
    category_set = set(categories)
    for gold, pred in zip(gold_sets, pred_sets):
        gold_labels = sorted(set(gold) & category_set) or [none_label]
        pred_labels = sorted(set(pred) & category_set) or [none_label]
        for gold_label in gold_labels:
            for pred_label in pred_labels:
                matrix.loc[gold_label, pred_label] += 1
    matrix.index.name = "adjudicated_category"
    return matrix.reset_index()


def redact_chief_complaint_example(text: object, *, max_chars: int = 160) -> str:
    """Conservatively redact a short chief-complaint example for disagreement review."""
    if pd.isna(text):
        return ""
    redacted = str(text)
    redacted = re.sub(r"\[\*\*.*?\*\*\]", "[REDACTED]", redacted)
    redacted = re.sub(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b", "[DATE]", redacted)
    redacted = re.sub(r"\b\d{2,}\b", "[NUMBER]", redacted)
    redacted = re.sub(r"\s+", " ", redacted).strip()
    if len(redacted) > max_chars:
        redacted = redacted[: max_chars - 3].rstrip() + "..."
    return redacted
