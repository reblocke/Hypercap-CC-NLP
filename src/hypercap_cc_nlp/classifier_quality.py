"""Classifier quality helpers for hypercapnia flags, CC missingness, and contracts."""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

PSEUDO_MISSING_EXACT_TOKENS = {
    "-",
    "--",
    "—",
    "n",
    "na",
    "n/a",
    "none",
    "unknown",
    "refused",
}
PSEUDO_MISSING_REGEX = re.compile(r"^_+$")
PSEUDO_MISSING_PUNCT_REGEX = re.compile(r"^[^a-z0-9]+$")


def _canonicalize_cc_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.strip().lower()
    text = text.replace("–", "-").replace("—", "-")
    return " ".join(text.split())


def annotate_cc_missingness(
    df: pd.DataFrame,
    *,
    cc_column: str,
    nlp_input_column: str = "cc_text_for_nlp",
    missing_flag_column: str = "cc_missing_flag",
    pseudo_flag_column: str = "cc_pseudomissing_flag",
    missing_reason_column: str = "cc_missing_reason",
) -> pd.DataFrame:
    """Annotate true-vs-pseudo missing CCs and derive deterministic NLP input text."""
    if cc_column not in df.columns:
        raise KeyError(f"Chief complaint column not found: {cc_column}")

    out = df.copy()
    canonical = out[cc_column].map(_canonicalize_cc_text)
    is_true_missing = canonical.eq("")
    is_pseudo_missing = (
        canonical.isin(PSEUDO_MISSING_EXACT_TOKENS)
        | canonical.str.match(PSEUDO_MISSING_REGEX, na=False)
        | canonical.str.match(PSEUDO_MISSING_PUNCT_REGEX, na=False)
    )

    reason = pd.Series("valid", index=out.index, dtype="string")
    reason.loc[is_true_missing] = "true_missing"
    reason.loc[is_pseudo_missing] = "pseudo_missing_token"

    out[missing_flag_column] = (is_true_missing | is_pseudo_missing).astype("boolean")
    out[pseudo_flag_column] = is_pseudo_missing.astype("boolean")
    out[missing_reason_column] = reason
    out[nlp_input_column] = out[cc_column].astype("string")
    out.loc[is_true_missing | is_pseudo_missing, nlp_input_column] = pd.NA
    return out


def apply_pseudomissing_uncodable_policy(
    df: pd.DataFrame,
    *,
    max_rfv: int = 5,
    pseudo_flag_column: str = "cc_pseudomissing_flag",
) -> pd.DataFrame:
    """Force pseudo-missing CC rows to a deterministic uncodable RFV payload."""
    if pseudo_flag_column not in df.columns:
        return df

    out = df.copy()
    pseudo_mask = out[pseudo_flag_column].fillna(False).astype(bool)
    if not pseudo_mask.any():
        return out

    payload = json.dumps(
        [
            {
                "seg_idx": 0,
                "segment": "",
                "code": "RVC-UNCL",
                "name": "Uncodable/Unknown",
                "sim": None,
                "rule_code": "pseudo_missing_cc",
                "rule_used": True,
            }
        ]
    )
    _apply_uncodable_payload(
        out,
        mask=pseudo_mask,
        max_rfv=max_rfv,
        payload=payload,
    )
    return out


def apply_blank_primary_uncodable_policy(
    df: pd.DataFrame,
    *,
    max_rfv: int = 5,
    missing_reason_column: str = "cc_missing_reason",
) -> pd.DataFrame:
    """Force non-true-missing blank RFV primaries to deterministic uncodable output."""
    if "RFV1_name" not in df.columns:
        return df

    out = df.copy()
    blank_primary = out["RFV1_name"].fillna("").astype(str).str.strip().eq("")
    true_missing = (
        out[missing_reason_column].astype(str).eq("true_missing")
        if missing_reason_column in out.columns
        else pd.Series(False, index=out.index)
    )
    fallback_mask = blank_primary & ~true_missing
    if not fallback_mask.any():
        return out

    payload = json.dumps(
        [
            {
                "seg_idx": 0,
                "segment": "",
                "code": "RVC-UNCL",
                "name": "Uncodable/Unknown",
                "sim": None,
                "rule_code": "blank_primary_fallback",
                "rule_used": True,
            }
        ]
    )
    _apply_uncodable_payload(
        out,
        mask=fallback_mask,
        max_rfv=max_rfv,
        payload=payload,
    )
    return out


def _apply_uncodable_payload(
    out: pd.DataFrame,
    *,
    mask: pd.Series,
    max_rfv: int,
    payload: str,
) -> None:
    """Apply a deterministic uncodable RFV payload in-place."""
    out.loc[mask, "RFV1"] = "uncodable"
    out.loc[mask, "RFV1_name"] = "Uncodable/Unknown"
    out.loc[mask, "RFV1_support"] = ""
    out.loc[mask, "RFV1_sim"] = math.nan

    for slot in range(2, max_rfv + 1):
        out.loc[mask, f"RFV{slot}"] = ""
        out.loc[mask, f"RFV{slot}_name"] = ""
        out.loc[mask, f"RFV{slot}_support"] = ""
        out.loc[mask, f"RFV{slot}_sim"] = math.nan

    out.loc[mask, "segment_preds"] = payload


def build_cc_missing_audit(
    df: pd.DataFrame,
    *,
    cc_column: str,
    missing_reason_column: str = "cc_missing_reason",
    max_examples: int = 5,
) -> pd.DataFrame:
    """Summarize CC missingness categories with example raw values."""
    if cc_column not in df.columns:
        raise KeyError(f"Chief complaint column not found: {cc_column}")
    if missing_reason_column not in df.columns:
        raise KeyError(f"Missing reason column not found: {missing_reason_column}")

    frame = df[[cc_column, missing_reason_column]].copy()
    frame[cc_column] = frame[cc_column].astype("string")
    rows: list[dict[str, Any]] = []
    for reason, group in frame.groupby(missing_reason_column, dropna=False):
        examples = (
            group[cc_column]
            .fillna("<NA>")
            .astype(str)
            .value_counts(dropna=False)
            .head(max_examples)
            .index.tolist()
        )
        rows.append(
            {
                "cc_missing_reason": str(reason),
                "row_count": int(len(group)),
                "examples": "; ".join(examples),
            }
        )
    return pd.DataFrame(rows).sort_values("cc_missing_reason").reset_index(drop=True)


def classifier_resource_paths(
    work_dir: Path,
    *,
    appendix_relpath: str,
    summary_relpath: str,
) -> dict[str, Path]:
    """Return required classifier resource paths under ``work_dir``."""
    return {
        "appendix_csv": (work_dir / appendix_relpath).expanduser().resolve(),
        "summary_weights_csv": (work_dir / summary_relpath).expanduser().resolve(),
    }


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_classifier_resources(
    work_dir: Path,
    *,
    appendix_relpath: str,
    summary_relpath: str,
    manifest_path: str | None = "Annotation/resource_manifest.json",
    strict_hash: bool = True,
) -> dict[str, Any]:
    """Validate required classifier resources and optional hash manifest."""
    checks: list[dict[str, Any]] = []
    missing_paths: list[str] = []
    for name, path in classifier_resource_paths(
        work_dir,
        appendix_relpath=appendix_relpath,
        summary_relpath=summary_relpath,
    ).items():
        exists = path.exists()
        checks.append({"resource": name, "path": str(path), "exists": exists})
        if not exists:
            missing_paths.append(str(path))

    hash_findings: list[dict[str, Any]] = []
    manifest_resolved: Path | None = None
    if manifest_path:
        manifest_resolved = (work_dir / manifest_path).expanduser().resolve()
        if manifest_resolved.exists():
            manifest = json.loads(manifest_resolved.read_text())
            resources = manifest.get("resources", [])
            for entry in resources:
                rel_path = entry.get("path")
                expected = str(entry.get("sha256", "")).strip().lower()
                if not rel_path or not expected:
                    continue
                path = (work_dir / rel_path).expanduser().resolve()
                if not path.exists():
                    hash_findings.append(
                        {
                            "path": str(path),
                            "status": "missing",
                            "expected_sha256": expected,
                            "actual_sha256": None,
                        }
                    )
                    continue
                actual = _sha256(path)
                if actual != expected:
                    hash_findings.append(
                        {
                            "path": str(path),
                            "status": "hash_mismatch",
                            "expected_sha256": expected,
                            "actual_sha256": actual,
                        }
                    )

    errors: list[str] = []
    warnings: list[str] = []

    if missing_paths:
        errors.append(
            "Missing required classifier resources: " + ", ".join(sorted(missing_paths))
        )

    for finding in hash_findings:
        message = (
            f"{finding['status']} for {finding['path']} "
            f"(expected={finding['expected_sha256']}, actual={finding['actual_sha256']})"
        )
        if strict_hash:
            errors.append(message)
        else:
            warnings.append(message)

    status = "pass"
    if errors:
        status = "fail"
    elif warnings:
        status = "warning"

    return {
        "status": status,
        "strict_hash": bool(strict_hash),
        "manifest_path": str(manifest_resolved) if manifest_resolved else None,
        "checks": checks,
        "hash_findings": hash_findings,
        "errors": errors,
        "warnings": warnings,
    }


def validate_classifier_contract(
    df: pd.DataFrame,
    *,
    max_rfv: int = 5,
    pseudo_flag_column: str = "cc_pseudomissing_flag",
    missing_reason_column: str = "cc_missing_reason",
    missing_flag_column: str = "cc_missing_flag",
    authoritative_disagreement_tolerance: float = 0.0,
) -> dict[str, Any]:
    """Run deterministic post-export classifier contract checks."""
    findings: list[dict[str, str]] = []

    if "hadm_id" in df.columns:
        duplicates = int(df.duplicated(subset=["hadm_id"]).sum())
        if duplicates:
            findings.append(
                {
                    "severity": "error",
                    "code": "hadm_id_not_unique",
                    "message": f"Found {duplicates} duplicate hadm_id rows.",
                }
            )

    if "segment_preds" not in df.columns:
        findings.append(
            {
                "severity": "error",
                "code": "missing_segment_preds",
                "message": "segment_preds column missing from classifier output.",
            }
        )
    else:
        bad_json_rows = 0
        for value in df["segment_preds"].fillna("[]"):
            try:
                parsed = json.loads(value) if isinstance(value, str) else value
            except json.JSONDecodeError:
                bad_json_rows += 1
                continue
            if not isinstance(parsed, list):
                bad_json_rows += 1
        if bad_json_rows:
            findings.append(
                {
                    "severity": "error",
                    "code": "segment_preds_malformed",
                    "message": f"Found {bad_json_rows} malformed segment_preds rows.",
                }
            )

    required_cc_columns = {missing_reason_column}
    missing_cc_columns = sorted(required_cc_columns.difference(df.columns))
    if missing_cc_columns:
        findings.append(
            {
                "severity": "error",
                "code": "missing_cc_missingness_columns",
                "message": f"Missing CC missingness columns: {missing_cc_columns}",
            }
        )

    if "RFV1_name" in df.columns:
        blank_rfv1 = df["RFV1_name"].fillna("").astype(str).str.strip().eq("")
        if missing_reason_column in df.columns:
            allowed_blank = df[missing_reason_column].astype(str).eq("true_missing")
            unexpected_blank = int((blank_rfv1 & ~allowed_blank).sum())
        else:
            unexpected_blank = int(blank_rfv1.sum())
        if unexpected_blank:
            findings.append(
                {
                    "severity": "error",
                    "code": "unexpected_blank_rfv1",
                    "message": (
                        "Found blank RFV1_name rows outside allowed true-missing policy: "
                        f"{unexpected_blank}"
                    ),
                }
            )

    pseudo_count = 0
    if missing_reason_column in df.columns:
        pseudo_count = int(df[missing_reason_column].astype(str).eq("pseudo_missing_token").sum())

    if pseudo_flag_column in df.columns and missing_reason_column in df.columns:
        pseudo_flag_series = df[pseudo_flag_column].fillna(False).astype(bool)
        pseudo_expected_series = df[missing_reason_column].astype(str).eq("pseudo_missing_token")
        pseudo_flag_mismatch = int((pseudo_flag_series != pseudo_expected_series).sum())
        if pseudo_flag_mismatch:
            findings.append(
                {
                    "severity": "warning",
                    "code": "cc_pseudomissing_flag_mismatch",
                    "message": (
                        "cc_pseudomissing_flag mismatches cc_missing_reason pseudo_missing_token rows: "
                        f"{pseudo_flag_mismatch}"
                    ),
                }
            )

    if missing_flag_column in df.columns and missing_reason_column in df.columns:
        missing_flag_series = df[missing_flag_column].fillna(False).astype(bool)
        missing_expected_series = df[missing_reason_column].astype(str).ne("valid")
        missing_flag_mismatch = int((missing_flag_series != missing_expected_series).sum())
        if missing_flag_mismatch:
            findings.append(
                {
                    "severity": "warning",
                    "code": "cc_missing_flag_mismatch",
                    "message": (
                        "cc_missing_flag mismatches cc_missing_reason non-valid rows: "
                        f"{missing_flag_mismatch}"
                    ),
                }
            )

    error_count = sum(1 for finding in findings if finding["severity"] == "error")
    warning_count = sum(1 for finding in findings if finding["severity"] == "warning")
    status = "pass"
    if error_count:
        status = "fail"
    elif warning_count:
        status = "warning"

    return {
        "status": status,
        "error_count": error_count,
        "warning_count": warning_count,
        "pseudo_missing_rows": pseudo_count,
        "findings": findings,
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "max_rfv": int(max_rfv),
    }
