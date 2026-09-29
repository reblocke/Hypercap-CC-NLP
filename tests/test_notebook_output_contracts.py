from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

WORK_DIR = Path(__file__).resolve().parents[1]

PIPELINE_NOTEBOOKS = [
    WORK_DIR / "MIMICIV_hypercap_EXT_cohort.qmd",
    WORK_DIR / "Hypercap CC NLP Classifier.qmd",
    WORK_DIR / "Rater Agreement Analysis.qmd",
    WORK_DIR / "Hypercap CC NLP Analysis.qmd",
]

RENDERABLE_ANALYSIS_NOTEBOOKS = PIPELINE_NOTEBOOKS

DISALLOWED_RUNTIME_IMPORT_TOKENS = (
    "SRC_DIR = WORK_DIR / \"src\"",
    "sys.path.insert(0, str(SRC_DIR))",
    "from hypercap_cc_nlp.",
    "import hypercap_cc_nlp",
)


def test_renderable_notebooks_do_not_import_repo_local_runtime_modules() -> None:
    for notebook_path in RENDERABLE_ANALYSIS_NOTEBOOKS:
        text = notebook_path.read_text()
        for disallowed in DISALLOWED_RUNTIME_IMPORT_TOKENS:
            assert (
                disallowed not in text
            ), f"{notebook_path.name} contains disallowed runtime token {disallowed}"


def test_public_release_tree_excludes_generated_and_private_roots() -> None:
    for relative_path in (
        "artifacts",
        "debug",
        "Drafts",
        "Legacy Code",
        "MIMIC tabular data",
        "outputs",
        "Results",
        "tmp",
    ):
        tracked = subprocess.run(
            ["git", "ls-files", "--", relative_path],
            cwd=WORK_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        assert tracked.stdout.strip() == ""
        if (WORK_DIR / relative_path).exists():
            ignored = subprocess.run(
                ["git", "check-ignore", "-q", relative_path],
                cwd=WORK_DIR,
                check=False,
            )
            assert ignored.returncode == 0


def test_public_release_git_hygiene_patterns() -> None:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=WORK_DIR,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked_paths = completed.stdout.splitlines()
    forbidden_path_pattern = re.compile(
        r"(^MIMIC tabular data/|^Drafts/|^Results/|^debug/|^outputs/|"
        r"^artifacts/|^tmp/|^Legacy Code/|^\.codex/|^\.jupyter/|"
        r"\.DS_Store$|\.Rhistory$)"
    )
    forbidden_matches = [
        path for path in tracked_paths if forbidden_path_pattern.search(path)
    ]
    assert forbidden_matches == []

    forbidden_binary_suffixes = {
        ".jpeg",
        ".jpg",
        ".parquet",
        ".png",
        ".pptx",
        ".tif",
        ".tiff",
        ".xls",
        ".xlsx",
        ".zip",
    }
    tracked_binary_paths = [
        WORK_DIR / path
        for path in tracked_paths
        if Path(path).suffix.casefold()
        in forbidden_binary_suffixes | {".docx", ".pdf"}
    ]
    assert not [
        path for path in tracked_binary_paths if path.suffix.casefold() in forbidden_binary_suffixes
    ]

    for binary_path in tracked_binary_paths:
        notice_path = binary_path.parent / "README.md"
        assert notice_path.is_file()
        notice_text = notice_path.read_text().casefold()
        digest = hashlib.sha256(binary_path.read_bytes()).hexdigest()
        assert digest in notice_text
        if binary_path.suffix.casefold() == ".docx":
            assert "not intended for use" in notice_text
            assert "files are stored unchanged" in notice_text
        else:
            assert "unchanged copy" in notice_text
