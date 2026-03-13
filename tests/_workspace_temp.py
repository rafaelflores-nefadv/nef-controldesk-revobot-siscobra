from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import shutil
from uuid import uuid4


ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_TMP_ROOT = ROOT_DIR / "downloads" / ".test_tmp"


def make_workspace_dir(prefix: str) -> Path:
    TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    path = TEST_TMP_ROOT / f"{prefix}_{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def cleanup_workspace_dir(path: Path | None) -> None:
    if path is None:
        return
    shutil.rmtree(path, ignore_errors=True)


@contextmanager
def workspace_temp_dir(prefix: str):
    path = make_workspace_dir(prefix)
    try:
        yield path
    finally:
        cleanup_workspace_dir(path)
