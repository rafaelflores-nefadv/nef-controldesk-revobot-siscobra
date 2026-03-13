import logging
import re
from datetime import date
from pathlib import Path

SOURCE_ID_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_.-]+")


def sanitize_source_id(value: object, fallback: str = "source") -> str:
    raw = str(value or "").strip()
    if not raw:
        raw = fallback
    normalized = SOURCE_ID_SANITIZE_RE.sub("_", raw).strip("._-")
    return normalized or fallback


def source_log_path(log_dir: Path, cycle_date: date, source_id: str, folder: str = "sources") -> Path:
    safe_source = sanitize_source_id(source_id)
    return Path(log_dir) / folder / f"{cycle_date.isoformat()}_{safe_source}.log"


def build_source_logger(
    base_logger: logging.Logger,
    *,
    log_dir: Path,
    cycle_date: date,
    source_id: str,
    folder: str = "sources",
) -> logging.Logger:
    logger_name = f"{base_logger.name}.source.{sanitize_source_id(source_id)}"
    source_logger = logging.getLogger(logger_name)
    source_logger.setLevel(base_logger.level or logging.INFO)
    source_logger.propagate = True

    log_path = source_log_path(log_dir, cycle_date, source_id, folder=folder)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    existing_paths = {
        Path(getattr(handler, "baseFilename"))
        for handler in source_logger.handlers
        if isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None)
    }
    if log_path not in existing_paths:
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
        source_logger.addHandler(file_handler)
    return source_logger
