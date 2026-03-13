import argparse
import copy
import json
import logging
import os
import re
import requests
import shutil
import sys
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from time import monotonic, sleep

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from core.driver import criar_driver
from core.login import realizar_login
from core.download import (
    aguardar_download,
    limpar_arquivos_download_anteriores,
    remover_cabecalho_csv,
    copiar_arquivo,
    enviar_arquivo_ftp,
    testar_conexao_ftp,
    arquivo_ftp_existe_com_mesmo_tamanho,
)
from core.download_api import (
    FileManagerApiError,
    SessionExpiredError,
    UnexpectedApiResponseError,
    listar_arquivos_api_no_browser,
    baixar_arquivo_via_form_submit_no_browser,
    selecionar_csv_por_data,
)
from core.email_notifier import send_execution_email
from core.whatsapp_notifier import send_whatsapp_messages
from core.notification_dispatcher import dispatch_source_notifications
from core.pipeline_orchestrator import (
    execute_stage_flow,
    process_source_with_retries,
)
from core.source_logger import (
    build_source_logger,
    sanitize_source_id,
    source_log_path,
)
from config.settings import (
    COPY_DIR,
    COPY_TO_FTP,
    COPY_TO_SERVER,
    DAEMON_AFTER_RUN_SLEEP_SECONDS,
    DAEMON_MODE_ENABLED,
    DAEMON_POLL_SECONDS,
    DOWNLOAD_DIR,
    DOWNLOAD_SOURCES,
    ENABLE_COPIES,
    ENFORCE_SCHEDULE,
    FILE_PREFIX,
    FILE_MANAGER_EXPORT_FOLDER,
    GOOGLE_CHAT_TIMEOUT,
    GOOGLE_CHAT_WEBHOOK_URL,
    LOG_DIR,
    RETRY_DELAY_SECONDS,
    RETRY_MAX_ATTEMPTS,
    RETRY_ON_FAILURE_ENABLED,
    RUN_DAYS,
    RUN_START_DATE,
    RUN_TIME,
)

STAGE_ORDER = ("download", "prepare", "send_server", "send_ftp")
STAGE_LABELS = {
    "download": "Download",
    "prepare": "Prepare",
    "send_server": "Envio Servidor",
    "send_ftp": "Envio FTP",
}
STAGE_DEPENDENCIES = {
    "download": (),
    "prepare": ("download",),
    "send_server": ("prepare",),
    "send_ftp": ("prepare",),
}

LOCK_FILE_PATH = Path(LOG_DIR) / "revo360.lock"
LAST_SUCCESS_FILE_PATH = Path(LOG_DIR) / "last_success.txt"
MANUAL_CYCLE_DATE_FORMAT = "%d-%m-%Y"
RUN_ID_PATTERN = re.compile(r"^\d{6}$")


class OutsideExecutionWindowError(Exception):
    """Indica execucao fora da janela configurada (nao retentavel)."""


class SourceConfigurationError(RuntimeError):
    """Configuracao invalida em DOWNLOAD_SOURCES."""


STATE_SCHEMA_VERSION = 2
SOURCE_LOG_FOLDER = "sources"
OBSERVABILITY_RESULT_CATEGORIES = {"success", "failed", "not_found", "skipped", "invalid_config"}


def _sanitize_source_id(value: object, fallback: str = "source") -> str:
    return sanitize_source_id(value, fallback=fallback)


def _render_filename_template(template: str | None, cycle_date: date) -> str | None:
    if template is None:
        return None
    raw = str(template).strip()
    if not raw:
        return None
    values = {
        "date": cycle_date,
        "yyyy": cycle_date.strftime("%Y"),
        "mm": cycle_date.strftime("%m"),
        "dd": cycle_date.strftime("%d"),
        "yyyymmdd": cycle_date.strftime("%Y%m%d"),
    }
    try:
        return raw.format(**values)
    except Exception as exc:
        raise RuntimeError(
            f"filename_template invalido para source: {raw!r}. Erro: {exc}"
        ) from exc


def _legacy_source_definition() -> dict:
    return {
        "id": "default_legacy",
        "enabled": True,
        "remote_folder": FILE_MANAGER_EXPORT_FOLDER,
        "filename_template": None,
        "prepared_prefix": FILE_PREFIX,
        "copy_dir": COPY_DIR,
        "ftp_dir": None,
        "send_to_server": bool(COPY_TO_SERVER),
        "send_to_ftp": bool(COPY_TO_FTP),
    }


def _normalize_single_source(raw_source: dict, index: int, requested_targets: dict) -> dict:
    fallback_id = f"source_{index + 1:02d}"
    source_id = _sanitize_source_id(raw_source.get("id"), fallback=fallback_id)
    enabled = bool(raw_source.get("enabled", True))
    remote_folder = str(raw_source.get("remote_folder") or "").strip() or FILE_MANAGER_EXPORT_FOLDER
    prepared_prefix = str(raw_source.get("prepared_prefix") or "").strip() or FILE_PREFIX
    filename_template = raw_source.get("filename_template")
    if filename_template is not None:
        filename_template = str(filename_template).strip() or None
    copy_dir = raw_source.get("copy_dir")
    ftp_dir = raw_source.get("ftp_dir")
    copy_dir = str(copy_dir).strip() if copy_dir not in (None, "") else None
    ftp_dir = str(ftp_dir).strip() if ftp_dir not in (None, "") else None
    send_to_server = bool(raw_source.get("send_to_server", True)) and bool(requested_targets["server"])
    send_to_ftp = bool(raw_source.get("send_to_ftp", True)) and bool(requested_targets["ftp"])
    return {
        "id": source_id,
        "enabled": enabled,
        "remote_folder": remote_folder,
        "filename_template": filename_template,
        "prepared_prefix": prepared_prefix,
        "copy_dir": copy_dir,
        "ftp_dir": ftp_dir,
        "send_to_server": send_to_server,
        "send_to_ftp": send_to_ftp,
    }


def _raise_source_config_errors(errors: list[str]) -> None:
    if not errors:
        return
    details = "\n".join(f"- {line}" for line in errors)
    raise SourceConfigurationError(f"Configuracao DOWNLOAD_SOURCES invalida:\n{details}")


def _validate_filename_template_for_source(filename_template) -> None:
    if filename_template in (None, ""):
        return
    _render_filename_template(str(filename_template), date.today())


def _resolve_download_sources(*, requested_targets: dict | None = None) -> list[dict]:
    targets = _normalize_targets(
        requested_targets,
        fallback=_default_targets_from_settings(),
    )
    if DOWNLOAD_SOURCES is None:
        raw_sources = []
    elif isinstance(DOWNLOAD_SOURCES, list):
        raw_sources = DOWNLOAD_SOURCES
    else:
        raise SourceConfigurationError(
            "DOWNLOAD_SOURCES deve ser uma lista de objetos (dict)."
        )
    if not raw_sources:
        raw_sources = [_legacy_source_definition()]
    elif (
        len(raw_sources) == 1
        and isinstance(raw_sources[0], dict)
        and str(raw_sources[0].get("id") or "").strip() in {"default_0914", "default_legacy"}
    ):
        # Compatibilidade: quando estiver no config padrao legado, reusa os
        # valores globais atuais (importante para testes e overrides por patch).
        candidate = raw_sources[0]
        legacy = _legacy_source_definition()
        raw_sources = [
            {
                "id": candidate.get("id") or legacy["id"],
                "enabled": candidate.get("enabled", legacy["enabled"]),
                "remote_folder": legacy["remote_folder"],
                "filename_template": candidate.get("filename_template", legacy["filename_template"]),
                "prepared_prefix": legacy["prepared_prefix"],
                "copy_dir": legacy["copy_dir"],
                "ftp_dir": candidate.get("ftp_dir", legacy["ftp_dir"]),
                "send_to_server": legacy["send_to_server"],
                "send_to_ftp": legacy["send_to_ftp"],
            }
        ]

    normalized: list[dict] = []
    seen_ids: set[str] = set()
    errors: list[str] = []
    for index, raw in enumerate(raw_sources):
        source_label = f"source[{index}]"
        if not isinstance(raw, dict):
            errors.append(f"{source_label}: deve ser objeto/dict.")
            continue

        for required_key in ("id", "enabled", "remote_folder", "prepared_prefix"):
            if required_key not in raw:
                errors.append(f"{source_label}: campo obrigatorio ausente '{required_key}'.")
        if "enabled" in raw and not isinstance(raw.get("enabled"), bool):
            errors.append(f"{source_label}: campo 'enabled' deve ser booleano.")
        if "send_to_server" in raw and not isinstance(raw.get("send_to_server"), bool):
            errors.append(f"{source_label}: campo 'send_to_server' deve ser booleano.")
        if "send_to_ftp" in raw and not isinstance(raw.get("send_to_ftp"), bool):
            errors.append(f"{source_label}: campo 'send_to_ftp' deve ser booleano.")

        raw_id = str(raw.get("id") or "").strip()
        if not raw_id:
            errors.append(f"{source_label}: 'id' nao pode ser vazio.")
        raw_remote_folder = str(raw.get("remote_folder") or "").strip()
        if "remote_folder" in raw and not raw_remote_folder:
            errors.append(f"{source_label}: 'remote_folder' nao pode ser vazio.")
        raw_prepared_prefix = str(raw.get("prepared_prefix") or "").strip()
        if "prepared_prefix" in raw and not raw_prepared_prefix:
            errors.append(f"{source_label}: 'prepared_prefix' nao pode ser vazio.")
        source = _normalize_single_source(raw, index, targets)
        source_id = source["id"]
        try:
            _validate_filename_template_for_source(raw.get("filename_template"))
        except Exception as exc:
            errors.append(f"{source_label} ({source_id}): filename_template invalido ({exc}).")

        if source_id in seen_ids:
            errors.append(f"{source_label}: id duplicado detectado '{source_id}'.")
            continue
        seen_ids.add(source_id)

        if source["send_to_server"] and not source.get("copy_dir"):
            errors.append(
                f"{source_label} ({source_id}): send_to_server=True exige copy_dir configurado."
            )
        if source["send_to_ftp"] and not source.get("ftp_dir"):
            errors.append(
                f"{source_label} ({source_id}): send_to_ftp=True exige ftp_dir configurado."
            )

        normalized.append(source)

    if not normalized:
        normalized.append(_normalize_single_source(_legacy_source_definition(), 0, targets))
    _raise_source_config_errors(errors)
    if not any(bool(source.get("enabled", False)) for source in normalized):
        raise SourceConfigurationError(
            "Nenhum source habilitado em DOWNLOAD_SOURCES. Defina ao menos um source com enabled=True."
        )
    return normalized


def _resolve_sources_by_id(*, requested_targets: dict | None = None) -> dict[str, dict]:
    return {source["id"]: source for source in _resolve_download_sources(requested_targets=requested_targets)}


def _normalize_requested_sources(requested_sources: list[str] | None) -> list[str]:
    if not requested_sources:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_source in requested_sources:
        source_id = str(raw_source or "").strip()
        if not source_id:
            continue
        if source_id in seen:
            continue
        seen.add(source_id)
        normalized.append(source_id)
    return normalized


def _validate_requested_sources(requested_sources: list[str] | None, available_sources: list[dict]) -> list[str]:
    requested_ids = _normalize_requested_sources(requested_sources)
    if not requested_ids:
        return []

    available_ids: list[str] = []
    available_set: set[str] = set()
    for source in available_sources:
        source_id = str(source.get("id") or "").strip()
        if not source_id:
            continue
        if source_id in available_set:
            continue
        available_set.add(source_id)
        available_ids.append(source_id)

    missing = [source_id for source_id in requested_ids if source_id not in available_set]
    if missing:
        missing_text = ", ".join(missing)
        available_text = ", ".join(available_ids) if available_ids else "(nenhum)"
        raise SourceConfigurationError(
            f"Source(s) informado(s) nao encontrado(s): {missing_text}\n"
            f"Sources disponiveis: {available_text}"
        )
    return requested_ids


def _source_log_path(cycle_date: date, source_id: str) -> Path:
    return source_log_path(Path(LOG_DIR), cycle_date, source_id, folder=SOURCE_LOG_FOLDER)


def _source_logger(base_logger: logging.Logger, cycle_date: date, source_id: str) -> logging.Logger:
    return build_source_logger(
        base_logger,
        log_dir=Path(LOG_DIR),
        cycle_date=cycle_date,
        source_id=source_id,
        folder=SOURCE_LOG_FOLDER,
    )


def _default_targets_from_settings() -> dict:
    return {
        "server": bool(ENABLE_COPIES and COPY_TO_SERVER),
        "ftp": bool(ENABLE_COPIES and COPY_TO_FTP),
    }


def _normalize_targets(raw_targets: dict | None, *, fallback: dict | None = None) -> dict:
    normalized = {"server": False, "ftp": False}
    if fallback:
        normalized["server"] = bool(fallback.get("server", False))
        normalized["ftp"] = bool(fallback.get("ftp", False))
    if isinstance(raw_targets, dict):
        if "server" in raw_targets:
            normalized["server"] = bool(raw_targets.get("server"))
        if "ftp" in raw_targets:
            normalized["ftp"] = bool(raw_targets.get("ftp"))
    return normalized


def _state_targets(state: dict | None) -> dict:
    if isinstance(state, dict):
        return _normalize_targets(
            state.get("targets"),
            fallback=_default_targets_from_settings(),
        )
    return _default_targets_from_settings()


def _server_requested(state: dict | None = None) -> bool:
    return bool(_state_targets(state)["server"])


def _ftp_requested(state: dict | None = None) -> bool:
    return bool(_state_targets(state)["ftp"])


def _is_stage_required(stage: str, state: dict | None = None) -> bool:
    if stage == "send_server":
        return _server_requested(state)
    if stage == "send_ftp":
        return _ftp_requested(state)
    return True


def _stage_status_text(stage: str, stage_state: dict, state: dict) -> str:
    if not _is_stage_required(stage, state):
        return "SKIPPED"
    return "OK" if bool(stage_state.get("ok")) else "FALHOU"


def _state_path_for_date(cycle_date: date) -> Path:
    return Path(LOG_DIR) / f"state_{cycle_date.isoformat()}.json"


def _cycle_summary_path_for_date(cycle_date: date) -> Path:
    return Path(LOG_DIR) / f"cycle_summary_{cycle_date.isoformat()}.json"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_iso_datetime(raw_value: object) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(str(raw_value))
    except Exception:
        return None


def _duration_seconds(start: datetime | None, end: datetime | None) -> float | None:
    if not start or not end:
        return None
    delta = (end - start).total_seconds()
    if delta < 0:
        return None
    return round(delta, 3)


def _default_source_metrics() -> dict:
    return {
        "started_at": None,
        "finished_at": None,
        "duration_seconds": None,
        "result_category": None,
        "final_stage": None,
        "skipped_reason": None,
    }


def _default_stage_state() -> dict:
    return {
        "ok": False,
        "enabled": True,
        "tries": 0,
        "last_error": None,
        "last_attempt_ts": None,
    }


def _generate_run_id(now: datetime | None = None) -> str:
    current = now or datetime.now()
    return current.strftime("%H%M%S")


def _normalize_run_id(raw_run_id: object, *, fallback: str | None = None) -> str:
    if isinstance(raw_run_id, str) and RUN_ID_PATTERN.fullmatch(raw_run_id):
        return raw_run_id
    if fallback and RUN_ID_PATTERN.fullmatch(fallback):
        return fallback
    return _generate_run_id()


def _ensure_run_id_in_state(state: dict) -> str:
    run_id = _normalize_run_id(state.get("run_id"))
    state["run_id"] = run_id
    return run_id


def _source_targets(source: dict) -> dict:
    return {
        "server": bool(source.get("send_to_server", False)),
        "ftp": bool(source.get("send_to_ftp", False)),
    }


def _apply_non_requested_stage_defaults(state: dict) -> None:
    for stage_name in ("send_server", "send_ftp"):
        stage = state["stages"][stage_name]
        required = _is_stage_required(stage_name, state)
        stage["enabled"] = required
        if not required:
            stage["ok"] = True
            stage["last_error"] = None


def _new_item_state(source: dict, run_id: str) -> dict:
    now_iso = _now_iso()
    item = {
        "source_id": source["id"],
        "source": copy.deepcopy(source),
        "run_id": run_id,
        "targets": _normalize_targets(_source_targets(source)),
        "entered_retry": False,
        "status": "PENDING",
        "last_error": None,
        "paths": {
            "downloaded": None,
            "prepared": None,
        },
        "file": {
            "expected_name": None,
            "resolved_name": None,
            "found_in_listing": None,
            "listed_count": 0,
            "listed_at": None,
        },
        "stages": {stage: _default_stage_state() for stage in STAGE_ORDER},
        "source_signature": {
            "name": None,
            "size": None,
            "mtime": None,
        },
        "timestamps": {
            "created_at": now_iso,
            "updated_at": now_iso,
        },
        "metrics": _default_source_metrics(),
    }
    _apply_non_requested_stage_defaults(item)
    return item


def _new_cycle_state(
    cycle_date: date,
    *,
    targets: dict | None = None,
    sources: list[dict] | None = None,
) -> dict:
    run_id = _generate_run_id()
    normalized_targets = _normalize_targets(targets, fallback=_default_targets_from_settings())
    resolved_sources = sources or _resolve_download_sources(requested_targets=normalized_targets)
    state = {
        "schema_version": STATE_SCHEMA_VERSION,
        "date": cycle_date.isoformat(),
        "run_id": run_id,
        "targets": normalized_targets,
        "entered_retry": False,
        "sources": [copy.deepcopy(source) for source in resolved_sources],
        "items": {
            source["id"]: _new_item_state(source, run_id)
            for source in resolved_sources
        },
    }
    _sync_legacy_projection(state)
    return state


def _normalize_stage_map(raw_stages: dict | None) -> dict:
    stages = {stage: _default_stage_state() for stage in STAGE_ORDER}
    if not isinstance(raw_stages, dict):
        return stages

    for stage in STAGE_ORDER:
        raw_stage = raw_stages.get(stage, {})
        normalized = _default_stage_state()
        if isinstance(raw_stage, dict):
            normalized["ok"] = bool(raw_stage.get("ok", False))
            normalized["enabled"] = bool(raw_stage.get("enabled", True))
            try:
                normalized["tries"] = max(0, int(raw_stage.get("tries", 0) or 0))
            except (TypeError, ValueError):
                normalized["tries"] = 0
            normalized["last_error"] = raw_stage.get("last_error")
            normalized["last_attempt_ts"] = raw_stage.get("last_attempt_ts")
        stages[stage] = normalized
    return stages


def _normalize_item_state(raw_item: dict | None, source: dict, *, run_id: str) -> dict:
    item = _new_item_state(source, run_id)
    if not isinstance(raw_item, dict):
        return item

    item["run_id"] = _normalize_run_id(raw_item.get("run_id"), fallback=run_id)
    raw_targets = raw_item.get("targets")
    if isinstance(raw_targets, dict):
        item["targets"] = _normalize_targets(raw_targets, fallback=item["targets"])
    item["entered_retry"] = bool(raw_item.get("entered_retry", False))
    item["status"] = str(raw_item.get("status") or item["status"])
    item["last_error"] = raw_item.get("last_error")

    raw_paths = raw_item.get("paths")
    if isinstance(raw_paths, dict):
        item["paths"]["downloaded"] = raw_paths.get("downloaded")
        item["paths"]["prepared"] = raw_paths.get("prepared")

    raw_file = raw_item.get("file")
    if isinstance(raw_file, dict):
        item["file"]["expected_name"] = raw_file.get("expected_name")
        item["file"]["resolved_name"] = raw_file.get("resolved_name")
        item["file"]["found_in_listing"] = raw_file.get("found_in_listing")
        try:
            item["file"]["listed_count"] = max(0, int(raw_file.get("listed_count", 0) or 0))
        except (TypeError, ValueError):
            item["file"]["listed_count"] = 0
        item["file"]["listed_at"] = raw_file.get("listed_at")

    raw_source_signature = raw_item.get("source_signature")
    if isinstance(raw_source_signature, dict):
        item["source_signature"]["name"] = raw_source_signature.get("name")
        item["source_signature"]["size"] = raw_source_signature.get("size")
        item["source_signature"]["mtime"] = raw_source_signature.get("mtime")

    item["stages"] = _normalize_stage_map(raw_item.get("stages"))

    raw_timestamps = raw_item.get("timestamps")
    if isinstance(raw_timestamps, dict):
        item["timestamps"]["created_at"] = (
            raw_timestamps.get("created_at") or item["timestamps"]["created_at"]
        )
        item["timestamps"]["updated_at"] = (
            raw_timestamps.get("updated_at") or item["timestamps"]["updated_at"]
        )
    item["timestamps"]["updated_at"] = _now_iso()

    raw_metrics = raw_item.get("metrics")
    if isinstance(raw_metrics, dict):
        metrics = _default_source_metrics()
        metrics["started_at"] = raw_metrics.get("started_at")
        metrics["finished_at"] = raw_metrics.get("finished_at")
        metrics["duration_seconds"] = raw_metrics.get("duration_seconds")
        result_category = str(raw_metrics.get("result_category") or "").strip().lower()
        metrics["result_category"] = (
            result_category if result_category in OBSERVABILITY_RESULT_CATEGORIES else None
        )
        metrics["final_stage"] = raw_metrics.get("final_stage")
        metrics["skipped_reason"] = raw_metrics.get("skipped_reason")
        item["metrics"] = metrics

    _apply_non_requested_stage_defaults(item)
    return item


def _legacy_raw_item_from_state(raw_state: dict) -> dict:
    return {
        "run_id": raw_state.get("run_id"),
        "targets": raw_state.get("targets"),
        "entered_retry": raw_state.get("entered_retry", False),
        "status": "SUCCESS" if _state_all_required_stages_ok(raw_state) else "FAILED",
        "last_error": _stage_error_summary(raw_state),
        "paths": raw_state.get("paths"),
        "stages": raw_state.get("stages"),
        "source_signature": raw_state.get("source_signature"),
        "file": {
            "expected_name": None,
            "resolved_name": raw_state.get("source_signature", {}).get("name"),
            "found_in_listing": None,
            "listed_count": 0,
            "listed_at": None,
        },
    }


def _sync_legacy_projection(state: dict) -> None:
    items = state.get("items", {})
    if not isinstance(items, dict) or not items:
        return

    source_order = []
    for source in state.get("sources", []):
        source_id = source.get("id") if isinstance(source, dict) else None
        if source_id in items:
            source_order.append(source_id)
    if not source_order:
        source_order = list(items.keys())

    first_item = items[source_order[0]]
    state["paths"] = copy.deepcopy(first_item.get("paths", {}))
    state["stages"] = copy.deepcopy(first_item.get("stages", {}))
    state["source_signature"] = copy.deepcopy(first_item.get("source_signature", {}))
    state["entered_retry"] = any(
        bool(item.get("entered_retry", False))
        for item in items.values()
        if isinstance(item, dict)
    )


def _normalize_cycle_state(
    raw_state: dict,
    cycle_date: date,
    *,
    requested_targets: dict | None = None,
) -> dict:
    requested_targets = _normalize_targets(
        requested_targets,
        fallback=_default_targets_from_settings(),
    )
    configured_sources = _resolve_download_sources(requested_targets=requested_targets)
    if not isinstance(raw_state, dict):
        return _new_cycle_state(cycle_date, targets=requested_targets, sources=configured_sources)

    state = _new_cycle_state(cycle_date, targets=requested_targets, sources=configured_sources)
    state["schema_version"] = STATE_SCHEMA_VERSION
    state["run_id"] = _normalize_run_id(raw_state.get("run_id"), fallback=state.get("run_id"))

    raw_targets = raw_state.get("targets")
    if isinstance(raw_targets, dict):
        state["targets"] = _normalize_targets(raw_targets, fallback=state["targets"])

    raw_items = raw_state.get("items")
    if isinstance(raw_items, dict):
        for source in configured_sources:
            source_id = source["id"]
            state["items"][source_id] = _normalize_item_state(
                raw_items.get(source_id),
                source,
                run_id=state["run_id"],
            )
    else:
        # Compatibilidade com state legado (1 arquivo por ciclo).
        legacy_source = configured_sources[0]
        state["items"][legacy_source["id"]] = _normalize_item_state(
            _legacy_raw_item_from_state(raw_state),
            legacy_source,
            run_id=state["run_id"],
        )

    state["sources"] = [copy.deepcopy(source) for source in configured_sources]
    for source in configured_sources:
        if source["id"] not in state["items"]:
            state["items"][source["id"]] = _new_item_state(source, state["run_id"])
    _sync_legacy_projection(state)
    return state


def _save_cycle_state(state: dict, logger: logging.Logger) -> None:
    _sync_legacy_projection(state)
    state_path = _state_path_for_date(date.fromisoformat(state["date"]))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(".tmp")
    try:
        tmp_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(state_path)
    except Exception as exc:
        logger.warning("Falha ao persistir state em %s: %s", state_path, exc)
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _load_cycle_state(
    cycle_date: date,
    logger: logging.Logger,
    *,
    reset: bool = False,
    requested_targets: dict | None = None,
) -> dict:
    state_path = _state_path_for_date(cycle_date)
    requested_targets = _normalize_targets(
        requested_targets,
        fallback=_default_targets_from_settings(),
    )
    if reset and state_path.exists():
        try:
            state_path.unlink()
            logger.info("State do ciclo removido por --force-run: %s", state_path)
        except Exception as exc:
            logger.warning("Falha ao remover state antigo (%s): %s", state_path, exc)

    configured_sources = _resolve_download_sources(requested_targets=requested_targets)
    if not state_path.exists():
        state = _new_cycle_state(
            cycle_date,
            targets=requested_targets,
            sources=configured_sources,
        )
        _save_cycle_state(state, logger)
        return state

    try:
        loaded = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Falha ao ler state (%s). Reiniciando state do dia. Erro: %s", state_path, exc)
        state = _new_cycle_state(
            cycle_date,
            targets=requested_targets,
            sources=configured_sources,
        )
        _save_cycle_state(state, logger)
        return state

    state = _normalize_cycle_state(loaded, cycle_date, requested_targets=requested_targets)
    items = state.get("items", {})
    if isinstance(items, dict):
        for source_id, item_state in items.items():
            source_logger = _source_logger(logger, cycle_date, source_id)
            _invalidate_missing_checkpoint_paths(item_state, source_logger)

    loaded_targets = loaded.get("targets") if isinstance(loaded, dict) else None
    if isinstance(loaded_targets, dict):
        persisted_targets = _normalize_targets(loaded_targets, fallback=requested_targets)
        if persisted_targets != requested_targets:
            logger.warning(
                "State de %s ja existe com targets server=%s ftp=%s; mantendo targets persistidos.",
                cycle_date.isoformat(),
                persisted_targets["server"],
                persisted_targets["ftp"],
            )
    _save_cycle_state(state, logger)
    return state


def _read_last_success_date(logger: logging.Logger) -> date | None:
    try:
        raw = LAST_SUCCESS_FILE_PATH.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except Exception as exc:
        logger.warning("Falha ao ler estado de sucesso (%s): %s", LAST_SUCCESS_FILE_PATH, exc)
        return None

    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        logger.warning(
            "Estado de sucesso invalido em %s: %r (esperado YYYY-MM-DD).",
            LAST_SUCCESS_FILE_PATH,
            raw,
        )
        return None


def _has_success_for_today(logger: logging.Logger) -> bool:
    last_success = _read_last_success_date(logger)
    return bool(last_success and last_success == date.today())


def _store_success_date(success_date: date, logger: logging.Logger) -> None:
    LAST_SUCCESS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = LAST_SUCCESS_FILE_PATH.with_suffix(".tmp")
    try:
        tmp_path.write_text(success_date.isoformat(), encoding="utf-8")
        tmp_path.replace(LAST_SUCCESS_FILE_PATH)
    except Exception as exc:
        logger.warning("Falha ao persistir last_success em %s: %s", LAST_SUCCESS_FILE_PATH, exc)
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


@contextmanager
def instance_lock(logger: logging.Logger):
    LOCK_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    acquired = False
    try:
        fd = os.open(str(LOCK_FILE_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        acquired = True
        with os.fdopen(fd, "w", encoding="utf-8") as lock_fp:
            lock_fp.write(f"pid={os.getpid()} started_at={datetime.now().isoformat()}\n")
        logger.info("Lock de instancia adquirido: %s", LOCK_FILE_PATH)
        yield True
    except FileExistsError:
        logger.warning(
            "Outra instancia em execucao (lock ativo em %s). Encerrando sem executar ciclo.",
            LOCK_FILE_PATH,
        )
        yield False
    finally:
        if acquired:
            try:
                LOCK_FILE_PATH.unlink()
                logger.info("Lock de instancia removido: %s", LOCK_FILE_PATH)
            except FileNotFoundError:
                logger.warning("Lock ja removido antes do encerramento: %s", LOCK_FILE_PATH)
            except Exception as exc:
                logger.warning("Falha ao remover lock %s: %s", LOCK_FILE_PATH, exc)


def configurar_log():
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.getLogger(__name__).info("Log iniciado em: %s", log_path)


def validar_agendamento(logger: logging.Logger) -> bool:
    if not ENFORCE_SCHEDULE:
        return True
    now = datetime.now()
    if RUN_START_DATE:
        try:
            data_inicio = date.fromisoformat(str(RUN_START_DATE))
        except Exception:
            logger.error("RUN_START_DATE invalido: %s (use YYYY-MM-DD)", RUN_START_DATE)
            return False
        if now.date() < data_inicio:
            logger.info(
                "Execucao ignorada: data atual %s antes de %s.",
                now.strftime("%Y-%m-%d"),
                data_inicio.isoformat(),
            )
            return False
    if RUN_DAYS:
        try:
            dias_validos = {int(dia) for dia in RUN_DAYS}
        except (TypeError, ValueError):
            logger.error("RUN_DAYS invalido: %s (use inteiros 0-6)", RUN_DAYS)
            return False
        if now.weekday() not in dias_validos:
            logger.info(
                "Execucao ignorada: dia da semana nao permitido (hoje=%s).",
                now.weekday(),
            )
            return False
    if RUN_TIME:
        try:
            hora, minuto = RUN_TIME.split(":")
            horario_execucao = now.replace(
                hour=int(hora),
                minute=int(minuto),
                second=0,
                microsecond=0,
            )
        except Exception:
            logger.error("RUN_TIME invalido: %s (use HH:MM)", RUN_TIME)
            return False
        janela_fim = horario_execucao.replace(second=59, microsecond=999999)
        if now < horario_execucao:
            logger.info(
                "Execucao ignorada: horario atual %s antes de %s.",
                now.strftime("%H:%M"),
                RUN_TIME,
            )
            return False
        if now > janela_fim:
            logger.info(
                "Execucao ignorada: horario atual %s depois da janela de %s.",
                now.strftime("%H:%M:%S"),
                RUN_TIME,
            )
            return False
    return True


def _state_all_required_stages_ok(state: dict) -> bool:
    stages = state.get("stages", {})
    if not isinstance(stages, dict):
        return False
    for stage in STAGE_ORDER:
        if not _is_stage_required(stage, state):
            continue
        stage_state = stages.get(stage, {})
        if not isinstance(stage_state, dict):
            return False
        if not bool(stage_state.get("ok")):
            return False
    return True


def _stage_dependencies_ok(stage: str, state: dict) -> bool:
    deps = STAGE_DEPENDENCIES.get(stage, ())
    stages = state.get("stages", {})
    if not isinstance(stages, dict):
        return False
    return all(bool(stages.get(dep, {}).get("ok")) for dep in deps)


def _source_from_item_state(state: dict) -> dict:
    source = state.get("source")
    if isinstance(source, dict):
        return source
    return _legacy_source_definition()


def _ensure_item_runtime_shape(state: dict) -> None:
    if "file" not in state or not isinstance(state.get("file"), dict):
        state["file"] = {
            "expected_name": None,
            "resolved_name": None,
            "found_in_listing": None,
            "listed_count": 0,
            "listed_at": None,
        }
    if "timestamps" not in state or not isinstance(state.get("timestamps"), dict):
        state["timestamps"] = {
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
    if "metrics" not in state or not isinstance(state.get("metrics"), dict):
        state["metrics"] = _default_source_metrics()
    else:
        for key, value in _default_source_metrics().items():
            state["metrics"].setdefault(key, value)
    if "source" not in state or not isinstance(state.get("source"), dict):
        state["source"] = _legacy_source_definition()


def _prepared_filename_for_cycle(cycle_date: date, state: dict) -> str:
    run_id = _ensure_run_id_in_state(state)
    source = _source_from_item_state(state)
    prepared_prefix = str(source.get("prepared_prefix") or FILE_PREFIX)
    return f"{prepared_prefix}{cycle_date.strftime('%Y%m%d')}_{run_id}.csv"


def _downloaded_path_from_state(state: dict) -> Path | None:
    raw = state.get("paths", {}).get("downloaded")
    return Path(raw) if raw else None


def _prepared_path_from_state(state: dict) -> Path | None:
    raw = state.get("paths", {}).get("prepared")
    return Path(raw) if raw else None


def _invalidate_missing_checkpoint_paths(state: dict, logger: logging.Logger) -> None:
    def _invalidate_stage(stage: str) -> None:
        stage_state = state["stages"][stage]
        stage_state["ok"] = False
        stage_state["tries"] = 0
        stage_state["last_error"] = None

    downloaded = _downloaded_path_from_state(state)
    if downloaded is not None and not downloaded.exists():
        logger.warning(
            "Checkpoint invalido para download: arquivo nao existe em disco (%s).",
            downloaded,
        )
        state["paths"]["downloaded"] = None
        _invalidate_stage("download")

    prepared = _prepared_path_from_state(state)
    if prepared is not None and not prepared.exists():
        logger.warning(
            "Checkpoint invalido para prepare: arquivo nao existe em disco (%s).",
            prepared,
        )
        state["paths"]["prepared"] = None
        _invalidate_stage("prepare")
        _invalidate_stage("send_server")
        _invalidate_stage("send_ftp")


def _server_destination_path(prepared_path: Path, state: dict) -> Path:
    source = _source_from_item_state(state)
    copy_dir = source.get("copy_dir") or COPY_DIR
    return Path(copy_dir) / prepared_path.name


def _build_source_signature(path: Path) -> dict:
    stat = path.stat()
    return {
        "name": path.name,
        "size": int(stat.st_size),
        "mtime": int(stat.st_mtime),
    }


def _recent_file_examples(itens: list[dict], limit: int = 5) -> list[str]:
    candidatos = []
    for item in itens:
        nome = item.get("name")
        if not isinstance(nome, str):
            continue
        if bool(item.get("isDirectory")):
            continue
        candidatos.append((str(item.get("dateModified") or ""), nome))

    candidatos.sort(reverse=True)
    return [nome for _, nome in candidatos[:limit]]


def _run_download_stage(logger: logging.Logger, state: dict, cycle_date: date) -> None:
    _ensure_item_runtime_shape(state)
    source = _source_from_item_state(state)
    source_id = source.get("id", "default")
    remote_folder = source.get("remote_folder") or FILE_MANAGER_EXPORT_FOLDER
    filename_template = source.get("filename_template")

    driver = None
    wait = None
    try:
        logger.info(
            "Iniciando login no REVO360 para o ciclo %s (source=%s)",
            cycle_date.isoformat(),
            source_id,
        )
        driver, wait = criar_driver()
        realizar_login(driver, wait)
        logger.info("Login realizado com sucesso")
        logger.info("File Manager sera consultado via navegacao e form submit no navegador autenticado")

        logger.info("Consultando pasta remota no navegador autenticado: %s", remote_folder)
        try:
            itens = listar_arquivos_api_no_browser(driver, remote_folder)
            logger.info(
                "Listagem via navegacao do navegador concluida: %s item(ns) em '%s'",
                len(itens),
                remote_folder,
            )
            state["file"]["listed_count"] = len(itens)
            state["file"]["listed_at"] = datetime.now().isoformat(timespec="seconds")
        except SessionExpiredError as exc:
            logger.exception(
                "Sessao do navegador do REVO360 parece expirada durante a listagem da pasta '%s'",
                remote_folder,
            )
            raise RuntimeError(
                "Sessao do navegador do REVO360 parece expirada ou a API nao ficou acessivel no contexto autenticado durante a listagem."
            ) from exc
        except (UnexpectedApiResponseError, FileManagerApiError) as exc:
            logger.exception(
                "Falha ao listar arquivos no navegador autenticado na pasta '%s'",
                remote_folder,
            )
            raise RuntimeError(
                f"Falha ao listar arquivos na pasta '{remote_folder}'."
            ) from exc

        expected_name = _render_filename_template(filename_template, cycle_date)
        state["file"]["expected_name"] = expected_name
        try:
            if expected_name:
                csv_do_ciclo = None
                for item in itens:
                    if bool(item.get("isDirectory")):
                        continue
                    if str(item.get("name") or "").strip() == expected_name:
                        csv_do_ciclo = item
                        break
                if csv_do_ciclo is None:
                    raise RuntimeError(
                        f"Arquivo esperado nao encontrado na listagem: {expected_name}"
                    )
            else:
                csv_do_ciclo = selecionar_csv_por_data(itens, cycle_date)
                expected_name = str(csv_do_ciclo.get("name") or "").strip()
                state["file"]["expected_name"] = expected_name
        except Exception as exc:
            exemplos = _recent_file_examples(itens)
            state["file"]["found_in_listing"] = False
            logger.error(
                "Arquivo do ciclo %s ainda nao disponivel na pasta '%s'. Itens retornados=%s. Exemplos recentes=%s",
                cycle_date.isoformat(),
                remote_folder,
                len(itens),
                ", ".join(exemplos) if exemplos else "nenhum",
            )
            raise RuntimeError(
                f"Arquivo CSV do ciclo {cycle_date.isoformat()} ainda nao esta disponivel na pasta '{remote_folder}'. Isso pode ocorrer se a geracao do arquivo ainda nao terminou no REVO360."
            ) from exc

        nome_esperado = str(csv_do_ciclo["name"]).strip()
        state["file"]["resolved_name"] = nome_esperado
        state["file"]["found_in_listing"] = True
        logger.info(
            "Arquivo selecionado para o ciclo %s (source=%s): %s",
            cycle_date.isoformat(),
            source_id,
            nome_esperado,
        )

        limpar_arquivos_download_anteriores(nome_esperado)
        logger.info("Iniciando download via form submit no navegador: %s", nome_esperado)
        download_started_at = monotonic()

        try:
            baixar_arquivo_via_form_submit_no_browser(
                driver,
                remote_folder,
                nome_esperado,
            )
        except SessionExpiredError as exc:
            logger.exception(
                "Sessao do navegador do REVO360 parece expirada durante o download do arquivo '%s'",
                nome_esperado,
            )
            raise RuntimeError(
                f"Sessao do navegador do REVO360 parece expirada ou a API nao ficou acessivel no contexto autenticado durante o download de '{nome_esperado}'."
            ) from exc
        except (UnexpectedApiResponseError, FileManagerApiError) as exc:
            logger.exception("Falha ao disparar download via navegador: %s", nome_esperado)
            raise RuntimeError(
                f"Falha ao disparar o download do arquivo '{nome_esperado}' no navegador."
            ) from exc

        logger.info("Download disparado. Aguardando arquivo no diretorio configurado: %s", DOWNLOAD_DIR)
        try:
            downloaded = aguardar_download(wait, nome_esperado)
        except Exception as exc:
            logger.exception(
                "Arquivo esperado nao apareceu no diretorio apos form submit: %s",
                nome_esperado,
            )
            raise RuntimeError(
                f"O arquivo '{nome_esperado}' nao apareceu no diretorio de download apos o disparo no navegador para o ciclo {cycle_date.isoformat()}."
            ) from exc

        if not downloaded.exists():
            logger.error("Download via navegador nao encontrado em disco: %s", downloaded)
            raise RuntimeError(
                f"Download nao encontrado em disco apos o disparo no navegador: {downloaded}"
            )
        if int(downloaded.stat().st_size) <= 0:
            logger.error("Arquivo baixado via navegador esta vazio: %s", downloaded)
            raise RuntimeError(f"Arquivo baixado esta vazio: {downloaded.name}")

        state["paths"]["downloaded"] = str(downloaded)
        state["source_signature"] = _build_source_signature(downloaded)
        state["last_error"] = None
        logger.info("Download via navegador concluido: %s", nome_esperado)
        logger.info("Arquivo salvo em: %s", downloaded)
        logger.info("Tamanho final do arquivo salvo: %s bytes", downloaded.stat().st_size)
        logger.info("Tempo total do download: %.2fs", monotonic() - download_started_at)
    finally:
        if driver:
            driver.quit()


def _run_prepare_stage(logger: logging.Logger, state: dict, cycle_date: date) -> None:
    _ensure_item_runtime_shape(state)
    downloaded = _downloaded_path_from_state(state)
    if not downloaded or not downloaded.exists():
        raise RuntimeError("Arquivo baixado nao encontrado para prepare.")

    existing_prepared = _prepared_path_from_state(state)
    if existing_prepared is not None:
        prepared_path = existing_prepared
    else:
        prepared_name = _prepared_filename_for_cycle(cycle_date, state)
        prepared_path = Path(DOWNLOAD_DIR) / prepared_name
    prepared_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = prepared_path.with_suffix(prepared_path.suffix + ".tmp")

    if tmp_path.exists():
        tmp_path.unlink()

    try:
        shutil.copy2(downloaded, tmp_path)
        remover_cabecalho_csv(tmp_path)
        os.replace(tmp_path, prepared_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

    state["paths"]["prepared"] = str(prepared_path)
    logger.info("Prepare concluido: %s", prepared_path)

def _run_send_server_stage(logger: logging.Logger, state: dict) -> None:
    _ensure_item_runtime_shape(state)
    prepared = _prepared_path_from_state(state)
    if not prepared or not prepared.exists():
        raise RuntimeError("Arquivo preparado nao encontrado para envio ao servidor.")

    destino = _server_destination_path(prepared, state)
    if destino.exists():
        try:
            if int(destino.stat().st_size) == int(prepared.stat().st_size):
                logger.info(
                    "Servidor ja contem arquivo com mesmo tamanho (%s). Marcando como concluido.",
                    destino,
                )
                return
            logger.info(
                "Arquivo ja existe no servidor com tamanho diferente (%s). Reenviando.",
                destino,
            )
        except Exception:
            logger.warning("Falha ao comparar tamanho no servidor (%s). Tentando reenviar.", destino)

    source = _source_from_item_state(state)
    copiar_arquivo(prepared, copy_dir=source.get("copy_dir") or COPY_DIR)


def _run_send_ftp_stage(logger: logging.Logger, state: dict) -> None:
    _ensure_item_runtime_shape(state)
    prepared = _prepared_path_from_state(state)
    if not prepared or not prepared.exists():
        raise RuntimeError("Arquivo preparado nao encontrado para envio FTP.")

    source = _source_from_item_state(state)
    ftp_dir = source.get("ftp_dir")
    if arquivo_ftp_existe_com_mesmo_tamanho(prepared, ftp_dir=ftp_dir):
        logger.info(
            "FTP ja contem arquivo com mesmo tamanho (%s). Marcando como concluido.",
            prepared.name,
        )
        return

    enviar_arquivo_ftp(prepared, ftp_dir=ftp_dir)


def _run_single_stage(stage: str, logger: logging.Logger, state: dict, cycle_date: date) -> None:
    if stage == "download":
        _run_download_stage(logger, state, cycle_date)
        return
    if stage == "prepare":
        _run_prepare_stage(logger, state, cycle_date)
        return
    if stage == "send_server":
        _run_send_server_stage(logger, state)
        return
    if stage == "send_ftp":
        _run_send_ftp_stage(logger, state)
        return
    raise RuntimeError(f"Etapa desconhecida: {stage}")


def _stage_error_summary(state: dict) -> str | None:
    failed = []
    for stage in STAGE_ORDER:
        if not _is_stage_required(stage, state):
            continue
        stage_state = state["stages"][stage]
        if stage_state.get("ok"):
            continue
        erro = stage_state.get("last_error") or "Sem detalhes"
        failed.append(f"{STAGE_LABELS[stage]}: {erro}")
    if not failed:
        return None
    return " | ".join(failed)


def _build_execution_summary(
    state: dict,
    *,
    attempt: int,
    attempts_total: int,
    success: bool,
    execution_start: datetime,
    will_retry: bool,
    retry_delay_seconds: int,
) -> dict:
    source = _source_from_item_state(state)
    source_id = str(source.get("id") or "default")
    file_state = state.get("file", {})
    expected_name = file_state.get("expected_name") if isinstance(file_state, dict) else None
    steps = [
        f"Source: {source_id}",
        f"Tentativa: {attempt}/{attempts_total}",
    ]
    if expected_name:
        steps.append(f"Arquivo esperado: {expected_name}")
    for stage in STAGE_ORDER:
        stage_state = state["stages"][stage]
        status_text = _stage_status_text(stage, stage_state, state)
        tries = int(stage_state.get("tries", 0) or 0)
        line = f"{STAGE_LABELS[stage]}: {status_text} (tentativas: {tries})"
        if status_text == "FALHOU" and stage_state.get("last_error"):
            line += f" erro={stage_state['last_error']}"
        steps.append(line)
    if will_retry:
        steps.append(f"Retry em {retry_delay_seconds}s")

    prepared = _prepared_path_from_state(state)
    downloaded = _downloaded_path_from_state(state)
    filename = prepared.name if prepared else (downloaded.name if downloaded else None)

    return {
        "status": "SUCESSO" if success else "FALHA",
        "start_time": execution_start,
        "end_time": datetime.now(),
        "filename": filename,
        "source_id": source_id,
        "expected_name": expected_name,
        "steps_executed": steps,
        "error_message": None if success else _stage_error_summary(state),
    }


def _build_resumo_from_state(
    state: dict,
    *,
    attempt: int,
    attempts_total: int,
    success: bool,
    will_retry: bool,
    retry_delay_seconds: int,
    policy: str,
    final_result: str | None = None,
) -> dict:
    downloaded = _downloaded_path_from_state(state)
    prepared = _prepared_path_from_state(state)
    source = _source_from_item_state(state)
    file_state = state.get("file", {})
    expected_name = file_state.get("expected_name") if isinstance(file_state, dict) else None
    resolved_name = file_state.get("resolved_name") if isinstance(file_state, dict) else None
    return {
        "success": success,
        "error": _stage_error_summary(state),
        "attempt": attempt,
        "attempts_total": attempts_total,
        "will_retry": will_retry,
        "next_retry_in_seconds": retry_delay_seconds if will_retry else None,
        "notification_policy": policy,
        "source_id": source.get("id"),
        "source_remote_folder": source.get("remote_folder"),
        "expected_name": expected_name,
        "resolved_name": resolved_name,
        "original_name": downloaded.name if downloaded else None,
        "final_name": prepared.name if prepared else None,
        "server_requested": _server_requested(state),
        "ftp_requested": _ftp_requested(state),
        "server_sent": bool(state["stages"]["send_server"]["ok"]),
        "ftp_sent": bool(state["stages"]["send_ftp"]["ok"]),
        "whatsapp_requested": False,
        "whatsapp_total": 0,
        "whatsapp_sent": 0,
        "whatsapp_failed": 0,
        "whatsapp_failures": [],
        "stages": copy.deepcopy(state["stages"]),
        "entered_retry": bool(state.get("entered_retry", False)),
        "attempt_final": attempt,
        "final_result": final_result,
    }


def executar_fluxo_por_etapas(
    logger: logging.Logger,
    state: dict,
    cycle_date: date,
    *,
    attempt: int,
    attempts_total: int,
    retry_enabled: bool,
    retry_delay_seconds: int,
    cycle_state: dict | None = None,
) -> tuple[bool, dict, dict, dict]:
    persist_state = cycle_state if isinstance(cycle_state, dict) else state

    def _save() -> None:
        _save_cycle_state(persist_state, logger)

    return execute_stage_flow(
        logger=logger,
        state=state,
        cycle_date=cycle_date,
        attempt=attempt,
        attempts_total=attempts_total,
        retry_enabled=retry_enabled,
        retry_delay_seconds=retry_delay_seconds,
        stage_order=STAGE_ORDER,
        is_stage_required=_is_stage_required,
        stage_dependencies_ok=_stage_dependencies_ok,
        run_single_stage=_run_single_stage,
        save_state=_save,
        ensure_item_runtime_shape=_ensure_item_runtime_shape,
        build_resumo_from_state=_build_resumo_from_state,
        build_execution_summary=_build_execution_summary,
        stage_error_summary=_stage_error_summary,
    )


def _safe_emit_notifications(resumo: dict, execution_summary: dict, logger: logging.Logger) -> None:
    try:
        emitir_notificacoes(resumo, execution_summary, logger)
    except Exception as exc:
        logger.exception(
            "Falha inesperada ao enviar notificacoes (ignorada para nao afetar o pipeline): %s",
            exc,
        )


def _enabled_sources_from_cycle_state(cycle_state: dict) -> list[dict]:
    sources = cycle_state.get("sources", [])
    if not isinstance(sources, list):
        return []
    return [source for source in sources if isinstance(source, dict) and bool(source.get("enabled", True))]


def _disabled_sources_from_cycle_state(cycle_state: dict) -> list[dict]:
    sources = cycle_state.get("sources", [])
    if not isinstance(sources, list):
        return []
    return [source for source in sources if isinstance(source, dict) and not bool(source.get("enabled", True))]


def _all_enabled_sources_completed(cycle_state: dict) -> bool:
    items = cycle_state.get("items", {})
    if not isinstance(items, dict):
        return False
    for source in _enabled_sources_from_cycle_state(cycle_state):
        source_id = source.get("id")
        item_state = items.get(source_id)
        if not isinstance(item_state, dict):
            return False
        if not _state_all_required_stages_ok(item_state):
            return False
    return True


def _first_failed_stage(item_state: dict) -> str | None:
    stages = item_state.get("stages", {})
    if not isinstance(stages, dict):
        return None
    for stage in STAGE_ORDER:
        if not _is_stage_required(stage, item_state):
            continue
        stage_state = stages.get(stage, {})
        if not isinstance(stage_state, dict):
            return stage
        if not bool(stage_state.get("ok", False)):
            return stage
    return None


def _last_completed_stage(item_state: dict) -> str | None:
    stages = item_state.get("stages", {})
    if not isinstance(stages, dict):
        return None
    last_stage = None
    for stage in STAGE_ORDER:
        if not _is_stage_required(stage, item_state):
            continue
        stage_state = stages.get(stage, {})
        if not isinstance(stage_state, dict):
            break
        if not bool(stage_state.get("ok", False)):
            break
        last_stage = stage
    return last_stage


def _source_result_category(source: dict, item_state: dict | None) -> str:
    if not bool(source.get("enabled", True)):
        return "skipped"
    if not isinstance(item_state, dict):
        return "skipped"

    metrics = item_state.get("metrics", {})
    if isinstance(metrics, dict):
        explicit = str(metrics.get("result_category") or "").strip().lower()
        if explicit in OBSERVABILITY_RESULT_CATEGORIES:
            return explicit
        if str(metrics.get("skipped_reason") or "").strip() == "already_completed":
            return "skipped"

    if _state_all_required_stages_ok(item_state):
        return "success"

    file_state = item_state.get("file", {})
    if isinstance(file_state, dict) and file_state.get("found_in_listing") is False:
        return "not_found"

    status = str(item_state.get("status") or "").upper()
    if status in {"FAILED", "RETRY"}:
        return "failed"

    return "skipped"


def _source_final_stage(item_state: dict | None) -> str | None:
    if not isinstance(item_state, dict):
        return None
    failed_stage = _first_failed_stage(item_state)
    if failed_stage:
        return failed_stage
    return _last_completed_stage(item_state)


def _source_observability_entry(source: dict, item_state: dict | None) -> dict:
    source_id = str(source.get("id") or "<sem-id>")
    enabled = bool(source.get("enabled", True))
    item = item_state if isinstance(item_state, dict) else {}
    file_state = item.get("file", {}) if isinstance(item.get("file"), dict) else {}
    metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}
    targets = _state_targets(item)
    result_category = _source_result_category(source, item_state)
    final_stage = _source_final_stage(item_state)
    started_at = metrics.get("started_at")
    finished_at = metrics.get("finished_at")
    duration_seconds = metrics.get("duration_seconds")
    if duration_seconds is None:
        duration_seconds = _duration_seconds(
            _parse_iso_datetime(started_at),
            _parse_iso_datetime(finished_at),
        )
    return {
        "source_id": source_id,
        "enabled": enabled,
        "status": str(item.get("status") or "UNKNOWN"),
        "result_category": result_category,
        "final_stage": final_stage,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "expected_name": file_state.get("expected_name"),
        "resolved_name": file_state.get("resolved_name"),
        "found_in_listing": file_state.get("found_in_listing"),
        "server_requested": bool(targets.get("server")),
        "ftp_requested": bool(targets.get("ftp")),
        "last_error": item.get("last_error"),
    }


def _cycle_stats_from_source_entries(entries: list[dict]) -> dict:
    enabled_entries = [entry for entry in entries if bool(entry.get("enabled", False))]
    disabled_entries = [entry for entry in entries if not bool(entry.get("enabled", False))]
    return {
        "sources_total": len(entries),
        "sources_enabled": len(enabled_entries),
        "sources_success": sum(1 for entry in enabled_entries if entry.get("result_category") == "success"),
        "sources_failed": sum(1 for entry in enabled_entries if entry.get("result_category") == "failed"),
        "sources_not_found": sum(1 for entry in enabled_entries if entry.get("result_category") == "not_found"),
        "sources_skipped": sum(1 for entry in enabled_entries if entry.get("result_category") == "skipped"),
        "sources_disabled": len(disabled_entries),
    }


def _build_cycle_summary(
    *,
    cycle_state: dict,
    cycle_date: date,
    cycle_started_at: datetime,
    cycle_finished_at: datetime,
    duration_seconds: float,
    cycle_success: bool,
) -> dict:
    items = cycle_state.get("items", {})
    sources_entries = []
    for source in cycle_state.get("sources", []):
        if not isinstance(source, dict):
            continue
        source_id = str(source.get("id") or "<sem-id>")
        item_state = items.get(source_id) if isinstance(items, dict) else None
        sources_entries.append(_source_observability_entry(source, item_state))
    stats = _cycle_stats_from_source_entries(sources_entries)
    return {
        "cycle_date": cycle_date.isoformat(),
        "run_id": cycle_state.get("run_id"),
        "started_at": cycle_started_at.isoformat(timespec="seconds"),
        "finished_at": cycle_finished_at.isoformat(timespec="seconds"),
        "duration_seconds": round(max(0.0, float(duration_seconds)), 3),
        "status": "SUCCESS" if cycle_success else "FAILED",
        "stats": stats,
        "sources": sources_entries,
    }


def _save_cycle_summary(cycle_summary: dict, logger: logging.Logger) -> None:
    cycle_date = date.fromisoformat(str(cycle_summary.get("cycle_date")))
    path = _cycle_summary_path_for_date(cycle_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    try:
        tmp_path.write_text(
            json.dumps(cycle_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)
        logger.info("Resumo observabilidade persistido em %s", path)
    except Exception as exc:
        logger.warning("Falha ao persistir resumo observabilidade (%s): %s", path, exc)
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def _log_cycle_sources_summary(cycle_summary: dict, logger: logging.Logger) -> None:
    logger.info("Resumo final do ciclo por source (padrao observabilidade):")
    for entry in cycle_summary.get("sources", []):
        if not isinstance(entry, dict):
            continue
        logger.info(
            "source=%s enabled=%s category=%s status=%s final_stage=%s started=%s finished=%s duration_s=%s expected=%s resolved=%s server=%s ftp=%s error=%s",
            entry.get("source_id") or "-",
            bool(entry.get("enabled", False)),
            entry.get("result_category") or "-",
            entry.get("status") or "-",
            entry.get("final_stage") or "-",
            entry.get("started_at") or "-",
            entry.get("finished_at") or "-",
            entry.get("duration_seconds") if entry.get("duration_seconds") is not None else "-",
            entry.get("expected_name") or "-",
            entry.get("resolved_name") or "-",
            bool(entry.get("server_requested", False)),
            bool(entry.get("ftp_requested", False)),
            entry.get("last_error") or "-",
        )

    stats = cycle_summary.get("stats", {})
    logger.info(
        "Resumo do ciclo: status=%s started=%s finished=%s duration_s=%s enabled=%s success=%s failed=%s not_found=%s skipped=%s disabled=%s",
        cycle_summary.get("status"),
        cycle_summary.get("started_at"),
        cycle_summary.get("finished_at"),
        cycle_summary.get("duration_seconds"),
        stats.get("sources_enabled", 0),
        stats.get("sources_success", 0),
        stats.get("sources_failed", 0),
        stats.get("sources_not_found", 0),
        stats.get("sources_skipped", 0),
        stats.get("sources_disabled", 0),
    )


def _run_source_with_retries(
    cycle_state: dict,
    source: dict,
    logger: logging.Logger,
    cycle_date: date,
    *,
    total_attempts: int,
    retry_enabled: bool,
    retry_delay_seconds: int,
) -> bool:
    source_id = source["id"]
    source_logger = _source_logger(logger, cycle_date, source_id)
    items = cycle_state.setdefault("items", {})
    if source_id not in items or not isinstance(items.get(source_id), dict):
        items[source_id] = _new_item_state(source, cycle_state.get("run_id", _generate_run_id()))
    item_state = items[source_id]
    _ensure_item_runtime_shape(item_state)
    metrics = item_state.setdefault("metrics", _default_source_metrics())
    already_completed = _state_all_required_stages_ok(item_state)
    if not metrics.get("started_at"):
        metrics["started_at"] = _now_iso()
    metrics["skipped_reason"] = "already_completed" if already_completed else None
    item_state["timestamps"]["updated_at"] = _now_iso()

    def _save() -> None:
        _save_cycle_state(cycle_state, source_logger)

    _save()
    source_start = datetime.now()
    source_start_monotonic = monotonic()
    source_success = False
    try:
        source_success = process_source_with_retries(
            source=source,
            source_logger=source_logger,
            cycle_state=cycle_state,
            item_state=item_state,
            cycle_date=cycle_date,
            total_attempts=total_attempts,
            retry_enabled=retry_enabled,
            retry_delay_seconds=retry_delay_seconds,
            save_state=_save,
            ensure_item_runtime_shape=_ensure_item_runtime_shape,
            normalize_targets=_normalize_targets,
            source_targets=_source_targets,
            apply_non_requested_stage_defaults=_apply_non_requested_stage_defaults,
            invalidate_missing_checkpoint_paths=_invalidate_missing_checkpoint_paths,
            state_all_required_stages_ok=_state_all_required_stages_ok,
            execute_flow=executar_fluxo_por_etapas,
            build_resumo_from_state=_build_resumo_from_state,
            build_execution_summary=_build_execution_summary,
            safe_emit_notifications=_safe_emit_notifications,
            stage_error_summary=_stage_error_summary,
            sleep_func=sleep,
        )
        return source_success
    finally:
        source_finished = datetime.now()
        metrics["finished_at"] = source_finished.isoformat(timespec="seconds")
        metrics["duration_seconds"] = round(monotonic() - source_start_monotonic, 3)
        metrics["final_stage"] = _source_final_stage(item_state)
        if already_completed and source_success:
            metrics["result_category"] = "skipped"
        else:
            metrics["result_category"] = _source_result_category(source, item_state)
        item_state["timestamps"]["updated_at"] = _now_iso()
        _save()
        source_logger.info(
            "Metricas do source: started=%s finished=%s duration_s=%s category=%s final_stage=%s",
            source_start.isoformat(timespec="seconds"),
            metrics["finished_at"],
            metrics["duration_seconds"],
            metrics["result_category"],
            metrics["final_stage"] or "-",
        )


def run_with_retries(
    logger: logging.Logger,
    *,
    force_run: bool = False,
    cycle_date: date | None = None,
    targets: dict | None = None,
    requested_sources: list[str] | None = None,
) -> int:
    cycle_date = cycle_date or date.today()
    cycle_started_at = datetime.now()
    cycle_started_monotonic = monotonic()
    logger.info("Inicio do ciclo %s em %s", cycle_date.isoformat(), cycle_started_at.isoformat(timespec="seconds"))

    def _finalize_cycle(cycle_state: dict, *, cycle_success: bool) -> None:
        cycle_finished_at = datetime.now()
        duration_seconds = monotonic() - cycle_started_monotonic
        cycle_summary = _build_cycle_summary(
            cycle_state=cycle_state,
            cycle_date=cycle_date,
            cycle_started_at=cycle_started_at,
            cycle_finished_at=cycle_finished_at,
            duration_seconds=duration_seconds,
            cycle_success=cycle_success,
        )
        _log_cycle_sources_summary(cycle_summary, logger)
        _save_cycle_summary(cycle_summary, logger)

    try:
        preflight_sources = _resolve_download_sources(requested_targets=targets)
        validated_requested_sources = _validate_requested_sources(requested_sources, preflight_sources)
    except SourceConfigurationError as exc:
        logger.error(
            "Falha de validacao da configuracao de sources. Corrija DOWNLOAD_SOURCES antes de executar.\n%s",
            exc,
        )
        logger.error("Resumo do ciclo %s: status=FAILED category=invalid_config", cycle_date.isoformat())
        raise

    retry_enabled = bool(RETRY_ON_FAILURE_ENABLED)
    try:
        max_attempts_cfg = max(1, int(RETRY_MAX_ATTEMPTS))
    except (TypeError, ValueError):
        logger.warning("RETRY_MAX_ATTEMPTS invalido (%s). Usando 1 tentativa.", RETRY_MAX_ATTEMPTS)
        max_attempts_cfg = 1
    try:
        retry_delay_seconds = max(0, int(RETRY_DELAY_SECONDS))
    except (TypeError, ValueError):
        logger.warning("RETRY_DELAY_SECONDS invalido (%s). Usando 0 segundos.", RETRY_DELAY_SECONDS)
        retry_delay_seconds = 0

    total_attempts = max_attempts_cfg if retry_enabled else 1
    cycle_state = _load_cycle_state(
        cycle_date,
        logger,
        reset=force_run,
        requested_targets=targets,
    )
    selected_targets = _state_targets(cycle_state)
    logger.info(
        "Ciclo %s carregado com targets: server=%s ftp=%s",
        cycle_date.isoformat(),
        selected_targets["server"],
        selected_targets["ftp"],
    )
    logger.info(
        "Pre-validacao de configuracao concluida: %s source(s) configurado(s).",
        len(preflight_sources),
    )

    enabled_sources = _enabled_sources_from_cycle_state(cycle_state)
    disabled_sources = _disabled_sources_from_cycle_state(cycle_state)
    if validated_requested_sources:
        if len(validated_requested_sources) == 1:
            logger.info("Source selecionado manualmente: %s", validated_requested_sources[0])
        else:
            logger.info(
                "Sources selecionados manualmente: %s",
                ", ".join(validated_requested_sources),
            )
        requested_set = set(validated_requested_sources)
        enabled_sources = [source for source in enabled_sources if source.get("id") in requested_set]
    enabled_ids = [str(source.get("id")) for source in enabled_sources]
    disabled_ids = [str(source.get("id")) for source in disabled_sources]
    if enabled_ids:
        logger.info("Sources habilitados para este ciclo (%s): %s", len(enabled_ids), ", ".join(enabled_ids))
    if disabled_ids:
        logger.info("Sources ignorados por estarem desabilitados (%s): %s", len(disabled_ids), ", ".join(disabled_ids))

    if not enabled_sources:
        if validated_requested_sources:
            raise SourceConfigurationError(
                "Nenhum source habilitado entre os sources selecionados: "
                + ", ".join(validated_requested_sources)
            )
        raise SourceConfigurationError(
            f"Nenhum source habilitado para o ciclo {cycle_date.isoformat()}."
        )

    if _all_enabled_sources_completed(cycle_state):
        logger.info("State do ciclo %s ja estava concluido para todos os sources habilitados.", cycle_date)
        _finalize_cycle(cycle_state, cycle_success=True)
        if cycle_date == date.today():
            _store_success_date(cycle_date, logger)
        return 0

    cycle_success = True
    for index, source in enumerate(enabled_sources, start=1):
        source_id = str(source.get("id") or "<sem-id>")
        logger.info("Processando source %s/%s: %s", index, len(enabled_sources), source_id)
        source_success = _run_source_with_retries(
            cycle_state,
            source,
            logger,
            cycle_date,
            total_attempts=total_attempts,
            retry_enabled=retry_enabled,
            retry_delay_seconds=retry_delay_seconds,
        )
        logger.info(
            "Source %s finalizado com status=%s",
            source_id,
            "SUCCESS" if source_success else "FAILED",
        )
        if not source_success:
            cycle_success = False

    _finalize_cycle(cycle_state, cycle_success=cycle_success)
    if cycle_success and cycle_date == date.today():
        _store_success_date(cycle_date, logger)

    return 0 if cycle_success else 1


def _parse_manual_cycle_date(raw_value: str) -> date:
    if not re.fullmatch(r"\d{2}-\d{2}-\d{4}", str(raw_value or "")):
        raise argparse.ArgumentTypeError(
            "Data invalida para --run-anytime. Use apenas DD-MM-YYYY (ex.: 26-02-2026)."
        )
    try:
        return datetime.strptime(raw_value, MANUAL_CYCLE_DATE_FORMAT).date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Data invalida para --run-anytime. Use apenas DD-MM-YYYY (ex.: 26-02-2026)."
        ) from exc


def _resolve_manual_targets(local_enabled: bool, ftp_enabled: bool) -> dict:
    if local_enabled or ftp_enabled:
        return {
            "server": bool(local_enabled),
            "ftp": bool(ftp_enabled),
        }
    return {"server": True, "ftp": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Revo360 bot")
    parser.add_argument("--test-ftp", action="store_true", help="Testa a conexao FTP e encerra")
    parser.add_argument(
        "--run-anytime",
        metavar="DD-MM-YYYY",
        type=_parse_manual_cycle_date,
        help="Executa ciclo manual na data informada (DD-MM-YYYY) ignorando agenda; sem --local/--ftp envia para ambos.",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="No modo manual, habilita envio para servidor local.",
    )
    parser.add_argument(
        "--ftp",
        action="store_true",
        help="No modo manual, habilita envio para FTP.",
    )
    parser.add_argument(
        "--source",
        action="append",
        metavar="SOURCE_ID",
        help="Executa apenas os sources informados (pode repetir a flag).",
    )
    parser.add_argument(
        "--force-run",
        action="store_true",
        help="Executa 1 ciclo imediatamente (sem daemon, sem agenda e sem idempotencia diaria)",
    )
    parser.add_argument("--test-email", action="store_true", help="Testa o envio de e-mail e encerra")
    parser.add_argument("--test-whatsapp", action="store_true", help="Testa o envio WhatsApp via WAHA e encerra")
    daemon_group = parser.add_mutually_exclusive_group()
    daemon_group.add_argument("--daemon", action="store_true", help="Forca modo daemon continuo")
    daemon_group.add_argument("--once", action="store_true", help="Forca execucao unica (modo job)")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if (args.local or args.ftp) and args.run_anytime is None:
        parser.error("As flags --local/--ftp exigem --run-anytime DD-MM-YYYY.")
    return args


def _parse_non_negative_int(value, default: int, logger: logging.Logger, setting_name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        logger.warning("%s invalido (%s). Usando %s.", setting_name, value, default)
        return default
    if parsed < 0:
        logger.warning("%s negativo (%s). Usando %s.", setting_name, value, default)
        return default
    return parsed


def _resolve_schedule(logger: logging.Logger) -> dict | None:
    start_date = None
    if RUN_START_DATE:
        try:
            start_date = date.fromisoformat(str(RUN_START_DATE))
        except Exception:
            logger.error("RUN_START_DATE invalido: %s (use YYYY-MM-DD)", RUN_START_DATE)
            return None

    allowed_days = None
    if RUN_DAYS:
        try:
            allowed_days = {int(dia) for dia in RUN_DAYS}
        except (TypeError, ValueError):
            logger.error("RUN_DAYS invalido: %s (use inteiros 0-6)", RUN_DAYS)
            return None
        if any(dia < 0 or dia > 6 for dia in allowed_days):
            logger.error("RUN_DAYS invalido: %s (use inteiros 0-6)", RUN_DAYS)
            return None

    run_time = None
    if RUN_TIME:
        try:
            hora, minuto = RUN_TIME.split(":")
            run_time = dt_time(hour=int(hora), minute=int(minuto))
        except Exception:
            logger.error("RUN_TIME invalido: %s (use HH:MM)", RUN_TIME)
            return None

    return {
        "start_date": start_date,
        "allowed_days": allowed_days,
        "run_time": run_time,
    }


def _is_day_allowed(target_date: date, schedule: dict) -> bool:
    start_date = schedule.get("start_date")
    allowed_days = schedule.get("allowed_days")
    if start_date and target_date < start_date:
        return False
    if allowed_days is not None and target_date.weekday() not in allowed_days:
        return False
    return True


def _is_inside_window(now: datetime, schedule: dict) -> tuple[bool, datetime | None, datetime | None]:
    if not _is_day_allowed(now.date(), schedule):
        return False, None, None

    run_time = schedule.get("run_time")
    if run_time is None:
        return (
            True,
            datetime.combine(now.date(), dt_time.min),
            datetime.combine(now.date(), dt_time.max),
        )

    window_start = datetime.combine(now.date(), run_time)
    window_end = window_start.replace(second=59, microsecond=999999)
    return window_start <= now <= window_end, window_start, window_end


def _next_window_start(
    now: datetime,
    schedule: dict,
    *,
    skip_today: bool = False,
) -> datetime | None:
    run_time = schedule.get("run_time")
    for offset in range(0, 370):
        candidate_date = now.date() + timedelta(days=offset)
        if skip_today and offset == 0:
            continue
        if not _is_day_allowed(candidate_date, schedule):
            continue
        if run_time is None:
            if offset == 0:
                return now + timedelta(seconds=1)
            return datetime.combine(candidate_date, dt_time.min)
        candidate = datetime.combine(candidate_date, run_time)
        if candidate <= now:
            continue
        return candidate
    return None


def _seconds_until(target: datetime, now: datetime | None = None) -> int:
    reference = now or datetime.now()
    return max(1, int((target - reference).total_seconds()))


def _run_once(
    logger: logging.Logger,
    *,
    ignore_schedule: bool = False,
    force_run: bool = False,
    cycle_date: date | None = None,
    targets: dict | None = None,
    requested_sources: list[str] | None = None,
) -> int:
    cycle_date = cycle_date or date.today()
    with instance_lock(logger) as acquired:
        if not acquired:
            return 0

        if not force_run and cycle_date == date.today() and _has_success_for_today(logger):
            logger.info(
                "Ciclo de hoje (%s) ja concluido com sucesso. Use --force-run para reexecutar.",
                date.today().isoformat(),
            )
            return 0

        if ignore_schedule:
            logger.warning("Execucao forcada: ignorando o agendamento configurado.")
            return run_with_retries(
                logger,
                force_run=force_run,
                cycle_date=cycle_date,
                targets=targets,
                requested_sources=requested_sources,
            )

        if not validar_agendamento(logger):
            try:
                raise OutsideExecutionWindowError(
                    "Execucao encerrada sem rodar o fluxo (fora da janela configurada)."
                )
            except OutsideExecutionWindowError as exc:
                logger.info("%s", exc)
                return 0

        return run_with_retries(
            logger,
            force_run=force_run,
            cycle_date=cycle_date,
            targets=targets,
            requested_sources=requested_sources,
        )


def run_daemon(logger: logging.Logger) -> None:
    poll_seconds = max(1, _parse_non_negative_int(DAEMON_POLL_SECONDS, 30, logger, "DAEMON_POLL_SECONDS"))
    after_run_sleep_seconds = max(
        1,
        _parse_non_negative_int(DAEMON_AFTER_RUN_SLEEP_SECONDS, 60, logger, "DAEMON_AFTER_RUN_SLEEP_SECONDS"),
    )
    with instance_lock(logger) as acquired:
        if not acquired:
            return

        last_run_date: date | None = None
        logger.info(
            "Modo daemon iniciado (poll=%ss, after_run_sleep=%ss).",
            poll_seconds,
            after_run_sleep_seconds,
        )

        while True:
            now = datetime.now()
            schedule = _resolve_schedule(logger) if ENFORCE_SCHEDULE else None

            if ENFORCE_SCHEDULE and schedule is None:
                sleep(poll_seconds)
                continue

            if ENFORCE_SCHEDULE and schedule is not None:
                inside_window, window_start, window_end = _is_inside_window(now, schedule)
            else:
                inside_window = True
                window_start = datetime.combine(now.date(), dt_time.min)
                window_end = datetime.combine(now.date(), dt_time.max)

            if inside_window and last_run_date != now.date():
                run_date = now.date()
                if _has_success_for_today(logger):
                    logger.info(
                        "Ciclo de hoje (%s) ja concluido com sucesso. Pulando execucao no daemon.",
                        run_date.isoformat(),
                    )
                    status = 0
                else:
                    logger.info(
                        "Dentro da janela %s -> %s. Executando ciclo do dia %s.",
                        window_start.strftime("%Y-%m-%d %H:%M:%S"),
                        window_end.strftime("%Y-%m-%d %H:%M:%S"),
                        run_date.isoformat(),
                    )
                    status = run_with_retries(logger, force_run=False)
                last_run_date = run_date

                if ENFORCE_SCHEDULE and schedule is not None:
                    now_after_run = datetime.now()
                    next_start = _next_window_start(
                        now_after_run,
                        schedule,
                        skip_today=now_after_run.date() == run_date,
                    )
                else:
                    next_start = datetime.combine(run_date + timedelta(days=1), dt_time.min)

                if next_start is None:
                    sleep_seconds = max(after_run_sleep_seconds, poll_seconds)
                    logger.info(
                        "Ciclo do dia concluido (status=%s). Proxima janela nao calculada. Dormindo %s segundos.",
                        status,
                        sleep_seconds,
                    )
                else:
                    sleep_seconds = max(after_run_sleep_seconds, _seconds_until(next_start))
                    logger.info(
                        "Ciclo do dia concluido (status=%s). Proxima janela em %s. Dormindo %s segundos.",
                        status,
                        next_start.strftime("%Y-%m-%d %H:%M:%S"),
                        sleep_seconds,
                    )
                sleep(sleep_seconds)
                continue

            if ENFORCE_SCHEDULE and schedule is not None:
                next_start = _next_window_start(now, schedule, skip_today=last_run_date == now.date())
                if next_start is None:
                    sleep_seconds = poll_seconds
                    logger.info(
                        "Aguardando proxima janela. Nao foi possivel calcular o horario exato. Dormindo %s segundos.",
                        sleep_seconds,
                    )
                else:
                    sleep_seconds = _seconds_until(next_start, now)
                    logger.info(
                        "Fora da janela (%s -> %s). Proxima janela em %s. Dormindo %s segundos.",
                        window_start.strftime("%Y-%m-%d %H:%M:%S") if window_start else "N/A",
                        window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else "N/A",
                        next_start.strftime("%Y-%m-%d %H:%M:%S"),
                        sleep_seconds,
                    )
            else:
                next_day = datetime.combine(now.date() + timedelta(days=1), dt_time.min)
                sleep_seconds = max(after_run_sleep_seconds, _seconds_until(next_day, now))
                logger.info(
                    "Agendamento desabilitado e ciclo do dia ja executado. Dormindo %s segundos ate o proximo dia.",
                    sleep_seconds,
                )

            sleep(sleep_seconds)


def main(
    args: argparse.Namespace | None = None,
    logger: logging.Logger | None = None,
    *,
    configure_logging: bool = True,
) -> int:
    if configure_logging:
        configurar_log()
    if logger is None:
        logger = logging.getLogger(__name__)
    if args is None:
        args = parse_args()

    if args.test_ftp:
        ok_ftp = testar_conexao_ftp()
        if ok_ftp:
            logger.info("Teste de FTP OK")
            return 0
        logger.error("Teste de FTP falhou")
        return 1

    if args.test_email:
        execution_summary = {
            "status": "SUCESSO",
            "start_time": datetime.now(),
            "end_time": datetime.now(),
            "filename": "TESTE_EMAIL.csv",
            "steps_executed": [
                "Download: OK",
                "Prepare: OK",
                "Envio Servidor: NAO SOLICITADO",
                "Envio FTP: NAO SOLICITADO",
            ],
            "error_message": None,
        }
        send_execution_email(execution_summary)
        logger.info("Teste de e-mail disparado.")
        return 0

    if args.test_whatsapp:
        resultado = send_whatsapp_messages("Teste WhatsApp WAHA - Revo360 Bot", logger)
        logger.info(
            "Teste WhatsApp WAHA: total=%s sent=%s failed=%s",
            resultado.get("total", 0),
            resultado.get("sent", 0),
            resultado.get("failed", 0),
        )
        for falha in resultado.get("failures", []):
            chat_id = falha.get("chat_id", "N/A")
            erro = falha.get("error", "N/A")
            logger.error("Falha WhatsApp chat_id=%s erro=%s", chat_id, erro)
        return 2 if resultado.get("failed", 0) else 0

    if args.run_anytime:
        manual_targets = _resolve_manual_targets(bool(args.local), bool(args.ftp))
        logger.warning(
            "Execucao manual solicitada para ciclo %s (server=%s ftp=%s).",
            args.run_anytime.isoformat(),
            manual_targets["server"],
            manual_targets["ftp"],
        )
        return _run_once(
            logger,
            ignore_schedule=True,
            force_run=bool(args.force_run),
            cycle_date=args.run_anytime,
            targets=manual_targets,
            requested_sources=args.source,
        )

    if args.force_run:
        return _run_once(
            logger,
            ignore_schedule=True,
            force_run=True,
            requested_sources=args.source,
        )

    return _run_once(
        logger,
        ignore_schedule=False,
        force_run=False,
        requested_sources=args.source,
    )

def _stage_required_in_resumo(stage: str, resumo: dict) -> bool:
    if stage == "send_server":
        return bool(resumo.get("server_requested", False))
    if stage == "send_ftp":
        return bool(resumo.get("ftp_requested", False))
    return True


def build_notification_header(policy: str) -> str:
    if policy == "initial":
        return "[INICIAL] Atualizacao da tentativa inicial"
    if policy == "final_recovered":
        return "[FINAL] Recuperado apos retry"
    if policy == "final_failure":
        return "[FINAL] Falha final apos retries"
    return "[INFO] Atualizacao de execucao"


def format_minutes(seconds: int) -> str:
    if seconds <= 0:
        return "0"
    return str((seconds + 59) // 60)


def _status_emoji(success: bool) -> str:
    return "✅" if success else "❌"


def _channel_status_line(*, label: str, requested: bool, sent: bool) -> str:
    if not requested:
        return f"{label}: ⏭️ SKIPPED"
    if sent:
        return f"{label}: ✅ OK"
    return f"{label}: ❌ FALHOU"


def _whatsapp_status_line(resumo: dict) -> str:
    requested = bool(resumo.get("whatsapp_requested", False))
    total = int(resumo.get("whatsapp_total", 0) or 0)
    sent = int(resumo.get("whatsapp_sent", 0) or 0)
    failed = int(resumo.get("whatsapp_failed", 0) or 0)

    if not requested and total == 0 and sent == 0 and failed == 0:
        return "WhatsApp: 🔄 envio em andamento"
    if not requested:
        return "WhatsApp: ⏭️ SKIPPED"
    if failed == 0:
        return f"WhatsApp: ✅ OK ({sent}/{total} enviados)"
    if sent > 0:
        return f"WhatsApp: ⚠️ PARCIAL ({sent}/{total} enviados)"
    return f"WhatsApp: ❌ FALHOU (0/{total} enviados)"


def _is_loop_notification_policy(policy: str) -> bool:
    return policy in {"final_recovered", "final_failure"}


def build_notification_text_normal(resumo: dict) -> str:
    success = bool(resumo.get("success", False))
    attempt = int(resumo.get("attempt", 1) or 1)
    attempts_total = int(resumo.get("attempts_total", 1) or 1)
    will_retry = bool(resumo.get("will_retry", False))
    next_retry_in_seconds = int(resumo.get("next_retry_in_seconds") or 0)
    notification_policy = resumo.get("notification_policy") or "none"
    source_id = resumo.get("source_id") or "N/A"
    expected_name = resumo.get("expected_name") or "N/A"
    original = resumo.get("original_name") or "N/A"
    resolved_name = resumo.get("resolved_name") or original
    final = resumo.get("final_name") or "N/A"
    final_result = resumo.get("final_result")

    lines = [
        build_notification_header(notification_policy),
        f"Source: {source_id}",
        f"Status atual: {_status_emoji(success)} {'SUCESSO' if success else 'FALHA'}",
        f"Tentativa: {attempt}/{attempts_total}",
        f"Arquivo esperado: {expected_name}",
        f"Arquivo remoto resolvido: {resolved_name}",
        f"Arquivo baixado: {original}",
        f"Arquivo preparado: {final}",
    ]
    if will_retry:
        lines.append(f"🔁 Proxima tentativa em: {format_minutes(next_retry_in_seconds)} min")
    if final_result:
        lines.append(f"Resultado: {final_result}")

    lines.extend(
        [
            "",
            "Status por canal:",
            _channel_status_line(
                label="Servidor",
                requested=bool(resumo.get("server_requested", False)),
                sent=bool(resumo.get("server_sent", False)),
            ),
            _channel_status_line(
                label="FTP",
                requested=bool(resumo.get("ftp_requested", False)),
                sent=bool(resumo.get("ftp_sent", False)),
            ),
            _whatsapp_status_line(resumo),
        ]
    )

    if not success and resumo.get("error"):
        lines.extend(["", f"Motivo: {resumo['error']}"])

    return "\n".join(lines)


def build_notification_text_loop(resumo: dict) -> str:
    success = bool(resumo.get("success", False))
    attempt = int(resumo.get("attempt", 1) or 1)
    attempts_total = int(resumo.get("attempts_total", 1) or 1)
    will_retry = bool(resumo.get("will_retry", False))
    next_retry_in_seconds = int(resumo.get("next_retry_in_seconds") or 0)
    notification_policy = resumo.get("notification_policy") or "none"
    source_id = resumo.get("source_id") or "N/A"
    expected_name = resumo.get("expected_name") or "N/A"
    resolved_name = resumo.get("resolved_name") or "N/A"
    original = resumo.get("original_name") or "N/A"
    final = resumo.get("final_name") or "N/A"
    final_result = resumo.get("final_result")
    entered_retry = bool(resumo.get("entered_retry", False))
    attempt_final = int(resumo.get("attempt_final", attempt) or attempt)
    stages = resumo.get("stages", {})

    lines = [
        build_notification_header(notification_policy),
        f"Source: {source_id}",
        f"Tentativa: {attempt}/{attempts_total}",
        f"Status atual: {'SUCESSO' if success else 'FALHA'}",
    ]
    if will_retry:
        lines.append(f"Proxima tentativa em: {format_minutes(next_retry_in_seconds)} min")
    if final_result:
        lines.append(f"Resultado final: {final_result}")

    lines.extend(
        [
            "",
            f"Arquivo esperado: {expected_name}",
            f"Arquivo remoto resolvido: {resolved_name}",
            f"Arquivo baixado: {original}",
            f"Arquivo preparado: {final}",
            "",
            "Relatorio por etapa:",
        ]
    )

    for stage in STAGE_ORDER:
        stage_state = stages.get(stage, {})
        tries = int(stage_state.get("tries", 0) or 0)
        stage_status = (
            "SKIPPED"
            if not _stage_required_in_resumo(stage, resumo)
            else ("OK" if bool(stage_state.get("ok")) else "FALHOU")
        )
        line = f"{STAGE_LABELS[stage]}: {stage_status} (tentativas: {tries})"
        if stage_status == "FALHOU" and stage_state.get("last_error"):
            line += f" ultimo_erro: {stage_state['last_error']}"
        lines.append(line)

    if notification_policy in {"final_recovered", "final_failure"}:
        lines.extend(
            [
                "",
                f"attempts_total: {attempts_total}",
                f"attempt_final: {attempt_final}",
                f"entered_retry: {entered_retry}",
            ]
        )

    if not success and resumo.get("error"):
        lines.extend(["", f"Motivo: {resumo['error']}"])

    return "\n".join(lines)


def build_notification_text(resumo: dict) -> str:
    notification_policy = resumo.get("notification_policy") or "none"
    if _is_loop_notification_policy(notification_policy):
        return build_notification_text_loop(resumo)
    return build_notification_text_normal(resumo)


def emitir_notificacoes(resumo: dict, execution_summary: dict, logger: logging.Logger) -> None:
    dispatch_source_notifications(
        resumo=resumo,
        execution_summary=execution_summary,
        logger=logger,
        build_notification_text=build_notification_text,
        build_notification_header=build_notification_header,
        send_whatsapp_messages=send_whatsapp_messages,
        send_google_chat=enviar_resumo_google_chat,
        send_execution_email=send_execution_email,
    )


def enviar_resumo_google_chat(texto: str, logger: logging.Logger) -> None:
    if not texto:
        return
    if not GOOGLE_CHAT_WEBHOOK_URL:
        logger.info("Webhook do Google Chat nao configurado. Pulando notificacao.")
        return
    enviar_notificacao_google_chat(texto, logger)


def montar_texto_resumo(resumo: dict) -> str:
    return build_notification_text(resumo)


def enviar_notificacao_google_chat(texto: str, logger: logging.Logger) -> None:
    payload = {"text": texto}
    try:
        response = requests.post(
            GOOGLE_CHAT_WEBHOOK_URL,
            json=payload,
            timeout=GOOGLE_CHAT_TIMEOUT,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        response.raise_for_status()
        logger.info("Resumo enviado ao Google Chat.")
    except requests.RequestException as exc:
        logger.exception("Falha ao enviar notificacao ao Google Chat: %s", exc)


if __name__ == "__main__":
    try:
        configurar_log()
        logger = logging.getLogger(__name__)
        args = parse_args()

        one_shot_requested = any(
            [
                args.force_run,
                args.run_anytime,
                args.test_ftp,
                args.test_email,
                args.test_whatsapp,
            ]
        )

        daemon_enabled = bool(DAEMON_MODE_ENABLED)
        if args.daemon:
            daemon_enabled = True
        if args.once:
            daemon_enabled = False

        if daemon_enabled and not one_shot_requested:
            run_daemon(logger)
            raise SystemExit(0)

        raise SystemExit(main(args=args, logger=logger, configure_logging=False))
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Execucao interrompida pelo usuario (Ctrl+C).")
        print("Execucao interrompida pelo usuario (Ctrl+C).")
        raise SystemExit(130)
