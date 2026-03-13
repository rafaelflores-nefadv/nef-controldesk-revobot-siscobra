from __future__ import annotations

from datetime import date, datetime
from time import sleep
from typing import Callable


def execute_stage_flow(
    *,
    logger,
    state: dict,
    cycle_date: date,
    attempt: int,
    attempts_total: int,
    retry_enabled: bool,
    retry_delay_seconds: int,
    stage_order: tuple[str, ...],
    is_stage_required: Callable[[str, dict], bool],
    stage_dependencies_ok: Callable[[str, dict], bool],
    run_single_stage: Callable[[str, object, dict, date], None],
    save_state: Callable[[], None],
    ensure_item_runtime_shape: Callable[[dict], None],
    build_resumo_from_state: Callable[..., dict],
    build_execution_summary: Callable[..., dict],
    stage_error_summary: Callable[[dict], str | None],
) -> tuple[bool, dict, dict, dict]:
    ensure_item_runtime_shape(state)
    execution_start = datetime.now()

    for stage in stage_order:
        stage_state = state["stages"][stage]
        if not is_stage_required(stage, state):
            stage_state["enabled"] = False
            stage_state["ok"] = True
            stage_state["last_error"] = None
            logger.info("Etapa %s desabilitada pelos targets do ciclo. Marcando como SKIPPED.", stage)
            save_state()
            continue

        stage_state["enabled"] = True

        if stage_state.get("ok"):
            logger.info("Etapa %s ja concluida em tentativa anterior. Pulando.", stage)
            continue

        if not stage_dependencies_ok(stage, state):
            logger.info("Etapa %s bloqueada por dependencia pendente. Pulando nesta tentativa.", stage)
            continue

        stage_state["tries"] = int(stage_state.get("tries", 0) or 0) + 1
        stage_state["last_attempt_ts"] = datetime.now().isoformat(timespec="seconds")
        logger.info("Executando etapa %s (try=%s).", stage, stage_state["tries"])
        try:
            run_single_stage(stage, logger, state, cycle_date)
            stage_state["ok"] = True
            stage_state["last_error"] = None
            logger.info("Etapa %s concluida com sucesso.", stage)
        except Exception as exc:
            stage_state["ok"] = False
            stage_state["last_error"] = str(exc)
            logger.error("Falha na etapa %s: %s", stage, exc, exc_info=True)
        finally:
            save_state()

    success = all(
        (not is_stage_required(stage, state)) or bool(state["stages"][stage].get("ok"))
        for stage in stage_order
    )
    will_retry = (not success) and retry_enabled and attempt < attempts_total
    if will_retry:
        state["entered_retry"] = True
        state["status"] = "RETRY"
        save_state()
    elif success:
        state["status"] = "SUCCESS"
        state["last_error"] = None
    else:
        state["status"] = "FAILED"
        state["last_error"] = stage_error_summary(state)

    if isinstance(state.get("timestamps"), dict):
        state["timestamps"]["updated_at"] = datetime.now().isoformat(timespec="seconds")

    resumo = build_resumo_from_state(
        state,
        attempt=attempt,
        attempts_total=attempts_total,
        success=success,
        will_retry=will_retry,
        retry_delay_seconds=retry_delay_seconds,
        policy="none",
    )
    execution_summary = build_execution_summary(
        state,
        attempt=attempt,
        attempts_total=attempts_total,
        success=success,
        execution_start=execution_start,
        will_retry=will_retry,
        retry_delay_seconds=retry_delay_seconds,
    )
    return success, resumo, execution_summary, state


def process_source_with_retries(
    *,
    source: dict,
    source_logger,
    cycle_state: dict,
    item_state: dict,
    cycle_date: date,
    total_attempts: int,
    retry_enabled: bool,
    retry_delay_seconds: int,
    save_state: Callable[[], None],
    ensure_item_runtime_shape: Callable[[dict], None],
    normalize_targets: Callable[[dict | None], dict],
    source_targets: Callable[[dict], dict],
    apply_non_requested_stage_defaults: Callable[[dict], None],
    invalidate_missing_checkpoint_paths: Callable[[dict, object], None],
    state_all_required_stages_ok: Callable[[dict], bool],
    execute_flow: Callable[..., tuple[bool, dict, dict, dict]],
    build_resumo_from_state: Callable[..., dict],
    build_execution_summary: Callable[..., dict],
    safe_emit_notifications: Callable[[dict, dict, object], None],
    stage_error_summary: Callable[[dict], str | None],
    sleep_func: Callable[[float], None] = sleep,
) -> bool:
    source_id = source["id"]
    source_logger.info("Iniciando pipeline do source=%s", source_id)

    ensure_item_runtime_shape(item_state)
    item_state["source"] = dict(source)
    item_state["targets"] = normalize_targets(item_state.get("targets"), fallback=source_targets(source))
    apply_non_requested_stage_defaults(item_state)
    invalidate_missing_checkpoint_paths(item_state, source_logger)
    save_state()

    if state_all_required_stages_ok(item_state):
        item_state["status"] = "SUCCESS"
        item_state["last_error"] = None
        save_state()
        source_logger.info("Source %s ja estava concluido. Pulando reprocessamento.", source_id)
        return True

    final_success = False
    final_attempt = 0
    for attempt in range(1, total_attempts + 1):
        final_attempt = attempt
        source_logger.info("Source %s - tentativa %s/%s", source_id, attempt, total_attempts)
        success, resumo, execution_summary, _ = execute_flow(
            source_logger,
            item_state,
            cycle_date,
            attempt=attempt,
            attempts_total=total_attempts,
            retry_enabled=retry_enabled,
            retry_delay_seconds=retry_delay_seconds,
            cycle_state=cycle_state,
        )

        if attempt == 1:
            resumo["notification_policy"] = "initial"
            resumo["final_result"] = "SUCESSO INICIAL" if success else "FALHA INICIAL"
            safe_emit_notifications(resumo, execution_summary, source_logger)
        else:
            source_logger.info(
                "Notificacao suprimida na tentativa intermediaria do source %s (%s/%s).",
                source_id,
                attempt,
                total_attempts,
            )

        if success:
            final_success = True
            source_logger.info("Source %s concluido com sucesso na tentativa %s.", source_id, attempt)
            break

        if retry_enabled and attempt < total_attempts:
            source_logger.warning(
                "Source %s falhou. Nova tentativa em %s segundos.",
                source_id,
                retry_delay_seconds,
            )
            sleep_func(retry_delay_seconds)
            continue

        source_logger.error("Source %s falhou definitivamente apos %s tentativa(s).", source_id, attempt)
        break

    if item_state.get("entered_retry"):
        final_policy = "final_recovered" if final_success else "final_failure"
        final_result = "RECUPERADO" if final_success else "FALHA FINAL"
        resumo_final = build_resumo_from_state(
            item_state,
            attempt=final_attempt,
            attempts_total=total_attempts,
            success=final_success,
            will_retry=False,
            retry_delay_seconds=retry_delay_seconds,
            policy=final_policy,
            final_result=final_result,
        )
        execution_summary_final = build_execution_summary(
            item_state,
            attempt=final_attempt,
            attempts_total=total_attempts,
            success=final_success,
            execution_start=datetime.now(),
            will_retry=False,
            retry_delay_seconds=retry_delay_seconds,
        )
        safe_emit_notifications(resumo_final, execution_summary_final, source_logger)

    item_state["status"] = "SUCCESS" if final_success else "FAILED"
    item_state["last_error"] = None if final_success else stage_error_summary(item_state)
    if isinstance(item_state.get("timestamps"), dict):
        item_state["timestamps"]["updated_at"] = datetime.now().isoformat(timespec="seconds")
    save_state()
    return final_success
