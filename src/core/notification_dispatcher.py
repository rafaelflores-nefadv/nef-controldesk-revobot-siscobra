from __future__ import annotations


def dispatch_source_notifications(
    *,
    resumo: dict,
    execution_summary: dict,
    logger,
    build_notification_text,
    build_notification_header,
    send_whatsapp_messages,
    send_google_chat,
    send_execution_email,
) -> None:
    texto_whatsapp = build_notification_text(resumo)
    try:
        resultado_whatsapp = send_whatsapp_messages(texto_whatsapp, logger)
    except Exception as exc:
        logger.exception("Falha inesperada ao enviar notificacao WhatsApp: %s", exc)
        resultado_whatsapp = {
            "requested": False,
            "total": 0,
            "sent": 0,
            "failed": 0,
            "failures": [],
        }

    resumo["whatsapp_requested"] = resultado_whatsapp.get("requested", False)
    resumo["whatsapp_total"] = int(resultado_whatsapp.get("total", 0) or 0)
    resumo["whatsapp_sent"] = int(resultado_whatsapp.get("sent", 0) or 0)
    resumo["whatsapp_failed"] = int(resultado_whatsapp.get("failed", 0) or 0)
    resumo["whatsapp_failures"] = resultado_whatsapp.get("failures", [])

    texto_final = build_notification_text(resumo)
    execution_summary["notification_text"] = texto_final
    execution_summary["notification_header"] = build_notification_header(
        resumo.get("notification_policy") or "none"
    )

    try:
        send_google_chat(texto_final, logger)
    except Exception as exc:
        logger.exception("Falha inesperada ao enviar notificacao Google Chat: %s", exc)

    try:
        send_execution_email(execution_summary)
    except Exception as exc:
        logger.exception("Falha inesperada ao enviar notificacao por e-mail: %s", exc)
