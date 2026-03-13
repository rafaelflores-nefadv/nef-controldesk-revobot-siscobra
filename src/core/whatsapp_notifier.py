import logging

import requests

from config.settings import (
    ENABLE_WHATSAPP_NOTIFICATION,
    WAHA_API_KEY,
    WAHA_BASE_URL,
    WAHA_CHAT_IDS,
    WAHA_FAIL_FAST,
    WAHA_SESSION,
    WAHA_TIMEOUT,
)


def _normalizar_chat_ids() -> list[str]:
    return [str(chat_id).strip() for chat_id in WAHA_CHAT_IDS if str(chat_id).strip()]


def send_whatsapp_messages(text: str, logger: logging.Logger) -> dict:
    chat_ids = _normalizar_chat_ids()
    resultado = {
        "requested": False,
        "total": 0,
        "sent": 0,
        "failed": 0,
        "failures": [],
    }

    if not ENABLE_WHATSAPP_NOTIFICATION:
        logger.info("Notificacao WhatsApp desabilitada em ENABLE_WHATSAPP_NOTIFICATION.")
        return resultado
    if not chat_ids:
        logger.info("WAHA_CHAT_IDS vazio. Nenhuma notificacao WhatsApp sera enviada.")
        return resultado

    resultado["requested"] = True
    resultado["total"] = len(chat_ids)
    base_url = WAHA_BASE_URL.rstrip("/")
    endpoint = f"{base_url}/api/sendText"
    headers = {}
    api_key = WAHA_API_KEY.strip()
    if api_key:
        headers["X-Api-Key"] = api_key

    logger.info(
        "Enviando notificacao WhatsApp via WAHA (base_url=%s, session=%s, destinatarios=%s).",
        base_url,
        WAHA_SESSION,
        len(chat_ids),
    )

    for chat_id in chat_ids:
        payload = {
            "session": WAHA_SESSION,
            "chatId": chat_id,
            "text": text,
        }
        try:
            response = requests.post(
                endpoint,
                json=payload,
                headers=headers or None,
                timeout=WAHA_TIMEOUT,
            )
            response.raise_for_status()
            resultado["sent"] += 1
        except requests.RequestException as exc:
            erro = str(exc)
            resultado["failed"] += 1
            resultado["failures"].append({"chat_id": chat_id, "error": erro})
            logger.error("Falha ao enviar WhatsApp para %s: %s", chat_id, erro)
            if WAHA_FAIL_FAST:
                break

    return resultado
