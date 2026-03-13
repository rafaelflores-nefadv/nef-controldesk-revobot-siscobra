import logging
import smtplib
from datetime import datetime
from email.message import EmailMessage

from config.settings import (
    EMAIL_FROM,
    EMAIL_PASSWORD,
    EMAIL_SMTP_HOST,
    EMAIL_SMTP_PORT,
    EMAIL_TIMEOUT,
    EMAIL_TO,
    EMAIL_USE_TLS,
    EMAIL_USER,
    ENABLE_EMAIL_NOTIFICATION,
)


def _format_datetime(value: datetime | None) -> str:
    if not value:
        return "N/A"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _format_steps(steps: list[str] | None) -> str:
    if not steps:
        return "N/A"
    return "\n".join(f"- {step}" for step in steps)


def send_execution_email(summary: dict) -> None:
    """
    Envia e-mail de notificacao da execucao do Revo360 Bot.
    Deve respeitar ENABLE_EMAIL_NOTIFICATION e EMAIL_TO.
    """
    logger = logging.getLogger(__name__)
    if not ENABLE_EMAIL_NOTIFICATION:
        return

    recipients = [email.strip() for email in EMAIL_TO if email and email.strip()]
    if not recipients:
        logger.info("EMAIL_TO vazio ou invalido. Pulando notificacao por e-mail.")
        return
    if not EMAIL_USER or not EMAIL_PASSWORD:
        logger.info("Credenciais de e-mail nao configuradas. Pulando notificacao.")
        return

    status = summary.get("status") or "N/A"
    start_time = _format_datetime(summary.get("start_time"))
    end_time = _format_datetime(summary.get("end_time"))
    filename = summary.get("filename") or "N/A"
    steps = _format_steps(summary.get("steps_executed"))
    error_message = summary.get("error_message") or ""

    notification_text = summary.get("notification_text")
    notification_header = summary.get("notification_header")

    subject = f"Revo360 Bot - {status}"
    if notification_header:
        subject = f"Revo360 Bot - {notification_header}"

    if notification_text:
        body = str(notification_text)
    else:
        body_lines = [
            "Notificacao de execucao - Revo360 Bot",
            "",
            f"Status: {status}",
            f"Inicio: {start_time}",
            f"Fim: {end_time}",
            f"Arquivo: {filename}",
            "",
            "Etapas:",
            steps,
        ]
        if status != "SUCESSO":
            body_lines.extend(["", f"Erro: {error_message or 'N/A'}"])
        body = "\n".join(body_lines)

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = EMAIL_FROM or EMAIL_USER
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, timeout=EMAIL_TIMEOUT) as smtp:
            if EMAIL_USE_TLS:
                smtp.starttls()
            smtp.login(EMAIL_USER, EMAIL_PASSWORD)
            smtp.send_message(message, to_addrs=recipients)
        logger.info("Notificacao por e-mail enviada.")
    except Exception as exc:
        logger.exception("Falha ao enviar notificacao por e-mail: %s", exc)
