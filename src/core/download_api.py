from __future__ import annotations

import base64
from binascii import Error as BinasciiError
from collections.abc import Mapping
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import re
from time import monotonic, sleep
from urllib.parse import unquote, urlsplit

import requests

from config.settings import (
    BASE_URL,
    FILE_MANAGER_API_BASE_URL,
    FILE_MANAGER_HTTP_RETRY_ATTEMPTS,
    FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS,
    FILE_MANAGER_HTTP_TIMEOUT_CONNECT,
    FILE_MANAGER_HTTP_TIMEOUT_READ,
)

logger = logging.getLogger(__name__)

FILE_MANAGER_ENDPOINT = "/api/file-manager-file-system"
FILE_MANAGER_ROOT = r"..\UPLOAD"
FILE_MANAGER_LIST_ACCEPT = "application/json, text/javascript, */*; q=0.01"
PREFERRED_API_PORT_MARKER = ":10024"
REQUEST_API_STORAGE_KEY = "requestApiRevolution"
REQUEST_TIMEOUT = (
    FILE_MANAGER_HTTP_TIMEOUT_CONNECT,
    FILE_MANAGER_HTTP_TIMEOUT_READ,
)
DATE_IN_FILENAME_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")
URL_IN_TEXT_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
LOGIN_MARKERS = (
    "<html",
    "<!doctype html",
    'name="login"',
    "name='login'",
    'name="password"',
    "name='password'",
    "/login",
    "style_form-ctn-login",
)
JSON_CONTENT_TYPES = {
    "application/json",
    "text/json",
    "application/problem+json",
}
HTML_CONTENT_TYPES = {
    "text/html",
    "application/xhtml+xml",
}
ALLOWED_DOWNLOAD_CONTENT_TYPES = {
    "application/octet-stream",
}
RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
MAX_LOG_PREVIEW_ITEMS = 5
SENSITIVE_HEADERS = {"cookie", "authorization", "proxy-authorization"}
BROWSER_CONTEXT_LOAD_TIMEOUT_SECONDS = 15.0
BROWSER_CONTEXT_LOAD_POLL_SECONDS = 0.25
BROWSER_AUTH_MARKERS = (
    "authentication required",
    "unauthorized",
    "login",
    "password",
    "senha",
    "usuario",
    "sign in",
)
BROWSER_LIST_FETCH_SCRIPT = """
const request = arguments[0];
const callback = arguments[arguments.length - 1];
(async () => {
  try {
    const url = new URL(request.endpoint, window.location.href);
    const params = request.params || {};
    Object.keys(params).forEach((key) => url.searchParams.set(key, params[key]));
    const response = await fetch(url.toString(), {
      method: request.method || "GET",
      credentials: "include",
      headers: request.headers || {},
    });
    const contentType = response.headers.get("content-type") || "";
    const text = await response.text();
    let payload = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch (error) {
        payload = null;
      }
    }
    callback({
      ok: true,
      status: response.status,
      url: response.url,
      contentType,
      contentLength: response.headers.get("content-length"),
      location: response.headers.get("location"),
      payload,
      textPreview: text.slice(0, 2000),
    });
  } catch (error) {
    callback({
      ok: false,
      error: String(error && error.message ? error.message : error),
    });
  }
})();
"""
BROWSER_DOWNLOAD_FETCH_SCRIPT = """
const request = arguments[0];
const callback = arguments[arguments.length - 1];
(async () => {
  try {
    const url = new URL(request.endpoint, window.location.href);
    const params = request.params || {};
    Object.keys(params).forEach((key) => url.searchParams.set(key, params[key]));

    const body = new URLSearchParams();
    const formData = request.formData || {};
    Object.keys(formData).forEach((key) => body.append(key, formData[key]));

    const response = await fetch(url.toString(), {
      method: request.method || "POST",
      credentials: "include",
      headers: request.headers || {},
      body: body.toString(),
    });

    const cloned = response.clone();
    let textPreview = "";
    try {
      textPreview = (await cloned.text()).slice(0, 2000);
    } catch (error) {
      textPreview = "";
    }

    const blob = await response.blob();
    let base64Payload = "";
    if (blob.size > 0) {
      base64Payload = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
          const result = typeof reader.result === "string" ? reader.result : "";
          const commaIndex = result.indexOf(",");
          resolve(commaIndex >= 0 ? result.slice(commaIndex + 1) : result);
        };
        reader.onerror = () => reject(new Error("Falha ao converter blob para base64."));
        reader.readAsDataURL(blob);
      });
    }

    callback({
      ok: true,
      status: response.status,
      url: response.url,
      contentType: response.headers.get("content-type") || "",
      contentLength: response.headers.get("content-length"),
      contentDisposition: response.headers.get("content-disposition"),
      location: response.headers.get("location"),
      size: blob.size,
      base64Payload,
      textPreview,
    });
  } catch (error) {
    callback({
      ok: false,
      error: String(error && error.message ? error.message : error),
    });
  }
})();
"""
BROWSER_DOWNLOAD_FORM_SUBMIT_SCRIPT = """
const actionUrl = arguments[0];
const fields = arguments[1] || {};
const root = document.body || document.documentElement;
if (!root) {
  return {
    submitted: false,
    error: "document.body ausente para submeter o formulario de download.",
  };
}

const form = document.createElement("form");
form.method = "POST";
form.action = actionUrl;
form.target = "_self";
form.acceptCharset = "UTF-8";
form.style.display = "none";

Object.entries(fields).forEach(([name, value]) => {
  const input = document.createElement("input");
  input.type = "hidden";
  input.name = name;
  input.value = value == null ? "" : String(value);
  form.appendChild(input);
});

root.appendChild(form);
form.submit();

if (form.parentNode) {
  form.parentNode.removeChild(form);
}

return {
  submitted: true,
  action: actionUrl,
  fieldNames: Object.keys(fields),
};
"""


class FileManagerApiError(RuntimeError):
    """Erro operacional da camada HTTP do File Manager."""


class SessionExpiredError(FileManagerApiError):
    """Resposta indica sessao expirada ou autenticacao nao propagada."""


class UnexpectedApiResponseError(FileManagerApiError):
    """Resposta da API veio em formato inesperado ou inconsistente."""


def _extrair_origem_http(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise RuntimeError(f"URL invalida para extrair origem HTTP: {url!r}")
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _resolver_origem_configurado() -> str | None:
    configurado = str(FILE_MANAGER_API_BASE_URL or "").strip()
    if not configurado:
        return None
    try:
        return _extrair_origem_http(configurado)
    except RuntimeError as exc:
        raise RuntimeError(
            f"Configuracao FILE_MANAGER_API_BASE_URL invalida: {configurado!r}"
        ) from exc


def _executar_script_seguro(driver, script: str) -> str | None:
    try:
        valor = driver.execute_script(script)
    except Exception:
        return None

    if isinstance(valor, str) and valor.strip() and valor.strip().lower() != "null":
        return valor.strip()
    return None


def _extrair_url_de_valor(valor) -> str | None:
    if valor is None:
        return None

    if isinstance(valor, str):
        texto = valor.strip().strip('"').strip("'")
        if not texto or texto.lower() == "null":
            return None
        if texto.startswith(("http://", "https://")):
            return texto.rstrip("/")
        try:
            return _extrair_url_de_valor(json.loads(texto))
        except Exception:
            match = URL_IN_TEXT_RE.search(texto)
            if match:
                return match.group(0).rstrip("/").rstrip("',\"}")
            return None

    if isinstance(valor, Mapping):
        for chave in (
            REQUEST_API_STORAGE_KEY,
            "baseURL",
            "baseUrl",
            "apiBaseUrl",
            "api_url",
            "url",
            "endpoint",
        ):
            if chave in valor:
                encontrada = _extrair_url_de_valor(valor.get(chave))
                if encontrada:
                    return encontrada
        for item in valor.values():
            encontrada = _extrair_url_de_valor(item)
            if encontrada:
                return encontrada
        return None

    if isinstance(valor, (list, tuple, set)):
        for item in valor:
            encontrada = _extrair_url_de_valor(item)
            if encontrada:
                return encontrada
        return None

    return None


def _capturar_contexto_janela(driver, handle) -> dict:
    page_url = (
        _executar_script_seguro(driver, "return window.location.href;")
        or getattr(driver, "current_url", "")
        or ""
    )
    window_origin = _executar_script_seguro(driver, "return window.location.origin;")
    storage_value = _executar_script_seguro(
        driver,
        f"return window.sessionStorage.getItem('{REQUEST_API_STORAGE_KEY}');",
    )
    storage_url = _extrair_url_de_valor(storage_value)

    logger.info(
        "Janela Selenium inspecionada: handle=%s url=%s origin=%s",
        handle,
        page_url or "<vazia>",
        window_origin or "<ausente>",
    )
    if storage_value:
        logger.info(
            "sessionStorage[%s] em handle=%s: %s",
            REQUEST_API_STORAGE_KEY,
            handle,
            storage_url or "<presente_sem_url>",
        )

    return {
        "handle": handle,
        "page_url": page_url,
        "window_origin": window_origin.rstrip("/") if isinstance(window_origin, str) else None,
        "storage_value": storage_value,
        "storage_url": storage_url,
    }


def _url_tem_origem(url: str | None, origem: str) -> bool:
    if not isinstance(url, str) or not url.strip():
        return False
    try:
        return _extrair_origem_http(url.strip()) == origem
    except RuntimeError:
        return False


def _contexto_original_ou_primeiro(contextos: list[dict], original_handle) -> dict | None:
    for contexto in contextos:
        if contexto.get("handle") == original_handle:
            return contexto
    return contextos[0] if contextos else None


def _encontrar_contexto_por_origem(contextos: list[dict], origem: str) -> dict | None:
    for contexto in contextos:
        if contexto.get("window_origin") == origem:
            return contexto
        if _url_tem_origem(contexto.get("page_url"), origem):
            return contexto
        if _url_tem_origem(contexto.get("storage_url"), origem):
            return contexto
    return None


def _resolver_origem_driver(driver) -> dict:
    original_handle = getattr(driver, "current_window_handle", None)
    origem_configurada = _resolver_origem_configurado()
    handles = list(getattr(driver, "window_handles", []) or [])
    if original_handle and original_handle not in handles:
        handles.insert(0, original_handle)
    if not handles and original_handle:
        handles = [original_handle]

    logger.info(
        "Descoberta do origin HTTP: %s window_handle(s) encontrados",
        len(handles),
    )

    contextos = []
    contexto_porta = None
    contexto_storage = None

    try:
        for handle in handles:
            if handle is not None and handle != getattr(driver, "current_window_handle", None):
                driver.switch_to.window(handle)
            contexto = _capturar_contexto_janela(driver, handle)
            contextos.append(contexto)

            url_para_porta = contexto["page_url"] or contexto["window_origin"] or ""
            if contexto_porta is None and PREFERRED_API_PORT_MARKER in str(url_para_porta):
                contexto_porta = contexto

            if contexto_storage is None and contexto.get("storage_url"):
                contexto_storage = contexto
    finally:
        if original_handle and getattr(driver, "current_window_handle", None) != original_handle:
            driver.switch_to.window(original_handle)

    contexto_escolhido = None
    origem = None
    page_url = None
    motivo = None

    if origem_configurada:
        contexto_escolhido = _encontrar_contexto_por_origem(contextos, origem_configurada)
        if contexto_escolhido is not None:
            logger.info(
                "FILE_MANAGER_API_BASE_URL configurada tem prioridade sobre autodiscovery: base_url=%s handle=%s",
                origem_configurada,
                contexto_escolhido.get("handle"),
            )
        else:
            contexto_escolhido = _contexto_original_ou_primeiro(contextos, original_handle)
            logger.info(
                "FILE_MANAGER_API_BASE_URL configurada tem prioridade sobre autodiscovery: base_url=%s sem handle correspondente; usando handle=%s",
                origem_configurada,
                contexto_escolhido.get("handle") if contexto_escolhido else original_handle,
            )
        origem = origem_configurada
        page_url = (
            contexto_escolhido.get("page_url") if contexto_escolhido else None
        ) or f"{origem}/"
        motivo = "config.FILE_MANAGER_API_BASE_URL"
    elif contexto_porta is not None:
        contexto_escolhido = contexto_porta
        origem = contexto_porta.get("window_origin")
        if not origem:
            origem = _extrair_origem_http(contexto_porta["page_url"])
        page_url = contexto_porta["page_url"] or f"{origem}/"
        motivo = "window_handle_10024"
    elif contexto_storage is not None:
        storage_url = contexto_storage["storage_url"]
        origem = _extrair_origem_http(storage_url)
        page_url = storage_url
        contexto_escolhido = contexto_storage
        logger.info(
            "FILE_MANAGER_API_BASE_URL nao configurada; usando autodiscovery via sessionStorage.%s",
            REQUEST_API_STORAGE_KEY,
        )
        motivo = f"sessionStorage.{REQUEST_API_STORAGE_KEY}"
    else:
        contexto_fallback = _contexto_original_ou_primeiro(contextos, original_handle)

        if contexto_fallback is not None:
            contexto_escolhido = contexto_fallback
            page_url = contexto_fallback["page_url"] or BASE_URL
            origem = contexto_fallback.get("window_origin")
        else:
            page_url = getattr(driver, "current_url", "") or BASE_URL
            origem = None
        if not origem:
            try:
                origem = _extrair_origem_http(page_url)
            except RuntimeError:
                origem = _extrair_origem_http(BASE_URL)
        motivo = "fallback.window_location"

    origem = origem.rstrip("/")
    referer = f"{origem}/"
    endpoint = f"{origem}{FILE_MANAGER_ENDPOINT}"
    logger.info(
        "Origin HTTP escolhido: motivo=%s pagina_atual=%s base_url=%s endpoint=%s",
        motivo,
        page_url,
        origem,
        endpoint,
    )
    return {
        "page_url": page_url,
        "origin": origem,
        "referer": referer,
        "endpoint": endpoint,
        "selected_handle": contexto_escolhido.get("handle") if contexto_escolhido else original_handle,
        "original_handle": original_handle,
        "reason": motivo,
        "contexts": contextos,
    }


def _normalizar_pasta_remota(pasta: str) -> str:
    if pasta is None:
        raise ValueError("A pasta remota deve ser informada.")

    normalized = str(pasta).strip().replace("/", "\\")
    while "\\\\" in normalized:
        normalized = normalized.replace("\\\\", "\\")
    normalized = normalized.strip("\\")

    if not normalized or normalized == ".":
        return ""

    lower = normalized.lower()
    root_lower = FILE_MANAGER_ROOT.lower()
    if lower == root_lower:
        return ""
    if lower.startswith(f"{root_lower}\\"):
        return normalized[len(FILE_MANAGER_ROOT) + 1 :]
    if lower == "upload":
        return ""
    if lower.startswith("upload\\"):
        return normalized[len("UPLOAD\\") :]
    return normalized


def _construir_path_info(pasta_normalizada: str) -> list[dict]:
    if not pasta_normalizada:
        return []

    acumulado = []
    partes = [parte.strip() for parte in pasta_normalizada.split("\\") if parte.strip()]
    for parte in partes:
        if acumulado:
            key = f"{acumulado[-1]['key']}\\{parte}"
        else:
            key = parte
        acumulado.append({"key": key, "name": parte})
    return acumulado


def _resolver_endpoint(sessao: requests.Session) -> str:
    endpoint = _session_attr_str(sessao, "revo360_file_manager_endpoint")
    if endpoint:
        return endpoint

    origin = _session_attr_str(sessao, "revo360_origin")
    if origin:
        return f"{origin}{FILE_MANAGER_ENDPOINT}"

    referer = _coletar_headers_sessao(sessao).get("Referer") or BASE_URL
    return f"{_extrair_origem_http(referer)}{FILE_MANAGER_ENDPOINT}"


def _coletar_headers_sessao(sessao: requests.Session) -> dict[str, str]:
    headers = getattr(sessao, "headers", None)
    if isinstance(headers, Mapping):
        return dict(headers)
    return {}


def _session_attr_str(sessao: requests.Session, attr_name: str) -> str | None:
    valor = getattr(sessao, attr_name, None)
    if isinstance(valor, str) and valor:
        return valor
    return None


def _session_attr_bool(sessao: requests.Session, attr_name: str) -> bool:
    valor = getattr(sessao, attr_name, False)
    if isinstance(valor, bool):
        return valor
    return False


def _headers_sem_sensiveis(headers: Mapping | None) -> dict[str, str]:
    if not isinstance(headers, Mapping):
        return {}
    return {
        str(chave): str(valor)
        for chave, valor in headers.items()
        if str(chave).lower() not in SENSITIVE_HEADERS
    }


def _cookie_diagnostic_rows(cookies) -> list[dict[str, object]]:
    diagnostico = []
    for cookie in cookies or []:
        diagnostico.append(
            {
                "name": cookie.get("name"),
                "domain": cookie.get("domain") or "<host-only>",
                "path": cookie.get("path") or "/",
                "secure": bool(cookie.get("secure", False)),
            }
        )
    return diagnostico


def _session_cookie_names(sessao: requests.Session) -> list[str]:
    cookies = getattr(sessao, "cookies", None)
    try:
        return sorted(
            {
                str(cookie.name)
                for cookie in cookies
                if getattr(cookie, "name", None)
            }
        )
    except Exception:
        return []


def _resolver_base_url(sessao: requests.Session) -> str:
    base_url = _session_attr_str(sessao, "revo360_base_url")
    if base_url:
        return str(base_url).rstrip("/")

    origin = _session_attr_str(sessao, "revo360_origin")
    if origin:
        return origin.rstrip("/")

    referer = _coletar_headers_sessao(sessao).get("Referer") or BASE_URL
    return _extrair_origem_http(referer)


def _resolver_referer(sessao: requests.Session) -> str:
    referer = _session_attr_str(sessao, "revo360_referer")
    if referer:
        return referer
    return f"{_resolver_base_url(sessao)}/"


def _headers_listagem(sessao: requests.Session) -> dict[str, str]:
    headers = {
        "Accept": FILE_MANAGER_LIST_ACCEPT,
        "Origin": _session_attr_str(sessao, "revo360_origin") or _resolver_base_url(sessao),
        "Referer": _resolver_referer(sessao),
    }
    user_agent = _coletar_headers_sessao(sessao).get("User-Agent")
    if user_agent:
        headers["User-Agent"] = user_agent
    return headers


def _log_request_diagnostic(
    sessao: requests.Session,
    method: str,
    endpoint: str,
    *,
    params: dict | None = None,
    headers: Mapping | None = None,
    operation: str,
) -> None:
    merged_headers = _coletar_headers_sessao(sessao)
    if isinstance(headers, Mapping):
        merged_headers.update(dict(headers))
    prepared = requests.Request(
        method=method,
        url=endpoint,
        params=params,
        headers=merged_headers,
    ).prepare()
    logger.info(
        "%s: request method=%s url=%s params=%s headers=%s cookies_na_sessao=%s",
        operation,
        method,
        prepared.url,
        params,
        _headers_sem_sensiveis(merged_headers),
        _session_cookie_names(sessao),
    )


def _log_response_diagnostic(response, operation: str) -> None:
    headers = getattr(response, "headers", {}) or {}
    content_length = headers.get("Content-Length") or headers.get("content-length")
    location = headers.get("Location") or headers.get("location")
    content_type = _normalizar_content_type(response) or "<ausente>"
    preview_texto = _preview_text(getattr(response, "text", ""))

    logger.info(
        "%s: response status=%s url=%s content_type=%s content_length=%s location=%s",
        operation,
        getattr(response, "status_code", None),
        getattr(response, "url", ""),
        content_type,
        content_length or "<ausente>",
        location or "<ausente>",
    )

    if content_type in HTML_CONTENT_TYPES or _parece_html_ou_login(preview_texto):
        logger.warning(
            "%s: corpo HTML inesperado (primeiros 200 chars): %s",
            operation,
            preview_texto or "<vazio>",
        )


def _path_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        return (urlsplit(url).path or "").lower()
    except Exception:
        return str(url).lower()


def _preview_text(text: str | None, limit: int = 200) -> str:
    if text is None:
        return ""
    normalized = " ".join(str(text).split())
    return normalized[:limit]


def _preview_bytes(content: bytes | None, limit: int = 200) -> str:
    if not content:
        return ""
    return _preview_text(content[:limit].decode("utf-8", errors="ignore"), limit=limit)


def _parece_html_ou_login(texto: str) -> bool:
    lowered = _preview_text(texto, limit=400).lower()
    return any(marker in lowered for marker in LOGIN_MARKERS)


def _parece_json_textual(texto: str) -> bool:
    stripped = _preview_text(texto, limit=400).lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


def _normalizar_content_type(response) -> str:
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Content-Type") or headers.get("content-type") or ""
    return raw.split(";", 1)[0].strip().lower()


def _content_disposition(response) -> str:
    headers = getattr(response, "headers", {}) or {}
    return (headers.get("Content-Disposition") or headers.get("content-disposition") or "").strip()


def _extrair_nome_content_disposition(content_disposition: str) -> str | None:
    if not content_disposition:
        return None

    match = re.search(r"filename\*\s*=\s*UTF-8''([^;]+)", content_disposition, re.IGNORECASE)
    if match:
        return Path(unquote(match.group(1))).name

    match = re.search(r'filename\s*=\s*"([^"]+)"', content_disposition, re.IGNORECASE)
    if match:
        return Path(match.group(1)).name

    match = re.search(r"filename\s*=\s*([^;]+)", content_disposition, re.IGNORECASE)
    if match:
        return Path(match.group(1).strip().strip('"')).name

    return None


def _detalhe_indica_sessao_expirada(detail: str) -> bool:
    lowered = str(detail).lower()
    return any(token in lowered for token in ("login", "sess", "expir", "unauthor", "forbidden", "autentic"))


def _raise_for_status_with_context(response, operation: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = getattr(response, "status_code", None)
        if status in {401, 403}:
            raise SessionExpiredError(
                f"{operation}: HTTP {status}; possivel sessao expirada ou autenticacao nao propagada."
            ) from exc
        raise


def _validar_resposta_nao_autenticada(response, operation: str, preview_texto: str = "") -> None:
    final_path = _path_from_url(getattr(response, "url", ""))
    if "/login" in final_path:
        raise SessionExpiredError(
            f"{operation}: resposta direcionada para login; possivel sessao expirada ou autenticacao nao propagada."
        )

    if getattr(response, "history", None):
        if "/login" in final_path:
            raise SessionExpiredError(
                f"{operation}: resposta com redirecionamento implicito para login."
            )

    content_type = _normalizar_content_type(response)
    if content_type in HTML_CONTENT_TYPES:
        raise SessionExpiredError(
            f"{operation}: recebeu HTML em vez da resposta esperada; possivel sessao expirada ou autenticacao nao propagada."
        )

    if preview_texto and _parece_html_ou_login(preview_texto):
        raise SessionExpiredError(
            f"{operation}: recebeu pagina/login HTML em vez da resposta esperada."
        )


def _extrair_itens_listagem(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        raise UnexpectedApiResponseError("Resposta da API de arquivos em formato inesperado.")

    if payload.get("success") is False:
        detail = payload.get("errorText") or payload.get("message") or "erro desconhecido"
        if _detalhe_indica_sessao_expirada(detail):
            raise SessionExpiredError(
                f"API File Manager indicou falha de autenticacao/sessao: {detail}"
            )
        raise FileManagerApiError(f"API File Manager retornou erro: {detail}")

    if isinstance(payload.get("result"), list):
        return payload["result"]

    result = payload.get("result")
    if isinstance(result, dict):
        for key in ("items", "data", "files"):
            if isinstance(result.get(key), list):
                return result[key]

    for key in ("items", "data", "files"):
        if isinstance(payload.get(key), list):
            return payload[key]

    raise UnexpectedApiResponseError(
        "Nao foi possivel localizar os itens da listagem na resposta da API."
    )


def _normalizar_item_listagem(item: dict) -> dict:
    return {
        "name": item.get("name"),
        "key": item.get("key"),
        "dateModified": item.get("dateModified"),
        "isDirectory": bool(item.get("isDirectory", False)),
        "size": item.get("size"),
    }


def _selecionar_item_por_nome(itens: list[dict], nome_arquivo: str) -> dict:
    for item in itens:
        if item.get("name") == nome_arquivo:
            return item

    nome_lower = nome_arquivo.casefold()
    for item in itens:
        name = item.get("name")
        if isinstance(name, str) and name.casefold() == nome_lower:
            return item

    raise FileManagerApiError(f"Arquivo nao encontrado via API: {nome_arquivo}")


def _is_retryable_request_exception(exc: requests.RequestException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True

    if isinstance(exc, requests.HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        return status in RETRYABLE_STATUS_CODES

    return False


def _executar_com_retry(rotulo: str, operacao):
    tentativas = max(1, int(FILE_MANAGER_HTTP_RETRY_ATTEMPTS))
    backoff = max(0.0, float(FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS))

    for tentativa in range(1, tentativas + 1):
        logger.info("Executando %s (tentativa %s/%s)", rotulo, tentativa, tentativas)
        try:
            return operacao()
        except (SessionExpiredError, UnexpectedApiResponseError, FileManagerApiError):
            raise
        except requests.RequestException as exc:
            retryable = _is_retryable_request_exception(exc)
            if not retryable or tentativa >= tentativas:
                raise

            delay = backoff * (2 ** (tentativa - 1))
            logger.warning(
                "Falha transiente em %s na tentativa %s/%s: %s. Nova tentativa em %.1fs.",
                rotulo,
                tentativa,
                tentativas,
                exc,
                delay,
            )
            if delay > 0:
                sleep(delay)

    raise RuntimeError(f"Fluxo de retry esgotado inesperadamente para {rotulo}.")


def _parse_listing_response(response, pasta_normalizada: str) -> list[dict]:
    operation = f"listagem da pasta '{pasta_normalizada}'"
    _raise_for_status_with_context(response, operation)
    preview_texto = _preview_text(getattr(response, "text", ""))
    _validar_resposta_nao_autenticada(response, operation, preview_texto=preview_texto)

    try:
        payload = response.json()
    except ValueError as exc:
        if _parece_html_ou_login(preview_texto):
            raise SessionExpiredError(
                f"{operation}: recebeu HTML/login em vez de JSON."
            ) from exc
        raise UnexpectedApiResponseError(
            f"{operation}: resposta sem JSON valido."
        ) from exc

    try:
        return _extrair_itens_listagem(payload)
    except FileManagerApiError:
        raise
    except Exception as exc:
        if _parece_html_ou_login(preview_texto):
            raise SessionExpiredError(
                f"{operation}: resposta inesperada sugere sessao expirada."
            ) from exc
        raise UnexpectedApiResponseError(
            f"{operation}: estrutura de resposta invalida."
        ) from exc


def _validar_headers_download(response, nome_arquivo: str, operation: str) -> None:
    _raise_for_status_with_context(response, operation)
    preview_texto = _preview_text(getattr(response, "text", ""))
    _validar_resposta_nao_autenticada(response, operation, preview_texto=preview_texto)

    content_type = _normalizar_content_type(response)
    if not content_type:
        raise UnexpectedApiResponseError(
            f"{operation}: resposta sem Content-Type."
        )

    if content_type in JSON_CONTENT_TYPES:
        try:
            payload = response.json()
        except ValueError as exc:
            raise UnexpectedApiResponseError(
                f"{operation}: resposta JSON invalida em vez do binario esperado."
            ) from exc

        if isinstance(payload, dict):
            if payload.get("success") is True and payload.get("result") is None:
                raise UnexpectedApiResponseError(
                    f"{operation}: a API retornou JSON success=true/result=null em vez do arquivo binario. Verifique o payload de Download com pathInfoList."
                )
            detail = payload.get("errorText") or payload.get("message")
            if detail and _detalhe_indica_sessao_expirada(detail):
                raise SessionExpiredError(
                    f"{operation}: API indicou possivel sessao expirada ou autenticacao nao propagada: {detail}"
                )
        raise UnexpectedApiResponseError(
            f"{operation}: resposta JSON inesperada em vez do binario esperado."
        )

    if content_type not in ALLOWED_DOWNLOAD_CONTENT_TYPES:
        raise UnexpectedApiResponseError(
            f"{operation}: Content-Type inesperado '{content_type}'."
        )

    content_disposition = _content_disposition(response)
    if not content_disposition:
        raise UnexpectedApiResponseError(
            f"{operation}: resposta sem Content-Disposition."
        )
    if "attachment" not in content_disposition.lower():
        raise UnexpectedApiResponseError(
            f"{operation}: Content-Disposition nao indica attachment."
        )

    nome_recebido = _extrair_nome_content_disposition(content_disposition)
    if nome_recebido and Path(nome_recebido).name.casefold() != Path(nome_arquivo).name.casefold():
        raise UnexpectedApiResponseError(
            f"{operation}: nome retornado pelo servidor ({nome_recebido}) difere do esperado ({nome_arquivo})."
        )


def _validar_primeiro_chunk_download(
    response,
    nome_arquivo: str,
    primeiro_chunk: bytes,
) -> None:
    operation = f"download do arquivo '{nome_arquivo}'"
    if not primeiro_chunk:
        raise UnexpectedApiResponseError(
            f"{operation}: corpo vazio retornado pela API."
        )

    preview = _preview_bytes(primeiro_chunk)
    if _parece_html_ou_login(preview):
        raise SessionExpiredError(
            f"{operation}: recebeu HTML/login em vez de binario; possivel sessao expirada ou autenticacao nao propagada."
        )

    if _parece_json_textual(preview):
        try:
            payload = json.loads(preview)
        except ValueError:
            payload = None

        if isinstance(payload, dict) and payload.get("success") is True and payload.get("result") is None:
            raise UnexpectedApiResponseError(
                f"{operation}: a API retornou JSON success=true/result=null em vez do arquivo binario. Verifique o payload de Download com pathInfoList."
            )
        raise UnexpectedApiResponseError(
            f"{operation}: resposta retornou JSON/texto de erro em vez do binario esperado."
        )


def _normalizar_browser_content_type(result: dict) -> str:
    raw = str(result.get("contentType") or "").strip()
    return raw.split(";", 1)[0].strip().lower()


def _browser_text_preview(result: dict, *, limit: int = 200) -> str:
    return _preview_text(result.get("textPreview") or "", limit=limit)


def _browser_content_disposition(result: dict) -> str:
    return str(result.get("contentDisposition") or "").strip()


def _browser_status_code(result: dict) -> int | None:
    try:
        return int(result.get("status"))
    except (TypeError, ValueError):
        return None


def _log_browser_fetch_result(result: dict, operation: str) -> None:
    content_type = _normalizar_browser_content_type(result) or "<ausente>"
    logger.info(
        "%s no browser: status=%s url=%s content_type=%s content_length=%s location=%s",
        operation,
        result.get("status"),
        result.get("url") or "<ausente>",
        content_type,
        result.get("contentLength") or "<ausente>",
        result.get("location") or "<ausente>",
    )

    preview = _browser_text_preview(result)
    if content_type in HTML_CONTENT_TYPES or _parece_html_ou_login(preview):
        logger.warning(
            "%s no browser: corpo HTML inesperado (primeiros 200 chars): %s",
            operation,
            preview or "<vazio>",
        )


def _raise_browser_fetch_error(result: object, operation: str) -> dict:
    if not isinstance(result, dict):
        raise UnexpectedApiResponseError(
            f"{operation}: retorno invalido do JavaScript no navegador."
        )

    if not result.get("ok", False):
        raise FileManagerApiError(
            f"{operation}: falha ao executar fetch no navegador: {result.get('error') or 'erro desconhecido'}"
        )

    return result


def _raise_for_browser_status(result: dict, operation: str) -> None:
    status = _browser_status_code(result)
    if status is None:
        raise UnexpectedApiResponseError(
            f"{operation}: retorno do navegador sem status HTTP valido."
        )
    if status in {401, 403}:
        raise SessionExpiredError(
            f"{operation}: HTTP {status} no fetch do navegador; possivel sessao expirada."
        )
    if status >= 400:
        raise FileManagerApiError(
            f"{operation}: fetch do navegador retornou HTTP {status}."
        )


def _validar_browser_response_nao_autenticada(result: dict, operation: str) -> None:
    final_path = _path_from_url(result.get("url") or "")
    preview = _browser_text_preview(result)
    content_type = _normalizar_browser_content_type(result)

    if "/login" in final_path:
        raise SessionExpiredError(
            f"{operation}: o fetch do navegador foi direcionado para login."
        )
    if content_type in HTML_CONTENT_TYPES:
        raise SessionExpiredError(
            f"{operation}: o navegador recebeu HTML em vez da resposta esperada."
        )
    if preview and _parece_html_ou_login(preview):
        raise SessionExpiredError(
            f"{operation}: o navegador recebeu pagina/login HTML em vez da resposta esperada."
        )


def _parse_listing_result_no_browser(result: object, pasta_label: str) -> list[dict]:
    operation = f"listagem da pasta '{pasta_label}'"
    result_dict = _raise_browser_fetch_error(result, operation)
    _log_browser_fetch_result(result_dict, operation)
    _raise_for_browser_status(result_dict, operation)
    _validar_browser_response_nao_autenticada(result_dict, operation)

    payload = result_dict.get("payload")
    if payload is None:
        raise UnexpectedApiResponseError(
            f"{operation}: fetch do navegador nao retornou JSON valido."
        )

    return _extrair_itens_listagem(payload)


def _validar_download_result_no_browser(result: object, nome_arquivo: str) -> dict:
    operation = f"download do arquivo '{nome_arquivo}'"
    result_dict = _raise_browser_fetch_error(result, operation)
    _log_browser_fetch_result(result_dict, operation)
    _raise_for_browser_status(result_dict, operation)
    _validar_browser_response_nao_autenticada(result_dict, operation)

    content_type = _normalizar_browser_content_type(result_dict)
    if content_type in JSON_CONTENT_TYPES or _parece_json_textual(_browser_text_preview(result_dict, limit=400)):
        preview = _browser_text_preview(result_dict, limit=400)
        try:
            payload = json.loads(preview) if preview else None
        except ValueError:
            payload = None
        if isinstance(payload, dict) and payload.get("success") is True and payload.get("result") is None:
            raise UnexpectedApiResponseError(
                f"{operation}: a API retornou JSON success=true/result=null em vez do arquivo binario. Verifique o payload de Download com pathInfoList."
            )
        raise UnexpectedApiResponseError(
            f"{operation}: fetch do navegador retornou JSON/texto em vez do binario esperado."
        )

    if content_type not in ALLOWED_DOWNLOAD_CONTENT_TYPES:
        raise UnexpectedApiResponseError(
            f"{operation}: Content-Type inesperado '{content_type or '<ausente>'}'."
        )

    content_disposition = _browser_content_disposition(result_dict)
    if not content_disposition:
        raise UnexpectedApiResponseError(
            f"{operation}: resposta sem Content-Disposition."
        )
    if "attachment" not in content_disposition.lower():
        raise UnexpectedApiResponseError(
            f"{operation}: Content-Disposition nao indica attachment."
        )

    nome_recebido = _extrair_nome_content_disposition(content_disposition)
    if nome_recebido and Path(nome_recebido).name.casefold() != Path(nome_arquivo).name.casefold():
        raise UnexpectedApiResponseError(
            f"{operation}: nome retornado pelo servidor ({nome_recebido}) difere do esperado ({nome_arquivo})."
        )

    tamanho = int(result_dict.get("size") or 0)
    if tamanho <= 0:
        raise UnexpectedApiResponseError(
            f"{operation}: o blob retornado pelo navegador veio vazio."
        )
    if not result_dict.get("base64Payload"):
        raise UnexpectedApiResponseError(
            f"{operation}: o navegador nao retornou base64 para o arquivo baixado."
        )
    return result_dict


def _salvar_base64_em_arquivo_temporario(base64_payload: str, destino, nome_arquivo: str) -> Path:
    operation = f"download do arquivo '{nome_arquivo}'"
    destino_path = Path(destino)
    if destino_path.exists() and destino_path.is_dir():
        destino_path = destino_path / nome_arquivo
    destino_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destino_path.with_suffix(destino_path.suffix + ".tmp")

    try:
        try:
            conteudo = base64.b64decode(base64_payload, validate=True)
        except (BinasciiError, ValueError) as exc:
            raise UnexpectedApiResponseError(
                f"{operation}: base64 retornado pelo navegador eh invalido."
            ) from exc

        if not conteudo:
            raise UnexpectedApiResponseError(
                f"{operation}: base64 retornado pelo navegador esta vazio."
            )

        with tmp_path.open("wb") as arquivo_tmp:
            arquivo_tmp.write(conteudo)

        if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            raise UnexpectedApiResponseError(
                f"{operation}: arquivo temporario salvo com tamanho zero."
            )

        os.replace(tmp_path, destino_path)
        return destino_path
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass


def _ler_texto_pagina_no_browser(driver) -> str:
    texto = _executar_script_seguro(driver, "return document.body ? document.body.innerText : '';")
    if isinstance(texto, str) and texto:
        return texto
    page_source = getattr(driver, "page_source", "") or ""
    return str(page_source)


def _montar_url_listagem_file_manager(endpoint: str, pasta_normalizada: str) -> str:
    params = {
        "path": FILE_MANAGER_ROOT,
        "command": "GetDirContents",
        "arguments": json.dumps(
            {"pathInfo": _construir_path_info(pasta_normalizada)},
            ensure_ascii=False,
        ),
    }
    return requests.Request("GET", endpoint, params=params).prepare().url


def _montar_path_info_list_download(pasta_normalizada: str, nome_arquivo: str) -> list[list[dict]]:
    path_info = _construir_path_info(pasta_normalizada)
    file_parent_key = path_info[-1]["key"] if path_info else ""
    file_key = f"{file_parent_key}\\{nome_arquivo}" if file_parent_key else nome_arquivo
    return [
        path_info
        + [
            {
                "key": file_key,
                "name": nome_arquivo,
            }
        ]
    ]


def _montar_argumentos_download_form(pasta_normalizada: str, nome_arquivo: str) -> str:
    return json.dumps(
        {"pathInfoList": _montar_path_info_list_download(pasta_normalizada, nome_arquivo)},
        ensure_ascii=False,
    )


def _montar_action_download_file_manager(endpoint: str) -> str:
    return requests.Request(
        "POST",
        endpoint,
        params={"path": FILE_MANAGER_ROOT},
    ).prepare().url


def _parse_listing_text_no_browser(texto: str, pasta_label: str, current_url: str) -> list[dict]:
    operation = f"listagem da pasta '{pasta_label}'"
    texto_limpo = str(texto or "").strip()
    preview = _preview_text(texto_limpo, limit=240)

    if "/login" in _path_from_url(current_url):
        raise SessionExpiredError(
            f"{operation}: o navegador foi direcionado para login durante a listagem."
        )
    if not texto_limpo:
        raise UnexpectedApiResponseError(
            f"{operation}: a navegacao do navegador retornou corpo vazio."
        )
    if _parece_html_ou_login(texto_limpo):
        raise SessionExpiredError(
            f"{operation}: a navegacao do navegador retornou pagina HTML/login em vez do JSON esperado."
        )
    if not _parece_json_textual(texto_limpo):
        raise UnexpectedApiResponseError(
            f"{operation}: o navegador nao retornou JSON valido. Preview: {preview or '<vazio>'}"
        )

    try:
        payload = json.loads(texto_limpo)
    except ValueError as exc:
        raise UnexpectedApiResponseError(
            f"{operation}: falha ao interpretar o JSON retornado pela navegacao do navegador. Preview: {preview or '<vazio>'}"
        ) from exc

    return _extrair_itens_listagem(payload)


def _resolver_base_url_file_manager_no_browser(driver) -> str:
    origem_configurada = _resolver_origem_configurado()
    if origem_configurada:
        logger.info(
            "Usando base URL configurada para o contexto same-origin do File Manager: %s",
            origem_configurada,
        )
        return origem_configurada

    contexto = _resolver_origem_driver(driver)
    logger.info(
        "FILE_MANAGER_API_BASE_URL nao configurada; usando fallback de descoberta para o contexto same-origin do File Manager: base_url=%s motivo=%s",
        contexto["origin"],
        contexto.get("reason"),
    )
    return contexto["origin"]


def _preview_texto_contexto_browser(driver) -> str:
    preview = _executar_script_seguro(
        driver,
        "return document.body ? document.body.innerText.slice(0, 200) : '';",
    )
    if preview:
        return preview
    page_source = getattr(driver, "page_source", "") or ""
    return str(page_source)[:200]


def _validar_contexto_file_manager_carregado(driver, base_url: str, handle) -> str:
    deadline = monotonic() + BROWSER_CONTEXT_LOAD_TIMEOUT_SECONDS
    ultimo_url = getattr(driver, "current_url", "") or ""
    ultimo_origin = _executar_script_seguro(driver, "return window.location.origin;")
    ultimo_ready = _executar_script_seguro(driver, "return document.readyState;")

    while monotonic() < deadline:
        ultimo_url = getattr(driver, "current_url", "") or ""
        ultimo_origin = _executar_script_seguro(driver, "return window.location.origin;")
        ultimo_ready = _executar_script_seguro(driver, "return document.readyState;")

        same_origin = (
            (isinstance(ultimo_origin, str) and ultimo_origin.rstrip("/") == base_url)
            or _url_tem_origem(ultimo_url, base_url)
        )
        ready = ultimo_ready in (None, "interactive", "complete")
        if same_origin and ready:
            break
        sleep(BROWSER_CONTEXT_LOAD_POLL_SECONDS)
    else:
        raise SessionExpiredError(
            f"A porta do File Manager em '{base_url}' nao carregou corretamente no navegador. Isso pode indicar autenticacao propria ou bloqueio de carregamento."
        )

    preview = _preview_texto_contexto_browser(driver).strip().lower()
    if preview and any(marker in preview for marker in BROWSER_AUTH_MARKERS):
        raise SessionExpiredError(
            f"A porta do File Manager em '{base_url}' parece exigir autenticacao propria no navegador."
        )

    logger.info(
        "Contexto same-origin do File Manager validado: handle=%s url=%s readyState=%s",
        handle,
        ultimo_url or f"{base_url}/",
        ultimo_ready or "<desconhecido>",
    )
    return ultimo_url or f"{base_url}/"


def garantir_contexto_file_manager_no_browser(driver) -> dict:
    original_handle = getattr(driver, "current_window_handle", None)
    url_original = (
        _executar_script_seguro(driver, "return window.location.href;")
        or getattr(driver, "current_url", "")
        or ""
    )
    base_url = _resolver_base_url_file_manager_no_browser(driver).rstrip("/")
    endpoint = f"{base_url}{FILE_MANAGER_ENDPOINT}"

    logger.info(
        "Garantindo contexto same-origin do File Manager na mesma aba: handle_atual=%s url_atual=%s url_original=%s base_url=%s",
        original_handle,
        url_original or "<vazia>",
        url_original or "<vazia>",
        base_url,
    )

    try:
        driver.get(base_url)
        page_url = _validar_contexto_file_manager_carregado(driver, base_url, original_handle)
        handle_file_manager = getattr(driver, "current_window_handle", None) or original_handle
        mesma_aba = bool(handle_file_manager == original_handle)
        logger.info(
            "Contexto do File Manager carregado na mesma guia: handle_atual=%s handle_original=%s handle_file_manager=%s url_atual=%s url_original=%s mesma_aba=%s aba_nova=%s",
            getattr(driver, "current_window_handle", None),
            original_handle,
            handle_file_manager,
            page_url,
            url_original or "<vazia>",
            mesma_aba,
            False,
        )
        return {
            "handle": handle_file_manager,
            "handle_original": original_handle,
            "handle_file_manager": handle_file_manager,
            "base_url": base_url,
            "endpoint": endpoint,
            "page_url": page_url,
            "original_handle": original_handle,
            "url_original": url_original,
            "mesma_aba": mesma_aba,
            "aba_nova": False,
            "created": False,
        }
    except SessionExpiredError:
        raise
    except Exception as exc:
        raise FileManagerApiError(
            f"Falha ao garantir o contexto same-origin do File Manager em '{base_url}'."
        ) from exc


def _executar_no_contexto_api_no_browser(
    driver,
    operation: str,
    executor,
    *,
    restore_original_url: bool = False,
):
    contexto = garantir_contexto_file_manager_no_browser(driver)
    original_handle = contexto.get("original_handle")
    selected_handle = contexto.get("handle")
    url_original = contexto.get("url_original")
    try:
        if selected_handle and selected_handle != getattr(driver, "current_window_handle", None):
            driver.switch_to.window(selected_handle)
        current_url = getattr(driver, "current_url", "") or ""
        mesma_aba = bool((selected_handle or original_handle) == original_handle)
        logger.info(
            "Executando %s no navegador autenticado: handle_atual=%s handle_original=%s handle_file_manager=%s url_atual=%s url_original=%s mesma_aba=%s aba_nova=%s base_url=%s endpoint=%s",
            operation,
            getattr(driver, "current_window_handle", None),
            original_handle,
            selected_handle or original_handle,
            current_url or contexto.get("page_url"),
            url_original or "<vazia>",
            mesma_aba,
            bool(contexto.get("aba_nova", contexto.get("created"))),
            contexto["base_url"],
            contexto["endpoint"],
        )
        return executor(contexto)
    finally:
        if original_handle and getattr(driver, "current_window_handle", None) != original_handle:
            driver.switch_to.window(original_handle)
        if (
            restore_original_url
            and url_original
            and getattr(driver, "current_window_handle", None) == original_handle
            and (getattr(driver, "current_url", "") or "") != url_original
        ):
            logger.info(
                "Retornando explicitamente para a URL original apos %s: handle=%s url_original=%s",
                operation,
                original_handle,
                url_original,
            )
            driver.get(url_original)


def listar_arquivos_api_no_browser(driver, pasta: str) -> list[dict]:
    pasta_normalizada = _normalizar_pasta_remota(pasta)
    pasta_label = pasta_normalizada or FILE_MANAGER_ROOT
    logger.info("Executando listagem via navegacao do navegador para a pasta '%s'", pasta_label)

    def _executar(contexto):
        list_url = _montar_url_listagem_file_manager(contexto["endpoint"], pasta_normalizada)
        logger.info(
            "Navegando na aba do File Manager para listar arquivos: handle_file_manager=%s url=%s",
            contexto["handle"],
            list_url,
        )
        driver.get(list_url)
        current_url = _validar_contexto_file_manager_carregado(
            driver,
            contexto["base_url"],
            contexto["handle"],
        )
        return {
            "url": current_url,
            "text": _ler_texto_pagina_no_browser(driver),
            "url_original": contexto.get("url_original"),
            "same_tab": contexto.get("mesma_aba", True),
        }

    result = _executar_no_contexto_api_no_browser(
        driver,
        f"listagem da pasta '{pasta_label}'",
        _executar,
    )
    raw_text = result.get("text") or ""
    logger.info(
        "Payload bruto da listagem (primeiros 1000 caracteres): handle_atual=%s url_atual=%s url_original=%s mesma_aba=%s payload=%s",
        getattr(driver, "current_window_handle", None),
        result.get("url") or "",
        result.get("url_original") or "",
        result.get("same_tab"),
        _preview_text(raw_text, limit=1000) or "<vazio>",
    )
    try:
        itens = _parse_listing_text_no_browser(
            raw_text,
            pasta_label,
            result.get("url") or "",
        )
    except (SessionExpiredError, UnexpectedApiResponseError):
        logger.warning(
            "Falha na listagem via endpoint bruto do File Manager. Ponto preparado para fallback futuro via navegacao UI autenticada."
        )
        raise
    itens_normalizados = [_normalizar_item_listagem(item) for item in itens]
    logger.info(
        "Listagem via navegacao do navegador concluida: %s item(ns) em '%s'",
        len(itens_normalizados),
        pasta_label,
    )
    return itens_normalizados


def baixar_arquivo_via_form_submit_no_browser(driver, pasta: str, nome_arquivo: str) -> dict:
    pasta_normalizada = _normalizar_pasta_remota(pasta)
    arguments = _montar_argumentos_download_form(pasta_normalizada, nome_arquivo)
    logger.info(
        "Disparando download via form submit no navegador: pasta='%s' arquivo='%s'",
        pasta_normalizada or FILE_MANAGER_ROOT,
        nome_arquivo,
    )

    def _executar(contexto):
        action_url = _montar_action_download_file_manager(contexto["endpoint"])
        logger.info(
            "Submetendo formulario de download na aba do File Manager: handle_file_manager=%s action=%s",
            contexto["handle"],
            action_url,
        )
        result = driver.execute_script(
            BROWSER_DOWNLOAD_FORM_SUBMIT_SCRIPT,
            action_url,
            {
                "command": "Download",
                "arguments": arguments,
            },
        )
        return {
            "action": action_url,
            "result": result,
        }

    result = _executar_no_contexto_api_no_browser(
        driver,
        f"download do arquivo '{nome_arquivo}'",
        _executar,
    )
    submit_result = result.get("result")
    if isinstance(submit_result, dict) and submit_result.get("submitted") is False:
        raise FileManagerApiError(
            f"download do arquivo '{nome_arquivo}': o navegador nao conseguiu submeter o formulario de download ({submit_result.get('error') or 'erro desconhecido'})."
        )
    logger.info(
        "Form submit de download disparado com sucesso: arquivo='%s' action=%s",
        nome_arquivo,
        result.get("action"),
    )
    return {
        "action": result.get("action"),
        "arguments": arguments,
    }


def _encontrar_download_real_em_diretorio(download_dir: Path, nome_arquivo: str) -> Path | None:
    esperado = download_dir / nome_arquivo
    if esperado.exists():
        return esperado

    stem = Path(nome_arquivo).stem
    suffix = Path(nome_arquivo).suffix
    var_regex = re.compile(rf"^{re.escape(stem)} \(\d+\){re.escape(suffix)}$")

    candidatos = []
    for item in download_dir.iterdir():
        if not item.is_file():
            continue
        if not var_regex.match(item.name):
            continue
        try:
            if item.stat().st_size <= 0:
                continue
        except OSError:
            continue
        candidatos.append(item)

    if not candidatos:
        return None
    return max(candidatos, key=lambda p: p.stat().st_mtime)


def _download_temporario_presente_em_diretorio(download_dir: Path, nome_arquivo: str) -> bool:
    stem = Path(nome_arquivo).stem
    suffix = Path(nome_arquivo).suffix
    var_temp_regex = re.compile(
        rf"^{re.escape(stem)} \(\d+\){re.escape(suffix)}\.crdownload$"
    )

    for item in download_dir.iterdir():
        if not item.is_file():
            continue
        if item.name == f"{nome_arquivo}.crdownload" or bool(var_temp_regex.match(item.name)):
            return True
    return False


def _aguardar_download_em_diretorio(destino, nome_arquivo: str, timeout_seconds: float = 120.0) -> Path:
    destino_path = Path(destino)
    download_dir = destino_path if destino_path.is_dir() else destino_path.parent
    deadline = monotonic() + timeout_seconds
    ultimo_encontrado = None

    while monotonic() < deadline:
        arquivo_final = _encontrar_download_real_em_diretorio(download_dir, nome_arquivo)
        if arquivo_final is not None:
            ultimo_encontrado = arquivo_final
            try:
                tamanho = int(arquivo_final.stat().st_size)
            except OSError:
                tamanho = 0
            if tamanho > 0 and not _download_temporario_presente_em_diretorio(download_dir, nome_arquivo):
                return arquivo_final
        sleep(0.5)

    if ultimo_encontrado is not None:
        raise RuntimeError(
            f"Download do arquivo '{nome_arquivo}' nao finalizou no diretorio {download_dir}."
        )
    raise RuntimeError(
        f"Download do arquivo '{nome_arquivo}' nao apareceu no diretorio {download_dir}."
    )


def baixar_arquivo_api_no_browser(driver, pasta: str, nome_arquivo: str, destino) -> Path:
    baixar_arquivo_via_form_submit_no_browser(driver, pasta, nome_arquivo)
    return _aguardar_download_em_diretorio(destino, nome_arquivo)


def _executar_get_dir_contents(sessao: requests.Session, path_info: list[dict], pasta_label: str) -> list[dict]:
    endpoint = _resolver_endpoint(sessao)
    headers = _headers_listagem(sessao)
    params = {
        "path": FILE_MANAGER_ROOT,
        "command": "GetDirContents",
        "arguments": json.dumps(
            {"pathInfo": path_info},
            ensure_ascii=False,
        ),
    }
    operation = f"listagem da pasta '{pasta_label}'"

    _log_request_diagnostic(
        sessao,
        "GET",
        endpoint,
        params=params,
        headers=headers,
        operation=operation,
    )
    response = None
    try:
        response = sessao.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        _log_response_diagnostic(response, operation)
        return _parse_listing_response(response, pasta_label)
    finally:
        if response is not None:
            response.close()


def _diagnosticar_autenticacao_sessao_http(sessao: requests.Session) -> None:
    if not _session_attr_bool(sessao, "revo360_auth_probe_enabled"):
        return
    if _session_attr_bool(sessao, "revo360_auth_probe_done"):
        return

    logger.info(
        "Executando diagnostico de autenticacao HTTP da API File Manager na raiz '%s'",
        FILE_MANAGER_ROOT,
    )

    def _operacao():
        itens = _executar_get_dir_contents(sessao, [], FILE_MANAGER_ROOT)
        logger.info(
            "Diagnostico da sessao HTTP da API concluido: raiz acessivel com %s item(ns)",
            len(itens),
        )
        return None

    try:
        _executar_com_retry("diagnostico da sessao HTTP da API", _operacao)
    except SessionExpiredError:
        logger.error(
            "Diagnostico da sessao HTTP da API detectou HTML/login na raiz do File Manager. Os cookies copiados do Selenium nao autenticaram a API."
        )
        raise

    sessao.revo360_auth_probe_done = True


def criar_sessao_requests_do_driver(driver) -> requests.Session:
    sessao = requests.Session()
    contexto_http = _resolver_origem_driver(driver)
    current_url = contexto_http["page_url"]
    origin = contexto_http["origin"]
    referer = contexto_http["referer"]
    endpoint = contexto_http["endpoint"]
    selected_handle = contexto_http.get("selected_handle")
    original_handle = contexto_http.get("original_handle")

    user_agent = None
    try:
        user_agent = driver.execute_script("return navigator.userAgent;")
    except Exception:
        user_agent = None

    sessao.headers.update(
        {
            "Accept": FILE_MANAGER_LIST_ACCEPT,
            "Origin": origin,
            "Referer": referer,
        }
    )
    if user_agent:
        sessao.headers["User-Agent"] = user_agent

    cookies = []
    try:
        if selected_handle and selected_handle != getattr(driver, "current_window_handle", None):
            driver.switch_to.window(selected_handle)
        cookies = driver.get_cookies()
    finally:
        if original_handle and getattr(driver, "current_window_handle", None) != original_handle:
            driver.switch_to.window(original_handle)

    cookie_names = [cookie.get("name") for cookie in cookies if cookie.get("name")]
    logger.info(
        "Criando sessao HTTP a partir do Selenium: pagina_atual=%s base_url=%s endpoint=%s cookies=%s nomes=%s",
        current_url,
        origin,
        endpoint,
        len(cookies),
        cookie_names,
    )
    for cookie_diag in _cookie_diagnostic_rows(cookies):
        logger.info(
            "Cookie Selenium detectado: name=%s domain=%s path=%s secure=%s",
            cookie_diag["name"],
            cookie_diag["domain"],
            cookie_diag["path"],
            cookie_diag["secure"],
        )

    for cookie in cookies:
        if not cookie.get("name"):
            continue

        cookie_kwargs = {}
        if cookie.get("domain"):
            cookie_kwargs["domain"] = cookie["domain"]
        cookie_kwargs["path"] = cookie.get("path") or "/"
        if "secure" in cookie:
            cookie_kwargs["secure"] = bool(cookie["secure"])
        if cookie.get("expiry") is not None:
            cookie_kwargs["expires"] = cookie["expiry"]

        rest = {}
        if cookie.get("httpOnly") is not None:
            rest["HttpOnly"] = cookie["httpOnly"]
        if cookie.get("sameSite"):
            rest["SameSite"] = cookie["sameSite"]
        if rest:
            cookie_kwargs["rest"] = rest

        sessao.cookies.set(cookie["name"], cookie.get("value", ""), **cookie_kwargs)

    sessao.revo360_origin = origin
    sessao.revo360_base_url = origin
    sessao.revo360_referer = referer
    sessao.revo360_page_url = current_url
    sessao.revo360_file_manager_endpoint = endpoint
    sessao.revo360_origin_resolution_reason = contexto_http.get("reason")
    sessao.revo360_auth_probe_enabled = True
    sessao.revo360_auth_probe_done = False
    logger.info(
        "Sessao HTTP criada com headers default=%s user_agent=%s cookies=%s",
        _headers_sem_sensiveis(sessao.headers),
        user_agent or "<indisponivel>",
        _session_cookie_names(sessao),
    )
    return sessao


def _listar_arquivos_api_raw(sessao: requests.Session, pasta: str) -> list[dict]:
    pasta_normalizada = _normalizar_pasta_remota(pasta)
    path_info = _construir_path_info(pasta_normalizada)
    _diagnosticar_autenticacao_sessao_http(sessao)

    def _operacao():
        return _executar_get_dir_contents(
            sessao,
            path_info,
            pasta_normalizada or FILE_MANAGER_ROOT,
        )

    return _executar_com_retry(
        f"listagem da pasta '{pasta_normalizada or FILE_MANAGER_ROOT}'",
        _operacao,
    )


def listar_arquivos_api(sessao: requests.Session, pasta: str) -> list[dict]:
    itens = _listar_arquivos_api_raw(sessao, pasta)
    itens_normalizados = [_normalizar_item_listagem(item) for item in itens]
    logger.info(
        "Listagem via API concluida para '%s': %s item(ns)",
        _normalizar_pasta_remota(pasta) or FILE_MANAGER_ROOT,
        len(itens_normalizados),
    )
    return itens_normalizados


def baixar_arquivo_api(
    sessao: requests.Session,
    pasta: str,
    nome_arquivo: str,
    destino,
) -> Path:
    pasta_normalizada = _normalizar_pasta_remota(pasta)
    itens = _listar_arquivos_api_raw(sessao, pasta)
    item = _selecionar_item_por_nome(itens, nome_arquivo)
    if bool(item.get("isDirectory")):
        raise FileManagerApiError(f"O item solicitado eh um diretorio: {nome_arquivo}")

    destino_path = Path(destino)
    if destino_path.exists() and destino_path.is_dir():
        destino_path = destino_path / nome_arquivo
    destino_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destino_path.with_suffix(destino_path.suffix + ".tmp")
    path_info = _construir_path_info(pasta_normalizada)
    file_parent_key = path_info[-1]["key"] if path_info else ""
    file_key = f"{file_parent_key}\\{nome_arquivo}" if file_parent_key else nome_arquivo
    arguments = json.dumps(
        {
            "pathInfoList": [
                path_info
                + [
                    {
                        "key": file_key,
                        "name": nome_arquivo,
                    }
                ]
            ]
        },
        ensure_ascii=False,
    )

    logger.info(
        "Arquivo selecionado para download via API na pasta '%s': %s",
        pasta_normalizada or FILE_MANAGER_ROOT,
        nome_arquivo,
    )
    logger.info(
        "Iniciando download via API do arquivo '%s' para %s",
        nome_arquivo,
        destino_path,
    )

    def _operacao():
        response = None
        try:
            response = sessao.post(
                _resolver_endpoint(sessao),
                params={"path": FILE_MANAGER_ROOT},
                data={
                    "command": "Download",
                    "arguments": arguments,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                stream=True,
                timeout=REQUEST_TIMEOUT,
            )
            operation = f"download do arquivo '{nome_arquivo}'"
            _validar_headers_download(response, nome_arquivo, operation)

            chunks = response.iter_content(chunk_size=64 * 1024)
            primeiro_chunk = next(chunks, b"")
            _validar_primeiro_chunk_download(response, nome_arquivo, primeiro_chunk)

            with tmp_path.open("wb") as arquivo_tmp:
                arquivo_tmp.write(primeiro_chunk)
                for chunk in chunks:
                    if not chunk:
                        continue
                    arquivo_tmp.write(chunk)

            if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
                raise UnexpectedApiResponseError(
                    f"{operation}: arquivo temporario salvo com tamanho zero."
                )

            os.replace(tmp_path, destino_path)
            logger.info(
                "Download via API concluido: %s (%s bytes)",
                destino_path,
                destino_path.stat().st_size,
            )
            return destino_path
        finally:
            if response is not None:
                response.close()
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

    return _executar_com_retry(
        f"download do arquivo '{nome_arquivo}'",
        _operacao,
    )


def extrair_data_nome_arquivo(nome) -> date | None:
    if nome is None:
        return None

    for match in DATE_IN_FILENAME_RE.finditer(Path(str(nome)).name):
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            continue
    return None


def selecionar_csv_por_data(itens: list[dict], cycle_date: date) -> dict:
    if isinstance(cycle_date, datetime):
        cycle_date = cycle_date.date()

    for item in itens:
        nome = item.get("name")
        if not isinstance(nome, str):
            continue
        if not nome.lower().endswith(".csv"):
            continue
        if extrair_data_nome_arquivo(nome) == cycle_date:
            return item

    raise FileManagerApiError(f"Nenhum CSV encontrado para a data {cycle_date.isoformat()}.")
