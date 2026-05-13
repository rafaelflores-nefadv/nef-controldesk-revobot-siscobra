from __future__ import annotations

import base64
from collections.abc import Mapping
from datetime import date, datetime
import json
import logging
import os
from pathlib import Path
import re
from time import sleep
from urllib.parse import unquote, urlsplit

import requests

from config.settings import (
    BASE_URL,
    FILE_MANAGER_API_BASE_URL,
    FILE_MANAGER_HTTP_RETRY_ATTEMPTS,
    FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS,
    FILE_MANAGER_HTTP_TIMEOUT_CONNECT,
    FILE_MANAGER_HTTP_TIMEOUT_READ,
    PASSWORD,
    REVO360_API_BASE_URL,
    REVO360_APPLICATION_ID,
    USERNAME,
)

logger = logging.getLogger(__name__)

FILE_MANAGER_ENDPOINT = "/api/file-manager-file-system"
FILE_MANAGER_ROOT = r"..\UPLOAD"
FILE_MANAGER_LIST_ACCEPT = "application/json, text/javascript, */*; q=0.01"
REQUEST_TIMEOUT = (
    FILE_MANAGER_HTTP_TIMEOUT_CONNECT,
    FILE_MANAGER_HTTP_TIMEOUT_READ,
)
DATE_IN_FILENAME_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")
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
    "application/csv",
    "text/csv",
    "application/vnd.ms-excel",
}
RETRYABLE_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
MAX_LOG_PREVIEW_ITEMS = 5
SENSITIVE_HEADERS = {"cookie", "authorization", "proxy-authorization"}
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


def _headers_download(sessao: requests.Session) -> dict[str, str]:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
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


def _basic_auth_header(usuario: str, senha: str) -> str:
    token = base64.b64encode(f"{usuario}:{senha}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _resolver_revo360_api_base_url() -> str:
    configurado = str(REVO360_API_BASE_URL or "").strip()
    if configurado:
        return configurado.rstrip("/")
    return f"{_extrair_origem_http(BASE_URL)}/api"


def criar_sessao_revo360_http(
    usuario: str | None = None,
    senha: str | None = None,
) -> requests.Session:
    """Cria uma sessao HTTP autenticada no REVO360."""
    usuario = usuario or USERNAME
    senha = senha or PASSWORD
    if not usuario or not senha:
        raise RuntimeError("Credenciais do REVO360 nao configuradas para login HTTP.")

    sessao = requests.Session()
    api_base_url = _resolver_revo360_api_base_url()
    portal_origin = _extrair_origem_http(BASE_URL)
    file_manager_origin = _resolver_origem_configurado() or portal_origin
    file_manager_endpoint = f"{file_manager_origin}{FILE_MANAGER_ENDPOINT}"

    sessao.headers.update(
        {
            "Accept": FILE_MANAGER_LIST_ACCEPT,
            "Origin": portal_origin,
            "Referer": f"{portal_origin}/",
        }
    )

    server_info_url = f"{api_base_url}/revolutionService/getServerInfo"
    login_url = f"{api_base_url}/revolutionService/loginUserCRM"

    logger.info("Iniciando autenticacao HTTP no REVO360: %s", login_url)
    server_info_response = None
    login_response = None
    try:
        server_info_response = sessao.post(
            server_info_url,
            json={
                "applicationId": REVO360_APPLICATION_ID,
                "hostApi": urlsplit(portal_origin).hostname or "nef.revo360.io",
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )
        _raise_for_status_with_context(server_info_response, "obtencao do serverInfo do REVO360")
        server_info = server_info_response.json()
        api_user = server_info.get("userName") if isinstance(server_info, dict) else None
        api_password = server_info.get("password") if isinstance(server_info, dict) else None
        if not api_user or not api_password:
            raise UnexpectedApiResponseError(
                "Login HTTP do REVO360: serverInfo nao retornou credenciais basicas da API."
            )

        login_response = sessao.post(
            login_url,
            json={
                "applicationId": REVO360_APPLICATION_ID,
                "userName": usuario,
                "password": senha,
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": _basic_auth_header(str(api_user), str(api_password)),
            },
            timeout=REQUEST_TIMEOUT,
        )
        _raise_for_status_with_context(login_response, "login HTTP do REVO360")
        payload = login_response.json()
        if not isinstance(payload, list) or not payload:
            raise UnexpectedApiResponseError(
                "Login HTTP do REVO360: resposta de autenticacao nao retornou usuario valido."
            )

        sessao.revo360_origin = file_manager_origin
        sessao.revo360_base_url = file_manager_origin
        sessao.revo360_referer = f"{portal_origin}/"
        sessao.revo360_page_url = BASE_URL
        sessao.revo360_file_manager_endpoint = file_manager_endpoint
        sessao.revo360_origin_resolution_reason = "config.FILE_MANAGER_API_BASE_URL"
        sessao.revo360_auth_probe_enabled = True
        sessao.revo360_auth_probe_done = False
        logger.info(
            "Login HTTP no REVO360 concluido: usuario=%s endpoint_file_manager=%s",
            usuario,
            file_manager_endpoint,
        )
        return sessao
    except ValueError as exc:
        raise UnexpectedApiResponseError(
            "Login HTTP do REVO360: resposta JSON invalida."
        ) from exc
    finally:
        if server_info_response is not None:
            server_info_response.close()
        if login_response is not None:
            login_response.close()


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
    if getattr(response, "status_code", None) != 200:
        raise FileManagerApiError(
            f"{operation}: status HTTP inesperado {getattr(response, 'status_code', None)}."
        )
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
            "Diagnostico da sessao HTTP da API detectou HTML/login na raiz do File Manager. A sessao HTTP nao autenticou a API."
        )
        raise

    sessao.revo360_auth_probe_done = True


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

    return baixar_exportacao_revo360(sessao, pasta_normalizada, nome_arquivo, destino)


def baixar_exportacao_revo360(
    sessao: requests.Session,
    pasta: str,
    arquivo: str,
    destino,
) -> Path:
    pasta_normalizada = _normalizar_pasta_remota(pasta)

    destino_path = Path(destino)
    if destino_path.exists() and destino_path.is_dir():
        destino_path = destino_path / arquivo
    destino_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destino_path.with_suffix(destino_path.suffix + ".tmp")
    arguments = _montar_argumentos_download_form(pasta_normalizada, arquivo)

    logger.info(
        "Carteira iniciada para download via API: %s",
        pasta_normalizada or FILE_MANAGER_ROOT,
    )
    logger.info(
        "Arquivo solicitado via API: %s",
        arquivo,
    )
    logger.info(
        "Iniciando download HTTP direto do arquivo '%s' para %s",
        arquivo,
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
                headers=_headers_download(sessao),
                stream=True,
                timeout=REQUEST_TIMEOUT,
            )
            operation = f"download do arquivo '{arquivo}'"
            _validar_headers_download(response, arquivo, operation)

            chunks = response.iter_content(chunk_size=64 * 1024)
            primeiro_chunk = next(chunks, b"")
            _validar_primeiro_chunk_download(response, arquivo, primeiro_chunk)

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
                "Download HTTP direto concluido com sucesso: arquivo=%s caminho=%s tamanho=%s bytes",
                arquivo,
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
        f"download do arquivo '{arquivo}'",
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
