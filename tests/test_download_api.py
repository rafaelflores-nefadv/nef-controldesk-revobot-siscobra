import base64
import json
import shutil
import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import download_api


class FakeResponse:
    def __init__(
        self,
        *,
        payload=None,
        chunks=None,
        status_code=200,
        headers=None,
        text=None,
        url="https://nef.revo360.io/api/file-manager-file-system",
        history=None,
        json_error: Exception | None = None,
    ):
        self._payload = payload
        self._chunks = chunks or []
        self.status_code = status_code
        self.headers = headers or {}
        self.url = url
        self.history = history or []
        self.closed = False
        self._json_error = json_error
        if text is not None:
            self.text = text
        elif payload is not None:
            self.text = str(payload)
        else:
            self.text = ""

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"status={self.status_code}")
            error.response = self
            raise error

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload

    def iter_content(self, chunk_size=1):
        del chunk_size
        for chunk in self._chunks:
            yield chunk

    def close(self):
        self.closed = True


class FakeSwitchTo:
    def __init__(self, driver):
        self.driver = driver

    def window(self, handle):
        if handle not in self.driver.window_handles:
            raise RuntimeError(f"Handle desconhecido: {handle}")
        self.driver.switch_calls.append(handle)
        self.driver.current_window_handle = handle

    def new_window(self, window_type):
        self.driver.new_window_calls.append(window_type)
        new_handle = f"window_{len(self.driver.window_handles) + 1}"
        self.driver.window_handles.append(new_handle)
        self.driver._contexts[new_handle] = {
            "url": "about:blank",
            "origin": None,
            "cookies": [],
            "ready_state": "complete",
            "body_text": "",
        }
        self.driver.current_window_handle = new_handle


class FakeSeleniumDriver:
    def __init__(
        self,
        contexts,
        original_handle,
        user_agent="Mozilla/5.0 Teste",
        navigation_overrides=None,
    ):
        self._contexts = contexts
        self.window_handles = list(contexts.keys())
        self.current_window_handle = original_handle
        self.switch_calls = []
        self.new_window_calls = []
        self.get_calls = []
        self.switch_to = FakeSwitchTo(self)
        self._user_agent = user_agent
        self._navigation_overrides = navigation_overrides or {}

    @property
    def current_url(self):
        return self._contexts[self.current_window_handle].get("url", "")

    @property
    def page_source(self):
        return self._contexts[self.current_window_handle].get("page_source", "")

    def execute_script(self, script):
        contexto = self._contexts[self.current_window_handle]
        if script == "return navigator.userAgent;":
            return self._user_agent
        if script == "return window.location.href;":
            return contexto.get("url")
        if script == "return window.location.origin;":
            return contexto.get("origin")
        if script == "return document.body ? document.body.innerText : '';":
            return contexto.get("body_text", "")
        if script == "return document.readyState;":
            return contexto.get("ready_state", "complete")
        if script == "return document.body ? document.body.innerText.slice(0, 200) : '';":
            return contexto.get("body_text", "")
        if script == "return window.sessionStorage.getItem('requestApiRevolution');":
            return contexto.get("request_api_revolution")
        raise AssertionError(f"Script inesperado no teste: {script}")

    def get_cookies(self):
        return list(self._contexts[self.current_window_handle].get("cookies", []))

    def get(self, url):
        self.get_calls.append(url)
        override = self._navigation_overrides.get(url)
        if isinstance(override, Exception):
            raise override

        contexto = self._contexts[self.current_window_handle]
        contexto["url"] = url
        contexto["origin"] = download_api._extrair_origem_http(url)
        contexto["ready_state"] = "complete"
        contexto["body_text"] = ""
        contexto["page_source"] = ""

        if isinstance(override, dict):
            contexto.update(override)


class DownloadApiTests(unittest.TestCase):
    def test_criar_sessao_requests_do_driver_copia_cookies_e_headers(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "main": {
                    "url": "https://nef.revo360.io/control-desk",
                    "origin": "https://nef.revo360.io",
                    "cookies": [
                        {
                            "name": "sessionid",
                            "value": "abc123",
                            "domain": "nef.revo360.io",
                            "path": "/",
                            "secure": True,
                            "httpOnly": True,
                            "sameSite": "Lax",
                        }
                    ],
                }
            },
            "main",
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", None):
            sessao = download_api.criar_sessao_requests_do_driver(driver)

        self.assertIsInstance(sessao, requests.Session)
        self.assertEqual(
            sessao.headers["Accept"],
            "application/json, text/javascript, */*; q=0.01",
        )
        self.assertEqual(sessao.headers["Origin"], "https://nef.revo360.io")
        self.assertEqual(sessao.headers["Referer"], "https://nef.revo360.io/")
        self.assertEqual(sessao.headers["User-Agent"], "Mozilla/5.0 Teste")
        self.assertEqual(
            sessao.revo360_file_manager_endpoint,
            "https://nef.revo360.io/api/file-manager-file-system",
        )
        self.assertEqual(sessao.revo360_base_url, "https://nef.revo360.io")
        self.assertEqual(sessao.revo360_referer, "https://nef.revo360.io/")
        self.assertEqual(sessao.revo360_page_url, "https://nef.revo360.io/control-desk")
        self.assertTrue(sessao.revo360_auth_probe_enabled)
        self.assertFalse(sessao.revo360_auth_probe_done)
        self.assertEqual(sessao.cookies.get("sessionid"), "abc123")
        cookie = sessao.cookies._cookies["nef.revo360.io"]["/"]["sessionid"]
        self.assertEqual(cookie.domain, "nef.revo360.io")
        self.assertEqual(cookie.path, "/")
        self.assertTrue(cookie.secure)

    def test_criar_sessao_requests_do_driver_preserva_porta_no_origin(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "main": {
                    "url": "https://nef.revo360.io:10024/",
                    "origin": "https://nef.revo360.io:10024",
                    "cookies": [
                        {
                            "name": "auth",
                            "value": "abc123",
                            "domain": "nef.revo360.io",
                            "path": "/",
                            "secure": True,
                        }
                    ],
                }
            },
            "main",
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", None):
            sessao = download_api.criar_sessao_requests_do_driver(driver)

        self.assertEqual(sessao.headers["Origin"], "https://nef.revo360.io:10024")
        self.assertEqual(sessao.headers["Referer"], "https://nef.revo360.io:10024/")
        self.assertEqual(sessao.revo360_base_url, "https://nef.revo360.io:10024")
        self.assertEqual(
            sessao.revo360_file_manager_endpoint,
            "https://nef.revo360.io:10024/api/file-manager-file-system",
        )

    def test_resolver_origem_driver_prioriza_aba_com_porta_10024_e_restaura_handle_original(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [],
                },
                "studio": {
                    "url": "https://nef.revo360.io:10024/control-desk",
                    "origin": "https://nef.revo360.io:10024",
                    "cookies": [{"name": "studio-cookie", "value": "1"}],
                },
            },
            "portal",
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", None):
            contexto = download_api._resolver_origem_driver(driver)

        self.assertEqual(contexto["origin"], "https://nef.revo360.io:10024")
        self.assertEqual(
            contexto["endpoint"],
            "https://nef.revo360.io:10024/api/file-manager-file-system",
        )
        self.assertEqual(contexto["selected_handle"], "studio")
        self.assertEqual(contexto["original_handle"], "portal")
        self.assertEqual(contexto["reason"], "window_handle_10024")
        self.assertEqual(driver.current_window_handle, "portal")
        self.assertEqual(driver.switch_calls, ["studio", "portal"])

    def test_resolver_origem_driver_usa_request_api_revolution_quando_nao_ha_aba_10024(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "main": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "request_api_revolution": "https://nef.revo360.io:10024/api",
                    "cookies": [{"name": "auth", "value": "1"}],
                }
            },
            "main",
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", None):
            contexto = download_api._resolver_origem_driver(driver)

        self.assertEqual(contexto["origin"], "https://nef.revo360.io:10024")
        self.assertEqual(contexto["reason"], "sessionStorage.requestApiRevolution")
        self.assertEqual(contexto["selected_handle"], "main")
        self.assertEqual(driver.current_window_handle, "main")

    def test_resolver_origem_driver_faz_fallback_para_window_location(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "main": {
                    "url": "https://nef.revo360.io/control-desk",
                    "origin": "https://nef.revo360.io",
                    "cookies": [{"name": "auth", "value": "1"}],
                }
            },
            "main",
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", None):
            contexto = download_api._resolver_origem_driver(driver)

        self.assertEqual(contexto["origin"], "https://nef.revo360.io")
        self.assertEqual(contexto["reason"], "fallback.window_location")
        self.assertEqual(contexto["selected_handle"], "main")

    def test_resolver_origem_driver_prioriza_base_configurada_sobre_request_api_revolution(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "main": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "request_api_revolution": "https://nef.revo360.io:10020",
                    "cookies": [{"name": "auth", "value": "1"}],
                }
            },
            "main",
        )

        with patch.object(
            download_api,
            "FILE_MANAGER_API_BASE_URL",
            "https://nef.revo360.io:10024",
        ):
            contexto = download_api._resolver_origem_driver(driver)

        self.assertEqual(contexto["origin"], "https://nef.revo360.io:10024")
        self.assertEqual(
            contexto["endpoint"],
            "https://nef.revo360.io:10024/api/file-manager-file-system",
        )
        self.assertEqual(contexto["reason"], "config.FILE_MANAGER_API_BASE_URL")
        self.assertEqual(contexto["selected_handle"], "main")

    def test_garantir_contexto_file_manager_no_browser_reutiliza_aba_same_origin(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [],
                },
                "file_manager": {
                    "url": "https://nef.revo360.io:10024/",
                    "origin": "https://nef.revo360.io:10024",
                    "cookies": [{"name": "auth", "value": "1"}],
                },
            },
            "portal",
        )

        with patch.object(
            download_api,
            "FILE_MANAGER_API_BASE_URL",
            "https://nef.revo360.io:10024",
        ):
            contexto = download_api.garantir_contexto_file_manager_no_browser(driver)

        self.assertEqual(contexto["handle"], "portal")
        self.assertEqual(contexto["handle_original"], "portal")
        self.assertEqual(contexto["handle_file_manager"], "portal")
        self.assertEqual(contexto["base_url"], "https://nef.revo360.io:10024")
        self.assertEqual(
            contexto["endpoint"],
            "https://nef.revo360.io:10024/api/file-manager-file-system",
        )
        self.assertEqual(contexto["url_original"], "https://nef.revo360.io/")
        self.assertTrue(contexto["mesma_aba"])
        self.assertFalse(contexto["aba_nova"])
        self.assertFalse(contexto["created"])
        self.assertEqual(contexto["original_handle"], "portal")
        self.assertEqual(driver.current_window_handle, "portal")
        self.assertEqual(driver.switch_calls, [])
        self.assertEqual(driver.new_window_calls, [])
        self.assertEqual(driver.get_calls, ["https://nef.revo360.io:10024"])

    def test_garantir_contexto_file_manager_no_browser_usa_mesma_aba_quando_nao_existe_handle_10024(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [{"name": "auth", "value": "1"}],
                }
            },
            "portal",
        )

        with patch.object(
            download_api,
            "FILE_MANAGER_API_BASE_URL",
            "https://nef.revo360.io:10024",
        ):
            contexto = download_api.garantir_contexto_file_manager_no_browser(driver)

        self.assertFalse(contexto["created"])
        self.assertEqual(contexto["handle"], "portal")
        self.assertEqual(contexto["handle_original"], "portal")
        self.assertEqual(contexto["handle_file_manager"], "portal")
        self.assertTrue(contexto["mesma_aba"])
        self.assertFalse(contexto["aba_nova"])
        self.assertEqual(contexto["base_url"], "https://nef.revo360.io:10024")
        self.assertEqual(contexto["original_handle"], "portal")
        self.assertEqual(contexto["url_original"], "https://nef.revo360.io/")
        self.assertIn("portal", driver.window_handles)
        self.assertEqual(driver.new_window_calls, [])
        self.assertEqual(driver.get_calls, ["https://nef.revo360.io:10024"])
        self.assertEqual(driver.current_window_handle, "portal")

    def test_garantir_contexto_file_manager_no_browser_falha_quando_porta_exige_autenticacao_propria(self) -> None:
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [{"name": "auth", "value": "1"}],
                }
            },
            "portal",
            navigation_overrides={
                "https://nef.revo360.io:10024": {
                    "url": "https://nef.revo360.io:10024/login",
                    "origin": "https://nef.revo360.io:10024",
                    "ready_state": "complete",
                    "body_text": "Authentication required",
                }
            },
        )

        with patch.object(
            download_api,
            "FILE_MANAGER_API_BASE_URL",
            "https://nef.revo360.io:10024",
        ):
            with self.assertRaisesRegex(
                download_api.SessionExpiredError,
                "exigir autenticacao propria",
            ):
                download_api.garantir_contexto_file_manager_no_browser(driver)

    def test_listar_arquivos_api_envia_get_dir_contents_e_normaliza_retorno(self) -> None:
        sessao = Mock()
        sessao.headers = {"User-Agent": "Mozilla/5.0 Teste"}
        sessao.revo360_origin = "https://nef.revo360.io"
        sessao.revo360_base_url = "https://nef.revo360.io"
        sessao.revo360_referer = "https://nef.revo360.io/"
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.revo360_auth_probe_enabled = False
        sessao.get.return_value = FakeResponse(
            payload={
                "result": [
                    {
                        "name": "Exportacao_Siscobra_0914_20260309.csv",
                        "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                        "dateModified": "2026-03-09T08:00:00",
                        "isDirectory": False,
                        "size": 1024,
                        "extra": "ignorado",
                    },
                    {
                        "name": "Subpasta",
                        "key": r"Exportação Siscobra 0914\Subpasta",
                        "dateModified": "2026-03-09T07:30:00",
                        "isDirectory": True,
                        "size": 0,
                    },
                ]
            },
            headers={"Content-Type": "application/json"},
        )

        itens = download_api.listar_arquivos_api(sessao, "Exportação Siscobra 0914")

        self.assertEqual(
            itens,
            [
                {
                    "name": "Exportacao_Siscobra_0914_20260309.csv",
                    "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                    "dateModified": "2026-03-09T08:00:00",
                    "isDirectory": False,
                    "size": 1024,
                },
                {
                    "name": "Subpasta",
                    "key": r"Exportação Siscobra 0914\Subpasta",
                    "dateModified": "2026-03-09T07:30:00",
                    "isDirectory": True,
                    "size": 0,
                },
            ],
        )

        sessao.get.assert_called_once_with(
            "https://nef.revo360.io/api/file-manager-file-system",
            params={
                "path": r"..\UPLOAD",
                "command": "GetDirContents",
                "arguments": json.dumps(
                    {
                        "pathInfo": [
                            {
                                "key": "Exportação Siscobra 0914",
                                "name": "Exportação Siscobra 0914",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
            },
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://nef.revo360.io",
                "Referer": "https://nef.revo360.io/",
                "User-Agent": "Mozilla/5.0 Teste",
            },
            timeout=download_api.REQUEST_TIMEOUT,
        )

    def test_listar_arquivos_api_retry_em_timeout_e_sucesso_na_segunda_tentativa(self) -> None:
        sessao = Mock()
        sessao.headers = {"User-Agent": "Mozilla/5.0 Teste"}
        sessao.revo360_origin = "https://nef.revo360.io"
        sessao.revo360_base_url = "https://nef.revo360.io"
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.revo360_auth_probe_enabled = False
        sessao.get.side_effect = [
            requests.Timeout("timeout"),
            FakeResponse(
                payload={"result": [{"name": "Exportacao_Siscobra_0914_20260309.csv"}]},
                headers={"Content-Type": "application/json"},
            ),
        ]

        with patch.object(download_api, "sleep") as sleep_mock:
            itens = download_api.listar_arquivos_api(sessao, "Exportação Siscobra 0914")

        self.assertEqual(itens, [{"name": "Exportacao_Siscobra_0914_20260309.csv", "key": None, "dateModified": None, "isDirectory": False, "size": None}])
        self.assertEqual(sessao.get.call_count, 2)
        sleep_mock.assert_called_once_with(download_api.FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS)

    def test_listar_arquivos_api_detecta_sessao_expirada_por_redirecionamento_para_login(self) -> None:
        sessao = Mock()
        sessao.headers = {"User-Agent": "Mozilla/5.0 Teste"}
        sessao.revo360_origin = "https://nef.revo360.io"
        sessao.revo360_base_url = "https://nef.revo360.io"
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.revo360_auth_probe_enabled = False
        sessao.get.return_value = FakeResponse(
            payload={"success": True},
            headers={"Content-Type": "application/json"},
            url="https://nef.revo360.io/login",
            history=[Mock(status_code=302)],
        )

        with self.assertRaisesRegex(download_api.SessionExpiredError, "login"):
            download_api.listar_arquivos_api(sessao, "Exportação Siscobra 0914")

    def test_listar_arquivos_api_detecta_html_inesperado(self) -> None:
        sessao = Mock()
        sessao.headers = {"User-Agent": "Mozilla/5.0 Teste"}
        sessao.revo360_origin = "https://nef.revo360.io"
        sessao.revo360_base_url = "https://nef.revo360.io"
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.revo360_auth_probe_enabled = False
        sessao.get.return_value = FakeResponse(
            text="<html><body>login</body></html>",
            headers={"Content-Type": "text/html"},
            json_error=ValueError("nao era json"),
        )

        with self.assertRaisesRegex(download_api.SessionExpiredError, "HTML"):
            download_api.listar_arquivos_api(sessao, "Exportação Siscobra 0914")

    def test_diagnostico_da_sessao_http_loga_html_inesperado_com_metadados(self) -> None:
        sessao = Mock()
        sessao.headers = {"User-Agent": "Mozilla/5.0 Teste"}
        sessao.revo360_origin = "https://nef.revo360.io"
        sessao.revo360_base_url = "https://nef.revo360.io"
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.revo360_auth_probe_enabled = True
        sessao.revo360_auth_probe_done = False
        sessao.get.return_value = FakeResponse(
            text="<html><body>login</body></html>",
            headers={
                "Content-Type": "text/html",
                "Content-Length": "31",
                "Location": "https://nef.revo360.io/login",
            },
            url="https://nef.revo360.io/login",
            json_error=ValueError("nao era json"),
        )

        with self.assertLogs(download_api.logger, level="INFO") as logs:
            with self.assertRaisesRegex(download_api.SessionExpiredError, "HTML|login"):
                download_api._diagnosticar_autenticacao_sessao_http(sessao)

        output = "\n".join(logs.output)
        self.assertIn("status=200", output)
        self.assertIn("content_type=text/html", output)
        self.assertIn("url=https://nef.revo360.io/login", output)
        self.assertIn("corpo HTML inesperado", output)
        sessao.get.assert_called_once_with(
            "https://nef.revo360.io/api/file-manager-file-system",
            params={
                "path": r"..\UPLOAD",
                "command": "GetDirContents",
                "arguments": json.dumps({"pathInfo": []}, ensure_ascii=False),
            },
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://nef.revo360.io",
                "Referer": "https://nef.revo360.io/",
                "User-Agent": "Mozilla/5.0 Teste",
            },
            timeout=download_api.REQUEST_TIMEOUT,
        )

    @unittest.skip("estrategia anterior baseada em fetch foi desativada")
    def test_listar_arquivos_api_no_browser_processa_json_e_normaliza_retorno(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        driver.execute_async_script.return_value = {
            "ok": True,
            "status": 200,
            "url": "https://nef.revo360.io:10024/api/file-manager-file-system",
            "contentType": "application/json",
            "payload": {
                "result": [
                    {
                        "name": "Exportacao_Siscobra_0914_20260309.csv",
                        "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                        "dateModified": "2026-03-09T08:00:00",
                        "isDirectory": False,
                        "size": 123,
                    }
                ]
            },
            "textPreview": '{"result":[{"name":"Exportacao_Siscobra_0914_20260309.csv"}]}',
        }

        with patch.object(
            download_api,
            "garantir_contexto_file_manager_no_browser",
            return_value={
                "handle": "file_manager",
                "base_url": "https://nef.revo360.io:10024",
                "page_url": "https://nef.revo360.io:10024/",
                "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                "original_handle": "main",
                "created": False,
            },
        ):
            itens = download_api.listar_arquivos_api_no_browser(driver, "Exportação Siscobra 0914")

        self.assertEqual(
            itens,
            [
                {
                    "name": "Exportacao_Siscobra_0914_20260309.csv",
                    "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                    "dateModified": "2026-03-09T08:00:00",
                    "isDirectory": False,
                    "size": 123,
                }
            ],
        )
        self.assertEqual(driver.execute_async_script.call_count, 1)
        script_args = driver.execute_async_script.call_args.args
        self.assertEqual(
            script_args[1]["endpoint"],
            "https://nef.revo360.io:10024/api/file-manager-file-system",
        )
        self.assertEqual(script_args[1]["params"]["path"], r"..\UPLOAD")
        self.assertEqual(
            json.loads(script_args[1]["params"]["arguments"]),
            {
                "pathInfo": [
                    {
                        "key": "Exportação Siscobra 0914",
                        "name": "Exportação Siscobra 0914",
                    }
                ]
            },
        )
        self.assertEqual(
            [call.args[0] for call in driver.switch_to.window.call_args_list],
            ["file_manager", "main"],
        )

    @unittest.skip("estrategia anterior baseada em fetch foi desativada")
    def test_listar_arquivos_api_no_browser_propaga_erro_do_js(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        driver.execute_async_script.return_value = {
            "ok": False,
            "error": "Failed to fetch",
        }

        with patch.object(
            download_api,
            "garantir_contexto_file_manager_no_browser",
            return_value={
                "handle": "file_manager",
                "base_url": "https://nef.revo360.io:10024",
                "page_url": "https://nef.revo360.io:10024/",
                "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                "original_handle": "main",
                "created": False,
            },
        ):
            with self.assertRaisesRegex(download_api.FileManagerApiError, "fetch no navegador: Failed to fetch"):
                download_api.listar_arquivos_api_no_browser(driver, "Exportação Siscobra 0914")

    @unittest.skip("estrategia anterior baseada em fetch/base64 foi desativada")
    def test_baixar_arquivo_api_no_browser_salva_base64_em_disco(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        conteudo = b"h1;h2\n1;2\n"
        driver.execute_async_script.return_value = {
            "ok": True,
            "status": 200,
            "url": "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD",
            "contentType": "application/octet-stream",
            "contentDisposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
            "size": len(conteudo),
            "base64Payload": base64.b64encode(conteudo).decode("ascii"),
            "textPreview": "",
        }
        itens = [
            {
                "name": "Exportacao_Siscobra_0914_20260309.csv",
                "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                "isDirectory": False,
            }
        ]
        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_browser_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        destino = base_tmp / "Exportacao_Siscobra_0914_20260309.csv"
        try:
            with patch.object(download_api, "listar_arquivos_api_no_browser", return_value=itens):
                with patch.object(
                    download_api,
                    "garantir_contexto_file_manager_no_browser",
                    return_value={
                        "handle": "file_manager",
                        "base_url": "https://nef.revo360.io:10024",
                        "page_url": "https://nef.revo360.io:10024/",
                        "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                        "original_handle": "main",
                        "created": False,
                    },
                ):
                    caminho = download_api.baixar_arquivo_api_no_browser(
                        driver,
                        "Exportação Siscobra 0914",
                        "Exportacao_Siscobra_0914_20260309.csv",
                        destino,
                    )

            self.assertEqual(caminho, destino)
            self.assertEqual(destino.read_bytes(), conteudo)
            self.assertFalse(destino.with_suffix(".csv.tmp").exists())
            self.assertEqual(
                [call.args[0] for call in driver.switch_to.window.call_args_list],
                ["file_manager", "main"],
            )
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    @unittest.skip("estrategia anterior baseada em fetch/base64 foi desativada")
    def test_baixar_arquivo_api_no_browser_falha_quando_base64_vazio(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        driver.execute_async_script.return_value = {
            "ok": True,
            "status": 200,
            "url": "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD",
            "contentType": "application/octet-stream",
            "contentDisposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
            "size": 10,
            "base64Payload": "",
            "textPreview": "",
        }
        itens = [
            {
                "name": "Exportacao_Siscobra_0914_20260309.csv",
                "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                "isDirectory": False,
            }
        ]

        with patch.object(download_api, "listar_arquivos_api_no_browser", return_value=itens):
            with patch.object(
                download_api,
                "garantir_contexto_file_manager_no_browser",
                return_value={
                    "handle": "file_manager",
                    "base_url": "https://nef.revo360.io:10024",
                    "page_url": "https://nef.revo360.io:10024/",
                    "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                    "original_handle": "main",
                    "created": False,
                },
            ):
                with self.assertRaisesRegex(download_api.UnexpectedApiResponseError, "nao retornou base64"):
                    download_api.baixar_arquivo_api_no_browser(
                        driver,
                        "Exportação Siscobra 0914",
                        "Exportacao_Siscobra_0914_20260309.csv",
                        ROOT_DIR / "downloads" / "unused.csv",
                    )

    def test_baixar_arquivo_api_faz_stream_para_arquivo_temporario_e_move_no_final(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        sessao.post.return_value = FakeResponse(
            chunks=[b"abc", b"", b"def"],
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Disposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
            },
        )
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "dateModified": "2026-03-09T08:00:00",
            "isDirectory": False,
            "size": 6,
        }

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / "nested" / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]) as listar_mock:
                caminho = download_api.baixar_arquivo_api(
                    sessao,
                    "Exportação Siscobra 0914",
                    item["name"],
                    destino,
                )

            self.assertEqual(caminho, destino)
            self.assertEqual(destino.read_bytes(), b"abcdef")
            self.assertFalse(destino.with_suffix(".csv.tmp").exists())
            listar_mock.assert_called_once_with(sessao, "Exportação Siscobra 0914")
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

        sessao.post.assert_called_once()
        post_args = sessao.post.call_args
        self.assertEqual(
            post_args.args[0],
            "https://nef.revo360.io/api/file-manager-file-system",
        )
        self.assertEqual(post_args.kwargs["params"], {"path": r"..\UPLOAD"})
        self.assertTrue(post_args.kwargs["stream"])
        self.assertEqual(post_args.kwargs["timeout"], download_api.REQUEST_TIMEOUT)
        self.assertEqual(post_args.kwargs["data"]["command"], "Download")
        self.assertEqual(
            json.loads(post_args.kwargs["data"]["arguments"]),
            {
                "pathInfoList": [[
                    {
                        "key": "Exportação Siscobra 0914",
                        "name": "Exportação Siscobra 0914",
                    },
                    {
                        "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                        "name": "Exportacao_Siscobra_0914_20260309.csv",
                    },
                ]]
            },
        )

    def test_baixar_arquivo_api_retry_em_http_503_e_sucesso_na_segunda_tentativa(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "isDirectory": False,
        }
        sessao.post.side_effect = [
            FakeResponse(status_code=503),
            FakeResponse(
                chunks=[b"abc"],
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Disposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
                },
            ),
        ]

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]):
                with patch.object(download_api, "sleep") as sleep_mock:
                    caminho = download_api.baixar_arquivo_api(
                        sessao,
                        "Exportação Siscobra 0914",
                        item["name"],
                        destino,
                    )

            self.assertEqual(caminho, destino)
            self.assertEqual(destino.read_bytes(), b"abc")
            self.assertEqual(sessao.post.call_count, 2)
            sleep_mock.assert_called_once_with(download_api.FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS)
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    def test_baixar_arquivo_api_detecta_html_no_lugar_do_binario(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "isDirectory": False,
        }
        sessao.post.return_value = FakeResponse(
            chunks=[b"<html><body>login</body></html>"],
            headers={
                "Content-Type": "text/html",
                "Content-Disposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
            },
        )

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]):
                with self.assertRaisesRegex(download_api.SessionExpiredError, "HTML|login"):
                    download_api.baixar_arquivo_api(
                        sessao,
                        "Exportação Siscobra 0914",
                        item["name"],
                        destino,
                    )
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    def test_baixar_arquivo_api_falha_quando_content_disposition_ausente(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "isDirectory": False,
        }
        sessao.post.return_value = FakeResponse(
            chunks=[b"abc"],
            headers={"Content-Type": "application/octet-stream"},
        )

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]):
                with self.assertRaisesRegex(download_api.UnexpectedApiResponseError, "Content-Disposition"):
                    download_api.baixar_arquivo_api(
                        sessao,
                        "Exportação Siscobra 0914",
                        item["name"],
                        destino,
                    )
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    def test_baixar_arquivo_api_falha_quando_download_retorna_vazio(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "isDirectory": False,
        }
        sessao.post.return_value = FakeResponse(
            chunks=[b""],
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Disposition": 'attachment; filename="Exportacao_Siscobra_0914_20260309.csv"',
            },
        )

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]):
                with self.assertRaisesRegex(download_api.UnexpectedApiResponseError, "corpo vazio|tamanho zero"):
                    download_api.baixar_arquivo_api(
                        sessao,
                        "Exportação Siscobra 0914",
                        item["name"],
                        destino,
                    )
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    def test_baixar_arquivo_api_trata_json_success_true_result_null_como_payload_incorreto(self) -> None:
        sessao = Mock()
        sessao.revo360_file_manager_endpoint = "https://nef.revo360.io/api/file-manager-file-system"
        item = {
            "name": "Exportacao_Siscobra_0914_20260309.csv",
            "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
            "isDirectory": False,
        }
        sessao.post.return_value = FakeResponse(
            payload={"success": True, "result": None},
            headers={"Content-Type": "application/json"},
            text='{"success": true, "result": null}',
        )

        base_tmp = ROOT_DIR / "downloads" / f"test_download_api_{uuid4().hex}"
        base_tmp.mkdir(parents=True, exist_ok=True)
        try:
            destino = base_tmp / item["name"]
            with patch.object(download_api, "_listar_arquivos_api_raw", return_value=[item]):
                with self.assertRaisesRegex(download_api.UnexpectedApiResponseError, "payload incorreto|pathInfoList"):
                    download_api.baixar_arquivo_api(
                        sessao,
                        "Exportação Siscobra 0914",
                        item["name"],
                        destino,
                    )
        finally:
            shutil.rmtree(base_tmp, ignore_errors=True)

    def test_extrair_data_nome_arquivo_retorna_date_quando_encontra_yyyymmdd(self) -> None:
        self.assertEqual(
            download_api.extrair_data_nome_arquivo("Exportacao_Siscobra_0914_20260309.csv"),
            date(2026, 3, 9),
        )
        self.assertIsNone(download_api.extrair_data_nome_arquivo("sem_data.csv"))

    def test_selecionar_csv_por_data_retorna_item_correto(self) -> None:
        itens = [
            {"name": "Exportacao_Siscobra_0914_20260308.csv"},
            {"name": "Exportacao_Siscobra_0914_20260309.csv"},
            {"name": "nao_e_csv.txt"},
        ]

        selecionado = download_api.selecionar_csv_por_data(itens, datetime(2026, 3, 9, 18, 40))

        self.assertEqual(selecionado, {"name": "Exportacao_Siscobra_0914_20260309.csv"})

    def test_selecionar_csv_por_data_falha_quando_nao_ha_match(self) -> None:
        with self.assertRaisesRegex(download_api.FileManagerApiError, "Nenhum CSV encontrado para a data 2026-03-09."):
            download_api.selecionar_csv_por_data(
                [{"name": "Exportacao_Siscobra_0914_20260308.csv"}],
                date(2026, 3, 9),
            )

    def test_listar_arquivos_api_no_browser_via_navegacao_parseia_json(self) -> None:
        list_url = (
            "https://nef.revo360.io:10024/api/file-manager-file-system"
            "?path=..%5CUPLOAD"
            "&command=GetDirContents"
            "&arguments=%7B%22pathInfo%22%3A+%5B%7B%22key%22%3A+%22Exporta%C3%A7%C3%A3o+Siscobra+0914%22%2C+%22name%22%3A+%22Exporta%C3%A7%C3%A3o+Siscobra+0914%22%7D%5D%7D"
        )
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [],
                }
            },
            "portal",
            navigation_overrides={
                list_url: {
                    "url": list_url,
                    "origin": "https://nef.revo360.io:10024",
                    "body_text": json.dumps(
                        {
                            "result": [
                                {
                                    "name": "Exportacao_Siscobra_0914_20260309.csv",
                                    "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                                    "dateModified": "2026-03-09T08:00:00",
                                    "isDirectory": False,
                                    "size": 123,
                                }
                            ]
                        },
                        ensure_ascii=False,
                    ),
                }
            },
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", "https://nef.revo360.io:10024"):
            itens = download_api.listar_arquivos_api_no_browser(driver, "Exportação Siscobra 0914")

        self.assertEqual(
            itens,
            [
                {
                    "name": "Exportacao_Siscobra_0914_20260309.csv",
                    "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                    "dateModified": "2026-03-09T08:00:00",
                    "isDirectory": False,
                    "size": 123,
                }
            ],
        )
        self.assertEqual(
            driver.get_calls,
            [
                "https://nef.revo360.io:10024",
                list_url,
            ],
        )

    def test_listar_arquivos_api_no_browser_falha_quando_navegacao_retorna_login(self) -> None:
        list_url = (
            "https://nef.revo360.io:10024/api/file-manager-file-system"
            "?path=..%5CUPLOAD"
            "&command=GetDirContents"
            "&arguments=%7B%22pathInfo%22%3A+%5B%7B%22key%22%3A+%22Exporta%C3%A7%C3%A3o+Siscobra+0914%22%2C+%22name%22%3A+%22Exporta%C3%A7%C3%A3o+Siscobra+0914%22%7D%5D%7D"
        )
        driver = FakeSeleniumDriver(
            {
                "portal": {
                    "url": "https://nef.revo360.io/",
                    "origin": "https://nef.revo360.io",
                    "cookies": [],
                }
            },
            "portal",
            navigation_overrides={
                list_url: {
                    "url": "https://nef.revo360.io:10024/login",
                    "origin": "https://nef.revo360.io:10024",
                    "body_text": "login required",
                }
            },
        )

        with patch.object(download_api, "FILE_MANAGER_API_BASE_URL", "https://nef.revo360.io:10024"):
            with self.assertRaisesRegex(download_api.SessionExpiredError, "autenticacao propria|login"):
                download_api.listar_arquivos_api_no_browser(driver, "Exportação Siscobra 0914")

    def test_baixar_arquivo_via_form_submit_no_browser_monta_payload_correto(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        driver.execute_script.return_value = {
            "submitted": True,
            "action": "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD",
            "fieldNames": ["command", "arguments"],
        }

        with patch.object(
            download_api,
            "garantir_contexto_file_manager_no_browser",
            return_value={
                "handle": "file_manager",
                "base_url": "https://nef.revo360.io:10024",
                "page_url": "https://nef.revo360.io:10024/",
                "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                "original_handle": "main",
                "created": False,
            },
        ):
            result = download_api.baixar_arquivo_via_form_submit_no_browser(
                driver,
                "Exportação Siscobra 0914",
                "Exportacao_Siscobra_0914_20260309.csv",
            )

        self.assertEqual(
            result["action"],
            "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD",
        )
        script_args = driver.execute_script.call_args.args
        self.assertEqual(script_args[0], download_api.BROWSER_DOWNLOAD_FORM_SUBMIT_SCRIPT)
        self.assertIn('form.target = "_self";', script_args[0])
        self.assertEqual(
            json.loads(script_args[2]["arguments"]),
            {
                "pathInfoList": [[
                    {
                        "key": "Exportação Siscobra 0914",
                        "name": "Exportação Siscobra 0914",
                    },
                    {
                        "key": r"Exportação Siscobra 0914\Exportacao_Siscobra_0914_20260309.csv",
                        "name": "Exportacao_Siscobra_0914_20260309.csv",
                    },
                ]]
            },
        )

    def test_baixar_arquivo_api_no_browser_falha_quando_download_nao_aparece(self) -> None:
        driver = Mock()
        driver.current_window_handle = "main"
        driver.switch_to.window.side_effect = lambda handle: setattr(driver, "current_window_handle", handle)
        driver.execute_script.return_value = {"submitted": True}

        with patch.object(
            download_api,
            "garantir_contexto_file_manager_no_browser",
            return_value={
                "handle": "file_manager",
                "base_url": "https://nef.revo360.io:10024",
                "page_url": "https://nef.revo360.io:10024/",
                "endpoint": "https://nef.revo360.io:10024/api/file-manager-file-system",
                "original_handle": "main",
                "created": False,
            },
        ):
            with patch.object(
                download_api,
                "_aguardar_download_em_diretorio",
                side_effect=RuntimeError("download nao apareceu"),
            ):
                with self.assertRaisesRegex(RuntimeError, "download nao apareceu"):
                    download_api.baixar_arquivo_api_no_browser(
                        driver,
                        "Exportação Siscobra 0914",
                        "Exportacao_Siscobra_0914_20260309.csv",
                        ROOT_DIR / "downloads" / "unused.csv",
                    )


if __name__ == "__main__":
    unittest.main()
