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


class DownloadApiTests(unittest.TestCase):
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

if __name__ == "__main__":
    unittest.main()
