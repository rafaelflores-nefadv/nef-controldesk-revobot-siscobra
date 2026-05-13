import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from core import download as download_core
from tests._workspace_temp import cleanup_workspace_dir, make_workspace_dir


class DownloadLegacyCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_dir = make_workspace_dir("download_legacy")
        self.download_dir = self.base_dir / "downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        cleanup_workspace_dir(self.base_dir)

    def test_listar_csvs_usa_api_e_retorna_formato_legado(self) -> None:
        session = Mock()
        wait = Mock()
        itens = [
            {
                "name": "Exportacao_Siscobra_0914_20260309.csv",
                "dateModified": "2026-03-09T08:00:00",
                "isDirectory": False,
            },
            {
                "name": "Subpasta",
                "dateModified": "2026-03-09T07:30:00",
                "isDirectory": True,
            },
            {
                "name": "nao_csv.txt",
                "dateModified": "2026-03-09T07:00:00",
                "isDirectory": False,
            },
        ]

        with patch.object(download_core, "listar_arquivos_api", return_value=itens) as listar_mock:
            csvs = download_core.listar_csvs(session, wait)

        listar_mock.assert_called_once_with(session, download_core.FILE_MANAGER_EXPORT_FOLDER)
        self.assertEqual(
            csvs,
            [
                {
                    "nome": "Exportacao_Siscobra_0914_20260309.csv",
                    "data_modificacao": "2026-03-09T08:00:00",
                }
            ],
        )
        self.assertEqual(wait.mock_calls, [])

    def test_baixar_arquivo_usa_api_http_direta(self) -> None:
        session = Mock()
        wait = Mock()
        nome_arquivo = "Exportacao_Siscobra_0914_20260309.csv"
        destino = self.download_dir / nome_arquivo

        def fake_download(_session, _pasta, _nome, _destino):
            self.assertEqual(_session, session)
            self.assertEqual(_pasta, download_core.FILE_MANAGER_EXPORT_FOLDER)
            self.assertEqual(_nome, nome_arquivo)
            self.assertEqual(_destino, destino)
            return destino

        with patch.object(download_core, "DOWNLOAD_DIR", self.download_dir):
            destino.write_text("h1;h2;h3\n1;2;3\n", encoding="utf-8")
            with patch.object(download_core, "baixar_exportacao_revo360", side_effect=fake_download) as download_mock:
                caminho = download_core.baixar_arquivo(session, wait, nome_arquivo)

        download_mock.assert_called_once_with(
            session,
            download_core.FILE_MANAGER_EXPORT_FOLDER,
            nome_arquivo,
            destino,
        )
        self.assertEqual(caminho, destino)
        self.assertTrue(destino.exists())

    def test_aguardar_download_valida_arquivo_local_sem_wait_externo(self) -> None:
        wait = Mock()
        nome_arquivo = "Exportacao_Siscobra_0914_20260309.csv"
        destino = self.download_dir / nome_arquivo
        destino.write_text("conteudo", encoding="utf-8")

        with patch.object(download_core, "DOWNLOAD_DIR", self.download_dir):
            validado = download_core.aguardar_download(wait, nome_arquivo)

        self.assertEqual(validado, destino)
        self.assertEqual(wait.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
