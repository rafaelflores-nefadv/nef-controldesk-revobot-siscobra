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
        driver = Mock()
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

        with patch.object(download_core, "listar_arquivos_api_no_browser", return_value=itens) as listar_mock:
            csvs = download_core.listar_csvs(driver, wait)

        listar_mock.assert_called_once_with(driver, download_core.FILE_MANAGER_EXPORT_FOLDER)
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

    def test_baixar_arquivo_dispara_form_submit_e_aguarda_arquivo(self) -> None:
        driver = Mock()
        wait = Mock()
        nome_arquivo = "Exportacao_Siscobra_0914_20260309.csv"
        destino = self.download_dir / nome_arquivo

        def fake_disparar(_driver, _pasta, _nome):
            self.assertEqual(_driver, driver)
            self.assertEqual(_pasta, download_core.FILE_MANAGER_EXPORT_FOLDER)
            self.assertEqual(_nome, nome_arquivo)
            return {"action": "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD"}

        with patch.object(download_core, "DOWNLOAD_DIR", self.download_dir):
            destino.write_text("h1;h2;h3\n1;2;3\n", encoding="utf-8")
            with patch.object(download_core, "baixar_arquivo_via_form_submit_no_browser", side_effect=fake_disparar) as disparar_mock:
                with patch.object(download_core, "aguardar_download", return_value=destino) as aguardar_mock:
                    caminho = download_core.baixar_arquivo(driver, wait, nome_arquivo)

        disparar_mock.assert_called_once_with(
            driver,
            download_core.FILE_MANAGER_EXPORT_FOLDER,
            nome_arquivo,
        )
        aguardar_mock.assert_called_once_with(wait, nome_arquivo)
        self.assertEqual(caminho, destino)
        self.assertTrue(destino.exists())

    def test_aguardar_download_valida_arquivo_local_sem_wait_do_selenium(self) -> None:
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
