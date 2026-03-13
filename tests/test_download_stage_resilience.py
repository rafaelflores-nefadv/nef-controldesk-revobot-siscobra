import logging
import os
import unittest
from contextlib import ExitStack
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import main
from core import download as download_core
from tests._workspace_temp import cleanup_workspace_dir, make_workspace_dir


class DownloadStageResilienceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_dir = make_workspace_dir("download_stage")
        self.download_dir = self.base_dir / "downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("test-revo360-download-resilience")
        self.logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        cleanup_workspace_dir(self.base_dir)

    def test_cleanup_removes_expected_variants_and_temp_files(self) -> None:
        expected = self.download_dir / "Exportacao.csv"
        variant_1 = self.download_dir / "Exportacao (1).csv"
        variant_2 = self.download_dir / "Exportacao (2).csv"
        expected_temp = self.download_dir / "Exportacao.csv.crdownload"
        variant_temp = self.download_dir / "Exportacao (3).csv.crdownload"
        unrelated = self.download_dir / "Outro.csv"

        for path in (expected, variant_1, variant_2, expected_temp, variant_temp, unrelated):
            path.write_text("dummy", encoding="utf-8")

        with patch.object(download_core, "DOWNLOAD_DIR", self.download_dir):
            download_core.limpar_arquivos_download_anteriores("Exportacao.csv")

        self.assertFalse(expected.exists())
        self.assertFalse(variant_1.exists())
        self.assertFalse(variant_2.exists())
        self.assertFalse(expected_temp.exists())
        self.assertFalse(variant_temp.exists())
        self.assertTrue(unrelated.exists())

    def test_find_real_download_returns_latest_variant(self) -> None:
        variant_1 = self.download_dir / "Exportacao (1).csv"
        variant_2 = self.download_dir / "Exportacao (2).csv"
        variant_1.write_text("old", encoding="utf-8")
        variant_2.write_text("new", encoding="utf-8")

        old_ts = datetime(2026, 2, 26, 14, 0, 0).timestamp()
        new_ts = datetime(2026, 2, 26, 14, 5, 0).timestamp()
        os.utime(variant_1, (old_ts, old_ts))
        os.utime(variant_2, (new_ts, new_ts))

        with patch.object(download_core, "DOWNLOAD_DIR", self.download_dir):
            found = download_core.encontrar_download_real("Exportacao.csv")

        self.assertEqual(found, variant_2)

    def test_download_stage_uses_form_submit_and_waits_for_file_for_cycle_date(self) -> None:
        cycle_date = date(2026, 2, 26)
        nome_esperado = "Exportacao_Siscobra_0914_20260226.csv"
        state = {"paths": {"downloaded": None}, "source_signature": None}

        driver = Mock()
        wait = Mock()
        itens = [
            {"name": "Exportacao_Siscobra_0914_20260225.csv"},
            {"name": nome_esperado},
        ]
        destino = self.download_dir / nome_esperado

        def fake_disparar_download(_driver, _pasta, _nome_arquivo):
            self.assertEqual(_driver, driver)
            self.assertEqual(_nome_arquivo, nome_esperado)
            return {"action": "https://nef.revo360.io:10024/api/file-manager-file-system?path=..%5CUPLOAD"}

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            listar_mock = stack.enter_context(
                patch.object(main, "listar_arquivos_api_no_browser", return_value=itens)
            )
            clean_mock = stack.enter_context(
                patch.object(main, "limpar_arquivos_download_anteriores", wraps=main.limpar_arquivos_download_anteriores)
            )
            disparar_mock = stack.enter_context(
                patch.object(main, "baixar_arquivo_via_form_submit_no_browser", side_effect=fake_disparar_download)
            )
            def fake_aguardar_download(_wait, _nome_arquivo):
                self.assertEqual(_wait, wait)
                self.assertEqual(_nome_arquivo, nome_esperado)
                destino.write_text("h1;h2;h3\n1;2;3\n", encoding="utf-8")
                return destino

            aguardar_mock = stack.enter_context(
                patch.object(main, "aguardar_download", side_effect=fake_aguardar_download)
            )

            main._run_download_stage(self.logger, state, cycle_date)

        listar_mock.assert_called_once_with(driver, main.FILE_MANAGER_EXPORT_FOLDER)
        clean_mock.assert_called_once_with(nome_esperado)
        disparar_mock.assert_called_once_with(
            driver,
            main.FILE_MANAGER_EXPORT_FOLDER,
            nome_esperado,
        )
        aguardar_mock.assert_called_once_with(wait, nome_esperado)
        self.assertEqual(state["paths"]["downloaded"], str(destino))
        self.assertEqual(state["source_signature"]["name"], destino.name)
        self.assertTrue(Path(state["paths"]["downloaded"]).exists())
        driver.quit.assert_called_once()
        self.assertEqual([call[0] for call in driver.method_calls], ["quit"])

    def test_download_stage_fails_when_cycle_file_is_not_available(self) -> None:
        cycle_date = date(2026, 2, 27)
        state = {"paths": {"downloaded": None}, "source_signature": None}

        driver = Mock()
        wait = Mock()
        itens = [{"name": "Exportacao_Siscobra_0914_20260226.csv"}]

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            stack.enter_context(patch.object(main, "listar_arquivos_api_no_browser", return_value=itens))
            disparar_mock = stack.enter_context(patch.object(main, "baixar_arquivo_via_form_submit_no_browser"))

            with self.assertRaisesRegex(
                RuntimeError,
                "Arquivo CSV do ciclo 2026-02-27 ainda nao esta disponivel.*geracao do arquivo ainda nao terminou",
            ):
                main._run_download_stage(self.logger, state, cycle_date)

        self.assertIsNone(state["paths"]["downloaded"])
        self.assertIsNone(state["source_signature"])
        disparar_mock.assert_not_called()
        driver.quit.assert_called_once()

    def test_download_stage_fails_with_clear_message_when_session_is_expired(self) -> None:
        cycle_date = date(2026, 2, 27)
        state = {"paths": {"downloaded": None}, "source_signature": None}

        driver = Mock()
        wait = Mock()

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            stack.enter_context(
                patch.object(
                    main,
                    "listar_arquivos_api_no_browser",
                    side_effect=main.SessionExpiredError("redirecionado para login"),
                )
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "Sessao do navegador do REVO360 parece expirada ou a API nao ficou acessivel no contexto autenticado durante a listagem.",
            ):
                main._run_download_stage(self.logger, state, cycle_date)

        self.assertIsNone(state["paths"]["downloaded"])
        self.assertIsNone(state["source_signature"])
        driver.quit.assert_called_once()

    def test_download_stage_fails_when_file_does_not_appear_in_download_dir(self) -> None:
        cycle_date = date(2026, 2, 26)
        nome_esperado = "Exportacao_Siscobra_0914_20260226.csv"
        state = {"paths": {"downloaded": None}, "source_signature": None}

        driver = Mock()
        wait = Mock()
        itens = [{"name": nome_esperado}]

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            stack.enter_context(patch.object(main, "listar_arquivos_api_no_browser", return_value=itens))
            stack.enter_context(patch.object(main, "baixar_arquivo_via_form_submit_no_browser"))
            stack.enter_context(
                patch.object(
                    main,
                    "aguardar_download",
                    side_effect=RuntimeError("arquivo nao apareceu"),
                )
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "nao apareceu no diretorio de download",
            ):
                main._run_download_stage(self.logger, state, cycle_date)

        self.assertIsNone(state["paths"]["downloaded"])
        self.assertIsNone(state["source_signature"])
        driver.quit.assert_called_once()

    def test_download_stage_template_not_found_records_absence_and_does_not_download(self) -> None:
        cycle_date = date(2026, 3, 13)
        state = {
            "source": {
                "id": "source_template",
                "remote_folder": "Pasta Template",
                "filename_template": "Arquivo_{date:%Y%m%d}.csv",
            },
            "paths": {"downloaded": None},
            "source_signature": None,
            "file": {
                "expected_name": None,
                "resolved_name": None,
                "found_in_listing": None,
                "listed_count": 0,
                "listed_at": None,
            },
        }

        driver = Mock()
        wait = Mock()
        itens = [{"name": "Arquivo_20260312.csv"}, {"name": "Outro.csv"}]

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            listar_mock = stack.enter_context(
                patch.object(main, "listar_arquivos_api_no_browser", return_value=itens)
            )
            disparar_mock = stack.enter_context(
                patch.object(main, "baixar_arquivo_via_form_submit_no_browser")
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "Arquivo CSV do ciclo 2026-03-13 ainda nao esta disponivel",
            ):
                main._run_download_stage(self.logger, state, cycle_date)

        listar_mock.assert_called_once_with(driver, "Pasta Template")
        disparar_mock.assert_not_called()
        self.assertEqual(state["file"]["expected_name"], "Arquivo_20260313.csv")
        self.assertIsNone(state["file"]["resolved_name"])
        self.assertFalse(state["file"]["found_in_listing"])
        self.assertIsNone(state["paths"]["downloaded"])
        driver.quit.assert_called_once()

    def test_download_stage_template_match_triggers_download(self) -> None:
        cycle_date = date(2026, 3, 13)
        nome_esperado = "Arquivo_20260313.csv"
        state = {
            "source": {
                "id": "source_template",
                "remote_folder": "Pasta Template",
                "filename_template": "Arquivo_{date:%Y%m%d}.csv",
            },
            "paths": {"downloaded": None},
            "source_signature": None,
            "file": {
                "expected_name": None,
                "resolved_name": None,
                "found_in_listing": None,
                "listed_count": 0,
                "listed_at": None,
            },
        }

        driver = Mock()
        wait = Mock()
        itens = [{"name": "Arquivo_20260312.csv"}, {"name": nome_esperado}]
        destino = self.download_dir / nome_esperado

        def fake_aguardar_download(_wait, _nome_arquivo):
            self.assertEqual(_wait, wait)
            self.assertEqual(_nome_arquivo, nome_esperado)
            destino.write_text("h1;h2;h3\n1;2;3\n", encoding="utf-8")
            return destino

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(download_core, "DOWNLOAD_DIR", self.download_dir))
            stack.enter_context(patch.object(main, "criar_driver", return_value=(driver, wait)))
            stack.enter_context(patch.object(main, "realizar_login"))
            listar_mock = stack.enter_context(
                patch.object(main, "listar_arquivos_api_no_browser", return_value=itens)
            )
            disparar_mock = stack.enter_context(
                patch.object(main, "baixar_arquivo_via_form_submit_no_browser")
            )
            aguardar_mock = stack.enter_context(
                patch.object(main, "aguardar_download", side_effect=fake_aguardar_download)
            )

            main._run_download_stage(self.logger, state, cycle_date)

        listar_mock.assert_called_once_with(driver, "Pasta Template")
        disparar_mock.assert_called_once_with(driver, "Pasta Template", nome_esperado)
        aguardar_mock.assert_called_once_with(wait, nome_esperado)
        self.assertEqual(state["file"]["expected_name"], nome_esperado)
        self.assertEqual(state["file"]["resolved_name"], nome_esperado)
        self.assertTrue(state["file"]["found_in_listing"])
        self.assertEqual(state["paths"]["downloaded"], str(destino))
        driver.quit.assert_called_once()

    def test_prepare_stage_removes_only_header_and_preserves_first_data_row(self) -> None:
        cycle_date = date(2026, 3, 13)
        downloaded = self.download_dir / "downloaded.csv"
        downloaded.write_text(
            "col1;col2;col3;col4\n"
            "first_data;aa;999;x\n"
            "second_data;bb;000045;y\n",
            encoding="utf-8",
        )
        state = {
            "source": {"id": "prepare_source", "prepared_prefix": "PREP_"},
            "paths": {"downloaded": str(downloaded), "prepared": None},
        }

        with patch.object(main, "DOWNLOAD_DIR", self.download_dir):
            main._run_prepare_stage(self.logger, state, cycle_date)

        prepared = Path(state["paths"]["prepared"])
        self.assertTrue(prepared.exists())
        self.assertEqual(prepared.parent, self.download_dir)

        lines = prepared.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines[0], "first_data;aa;00000000999;x")
        self.assertEqual(lines[1], "second_data;bb;00000000045;y")
        self.assertEqual(len(lines), 2)


if __name__ == "__main__":
    unittest.main()
