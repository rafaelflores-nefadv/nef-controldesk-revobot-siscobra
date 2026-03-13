import logging
import unittest
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import patch

import main
from tests._workspace_temp import cleanup_workspace_dir, make_workspace_dir


class CliSourceFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base = make_workspace_dir("cli_source_filter")
        self.log_dir = self.base / "logs"
        self.download_dir = self.base / "downloads"
        self.copy_dir = self.base / "copy"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.copy_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("test-revo360-cli-source-filter")
        self.logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        cleanup_workspace_dir(self.base)

    def _common_patches(self):
        return [
            patch.object(main, "LOG_DIR", self.log_dir),
            patch.object(main, "LOCK_FILE_PATH", self.log_dir / "revo360.lock"),
            patch.object(main, "LAST_SUCCESS_FILE_PATH", self.log_dir / "last_success.txt"),
            patch.object(main, "DOWNLOAD_DIR", self.download_dir),
            patch.object(main, "COPY_DIR", self.copy_dir),
            patch.object(main, "FILE_PREFIX", "LOCAL_TEST_"),
            patch.object(main, "ENABLE_COPIES", True),
            patch.object(main, "COPY_TO_SERVER", True),
            patch.object(main, "COPY_TO_FTP", True),
            patch.object(main, "RETRY_ON_FAILURE_ENABLED", True),
            patch.object(main, "RETRY_MAX_ATTEMPTS", 1),
            patch.object(main, "RETRY_DELAY_SECONDS", 0),
        ]

    def _download_ok(self, logger, state, cycle_date) -> None:
        source_id = state.get("source", {}).get("id", "unknown")
        path = self.download_dir / f"{source_id}_{cycle_date.strftime('%Y%m%d')}.csv"
        path.write_text("h1;h2;h3\nx;y;z\n1;2;3\n", encoding="utf-8")
        state["paths"]["downloaded"] = str(path)
        state["source_signature"] = {
            "name": path.name,
            "size": path.stat().st_size,
            "mtime": int(path.stat().st_mtime),
        }
        state.setdefault("file", {})
        state["file"]["expected_name"] = path.name
        state["file"]["resolved_name"] = path.name
        state["file"]["found_in_listing"] = True

    def _prepare_ok(self, logger, state, cycle_date) -> None:
        source_id = state.get("source", {}).get("id", "unknown")
        run_id = state.get("run_id", "000000")
        prepared = self.download_dir / f"{source_id}_{cycle_date.strftime('%Y%m%d')}_{run_id}.csv"
        prepared.write_text("1,2\n", encoding="utf-8")
        state["paths"]["prepared"] = str(prepared)

    def _sources(self) -> list[dict]:
        return [
            {
                "id": "siscobra_0914",
                "enabled": True,
                "remote_folder": "Exportacao Siscobra 0914",
                "filename_template": "Exportacao_Siscobra_0914_{date:%Y%m%d}.csv",
                "prepared_prefix": "S_0914_",
                "copy_dir": str(self.copy_dir / "0914"),
                "ftp_dir": "/ftp/0914",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "planalto",
                "enabled": True,
                "remote_folder": "Exportacao Planalto",
                "filename_template": "Exportacao_Planalto_{date:%Y%m%d}.csv",
                "prepared_prefix": "S_PLANALTO_",
                "copy_dir": str(self.copy_dir / "planalto"),
                "ftp_dir": "/ftp/planalto",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "federal",
                "enabled": True,
                "remote_folder": "Exportacao Federal",
                "filename_template": "Exportacao_Federal_{date:%Y%m%d}.csv",
                "prepared_prefix": "S_FEDERAL_",
                "copy_dir": str(self.copy_dir / "federal"),
                "ftp_dir": "/ftp/federal",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

    def test_run_anytime_single_source_executes_only_selected(self) -> None:
        called_sources: list[str] = []

        def download_side_effect(logger, state, cycle_date):
            called_sources.append(state.get("source", {}).get("id"))
            self._download_ok(logger, state, cycle_date)

        args = main.parse_args(["--run-anytime", "26-02-2026", "--source", "siscobra_0914"])
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", self._sources()))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_side_effect))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._prepare_ok))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.main(args=args, logger=self.logger, configure_logging=False)

        self.assertEqual(status, 0)
        self.assertEqual(called_sources, ["siscobra_0914"])

    def test_run_anytime_two_sources_executes_only_selected(self) -> None:
        called_sources: list[str] = []

        def download_side_effect(logger, state, cycle_date):
            called_sources.append(state.get("source", {}).get("id"))
            self._download_ok(logger, state, cycle_date)

        args = main.parse_args(
            [
                "--run-anytime",
                "26-02-2026",
                "--source",
                "siscobra_0914",
                "--source",
                "planalto",
            ]
        )
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", self._sources()))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_side_effect))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._prepare_ok))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.main(args=args, logger=self.logger, configure_logging=False)

        self.assertEqual(status, 0)
        self.assertCountEqual(called_sources, ["siscobra_0914", "planalto"])
        self.assertEqual(len(called_sources), 2)

    def test_run_anytime_unknown_source_raises_configuration_error(self) -> None:
        args = main.parse_args(["--run-anytime", "26-02-2026", "--source", "foo"])
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", self._sources()))
            run_download_mock = stack.enter_context(patch.object(main, "_run_download_stage"))
            with self.assertRaisesRegex(
                main.SourceConfigurationError,
                "Source\\(s\\) informado\\(s\\) nao encontrado\\(s\\): foo",
            ):
                main.main(args=args, logger=self.logger, configure_logging=False)

        run_download_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
