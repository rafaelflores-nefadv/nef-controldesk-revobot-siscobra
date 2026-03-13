import json
import logging
import unittest
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import patch

import main
from tests._workspace_temp import cleanup_workspace_dir, make_workspace_dir


class SourceConfigValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base = make_workspace_dir("source_config_validation")
        self.log_dir = self.base / "logs"
        self.download_dir = self.base / "downloads"
        self.copy_dir = self.base / "copy"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.copy_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("test-revo360-source-config")
        self.logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        cleanup_workspace_dir(self.base)

    def _state_path(self, cycle_date: date) -> Path:
        return self.log_dir / f"state_{cycle_date.isoformat()}.json"

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

    def _source(self, **overrides) -> dict:
        source = {
            "id": "source_a",
            "enabled": True,
            "remote_folder": "Pasta A",
            "filename_template": "A_{date:%Y%m%d}.csv",
            "prepared_prefix": "A_",
            "copy_dir": str(self.copy_dir / "a"),
            "ftp_dir": "/ftp/a",
            "send_to_server": True,
            "send_to_ftp": True,
        }
        source.update(overrides)
        return source

    def _success_download(self, logger, state, cycle_date) -> None:
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

    def _success_prepare(self, logger, state, cycle_date) -> None:
        source_id = state.get("source", {}).get("id", "unknown")
        run_id = state.get("run_id", "000000")
        prepared = self.download_dir / f"{source_id}_{cycle_date.strftime('%Y%m%d')}_{run_id}.csv"
        prepared.write_text("1,2\n", encoding="utf-8")
        state["paths"]["prepared"] = str(prepared)

    def test_duplicate_source_id_fails_fast(self) -> None:
        sources = [
            self._source(id="dup"),
            self._source(id="dup", remote_folder="Pasta B"),
        ]
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            run_download_mock = stack.enter_context(patch.object(main, "_run_download_stage"))
            with self.assertRaisesRegex(main.SourceConfigurationError, "id duplicado"):
                main.run_with_retries(self.logger, force_run=False)
        run_download_mock.assert_not_called()

    def test_missing_required_field_fails_fast(self) -> None:
        invalid = self._source()
        invalid.pop("remote_folder")
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", [invalid]))
            with self.assertRaisesRegex(main.SourceConfigurationError, "campo obrigatorio ausente 'remote_folder'"):
                main.run_with_retries(self.logger, force_run=False)

    def test_send_to_server_without_copy_dir_fails_fast(self) -> None:
        invalid = self._source(copy_dir=None, send_to_server=True)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", [invalid]))
            with self.assertRaisesRegex(main.SourceConfigurationError, "send_to_server=True exige copy_dir"):
                main.run_with_retries(self.logger, force_run=False)

    def test_send_to_ftp_without_ftp_dir_fails_fast(self) -> None:
        invalid = self._source(ftp_dir=None, send_to_ftp=True)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", [invalid]))
            with self.assertRaisesRegex(main.SourceConfigurationError, "send_to_ftp=True exige ftp_dir"):
                main.run_with_retries(self.logger, force_run=False)

    def test_invalid_template_fails_fast(self) -> None:
        invalid = self._source(filename_template="A_{data:%Y%m%d}.csv")
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", [invalid]))
            with self.assertRaisesRegex(main.SourceConfigurationError, "filename_template invalido"):
                main.run_with_retries(self.logger, force_run=False)

    def test_valid_multi_source_configuration_runs(self) -> None:
        cycle_date = date(2026, 3, 13)
        sources = [
            self._source(id="source_a"),
            self._source(
                id="source_b",
                remote_folder="Pasta B",
                filename_template="B_{date:%Y%m%d}.csv",
                prepared_prefix="B_",
                copy_dir=str(self.copy_dir / "b"),
                ftp_dir="/ftp/b",
            ),
        ]
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(status, 0)
        state = json.loads(self._state_path(cycle_date).read_text(encoding="utf-8"))
        self.assertIn("source_a", state["items"])
        self.assertIn("source_b", state["items"])
        self.assertEqual(state["items"]["source_a"]["status"], "SUCCESS")
        self.assertEqual(state["items"]["source_b"]["status"], "SUCCESS")

    def test_invalid_configuration_aborts_entire_execution(self) -> None:
        cycle_date = date(2026, 3, 14)
        invalid_sources = [
            self._source(id="source_ok"),
            self._source(id="source_bad", copy_dir=None, send_to_server=True),
        ]
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", invalid_sources))
            run_download_mock = stack.enter_context(
                patch.object(main, "_run_download_stage", side_effect=self._success_download)
            )
            with self.assertRaises(main.SourceConfigurationError):
                main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)
        run_download_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
