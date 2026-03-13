import json
import logging
import re
import unittest
from datetime import date
from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch

import main
from tests._workspace_temp import cleanup_workspace_dir, make_workspace_dir


class PipelineCheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base = make_workspace_dir("pipeline_checkpoint")
        self.log_dir = self.base / "logs"
        self.download_dir = self.base / "downloads"
        self.copy_dir = self.base / "copy"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.copy_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger("test-revo360-pipeline")
        self.logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        cleanup_workspace_dir(self.base)

    def _state_path(self, cycle_date: date | None = None) -> Path:
        current_cycle = cycle_date or date.today()
        return self.log_dir / f"state_{current_cycle.isoformat()}.json"

    def _last_success_path(self) -> Path:
        return self.log_dir / "last_success.txt"

    def _cycle_summary_path(self, cycle_date: date | None = None) -> Path:
        current_cycle = cycle_date or date.today()
        return self.log_dir / f"cycle_summary_{current_cycle.isoformat()}.json"

    def _common_patches(self):
        return [
            patch.object(main, "LOG_DIR", self.log_dir),
            patch.object(main, "LOCK_FILE_PATH", self.log_dir / "revo360.lock"),
            patch.object(main, "LAST_SUCCESS_FILE_PATH", self._last_success_path()),
            patch.object(main, "DOWNLOAD_DIR", self.download_dir),
            patch.object(main, "COPY_DIR", self.copy_dir),
            patch.object(main, "FILE_PREFIX", "LOCAL_TEST_"),
            patch.object(main, "ENABLE_COPIES", True),
            patch.object(main, "COPY_TO_SERVER", True),
            patch.object(main, "COPY_TO_FTP", True),
            patch.object(main, "RETRY_ON_FAILURE_ENABLED", True),
            patch.object(main, "RETRY_MAX_ATTEMPTS", 3),
            patch.object(main, "RETRY_DELAY_SECONDS", 0),
        ]

    def _read_state(self, cycle_date: date | None = None) -> dict:
        return json.loads(self._state_path(cycle_date).read_text(encoding="utf-8"))

    def _success_download(self, logger, state, cycle_date) -> None:
        path = self.download_dir / "Exportacao.csv"
        path.write_text(
            "h1;h2;h3\n"
            "descartar;xx;yy\n"
            "1;2;0001\n",
            encoding="utf-8",
        )
        state["paths"]["downloaded"] = str(path)
        state["source_signature"] = {"name": path.name, "size": path.stat().st_size, "mtime": int(path.stat().st_mtime)}

    def _success_prepare(self, logger, state, cycle_date) -> None:
        run_id = state.get("run_id", "000000")
        prepared = self.download_dir / f"LOCAL_TEST_{cycle_date.strftime('%Y%m%d')}_{run_id}.csv"
        prepared.write_text("1,2\n", encoding="utf-8")
        state["paths"]["prepared"] = str(prepared)

    def _success_download_multi(self, logger, state, cycle_date) -> None:
        source_id = state.get("source", {}).get("id", "unknown")
        path = self.download_dir / f"{source_id}_{cycle_date.strftime('%Y%m%d')}.csv"
        path.write_text(
            "h1;h2;h3\n"
            "descartar;xx;yy\n"
            "1;2;0001\n",
            encoding="utf-8",
        )
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

    def _success_prepare_multi(self, logger, state, cycle_date) -> None:
        source_id = state.get("source", {}).get("id", "unknown")
        run_id = state.get("run_id", "000000")
        prepared = self.download_dir / f"{source_id}_{cycle_date.strftime('%Y%m%d')}_{run_id}.csv"
        prepared.write_text("1,2\n", encoding="utf-8")
        state["paths"]["prepared"] = str(prepared)

    def test_a_success_first_attempt(self) -> None:
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            notify_mock = stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(notify_mock.call_count, 1)
        state = self._read_state()
        self.assertFalse(state["entered_retry"])
        self.assertTrue(self._last_success_path().exists())
        self.assertEqual(self._last_success_path().read_text(encoding="utf-8").strip(), date.today().isoformat())
        self.assertIn("items", state)
        self.assertEqual(len(state["items"]), 1)

    def test_b_download_fails_then_succeeds(self) -> None:
        calls = {"download": 0}

        def flaky_download(logger, state, cycle_date):
            calls["download"] += 1
            if calls["download"] == 1:
                raise RuntimeError("download falhou")
            self._success_download(logger, state, cycle_date)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=flaky_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            notify_mock = stack.enter_context(patch.object(main, "emitir_notificacoes"))
            stack.enter_context(patch.object(main, "sleep"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(notify_mock.call_count, 2)
        self.assertEqual(notify_mock.call_args_list[0].args[0]["notification_policy"], "initial")
        self.assertEqual(notify_mock.call_args_list[1].args[0]["notification_policy"], "final_recovered")

        state = self._read_state()
        self.assertTrue(state["entered_retry"])
        self.assertEqual(state["stages"]["download"]["tries"], 2)
        self.assertGreaterEqual(state["stages"]["prepare"]["tries"], 1)

    def test_c_retry_only_failed_destination(self) -> None:
        calls = {"server": 0, "ftp": 0}

        def flaky_server(logger, state):
            calls["server"] += 1
            if calls["server"] == 1:
                raise RuntimeError("server indisponivel")

        def ok_ftp(logger, state):
            calls["ftp"] += 1

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage", side_effect=flaky_server))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage", side_effect=ok_ftp))
            notify_mock = stack.enter_context(patch.object(main, "emitir_notificacoes"))
            stack.enter_context(patch.object(main, "sleep"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(notify_mock.call_count, 2)

        state = self._read_state()
        self.assertEqual(state["stages"]["send_server"]["tries"], 2)
        self.assertEqual(state["stages"]["send_ftp"]["tries"], 1)

        final_resumo = notify_mock.call_args_list[1].args[0]
        self.assertEqual(final_resumo["stages"]["send_server"]["tries"], 2)
        self.assertEqual(final_resumo["stages"]["send_ftp"]["tries"], 1)

    def test_d_notification_failure_does_not_trigger_retry(self) -> None:
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(
                patch.object(main, "emitir_notificacoes", side_effect=RuntimeError("notify down"))
            )
            sleep_mock = stack.enter_context(patch.object(main, "sleep"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(sleep_mock.call_count, 0)
        state = self._read_state()
        self.assertFalse(state["entered_retry"])
        self.assertEqual(state["stages"]["download"]["tries"], 1)
        self.assertEqual(state["stages"]["prepare"]["tries"], 1)
        self.assertEqual(state["stages"]["send_server"]["tries"], 1)
        self.assertEqual(state["stages"]["send_ftp"]["tries"], 1)

    def test_e_lock_existing_skips_pipeline(self) -> None:
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_has_success_for_today", return_value=False))
            run_mock = stack.enter_context(patch.object(main, "run_with_retries"))
            with main.instance_lock(self.logger) as acquired:
                self.assertTrue(acquired)
                status = main._run_once(self.logger, ignore_schedule=True, force_run=False)

        self.assertEqual(status, 0)
        run_mock.assert_not_called()

    def test_f_manual_cycle_ftp_only_uses_date_state_and_skips_server(self) -> None:
        cycle_date = date(2026, 2, 26)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            server_mock = stack.enter_context(patch.object(main, "_run_send_server_stage"))
            ftp_mock = stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(
                self.logger,
                force_run=False,
                cycle_date=cycle_date,
                targets={"server": False, "ftp": True},
            )

        self.assertEqual(status, 0)
        self.assertFalse(server_mock.called)
        self.assertEqual(ftp_mock.call_count, 1)
        state = self._read_state(cycle_date)
        self.assertEqual(state["date"], cycle_date.isoformat())
        self.assertEqual(state["targets"], {"server": False, "ftp": True})
        self.assertFalse(state["stages"]["send_server"]["enabled"])
        self.assertEqual(state["stages"]["send_server"]["tries"], 0)
        self.assertTrue(state["stages"]["send_ftp"]["enabled"])

    def test_g_resume_keeps_persisted_targets_even_with_new_flags(self) -> None:
        cycle_date = date(2026, 2, 25)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            server_first = stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage", side_effect=RuntimeError("ftp down")))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            first_status = main.run_with_retries(
                self.logger,
                cycle_date=cycle_date,
                targets={"server": False, "ftp": True},
            )

        self.assertEqual(first_status, 1)
        self.assertFalse(server_first.called)
        first_state = self._read_state(cycle_date)
        self.assertEqual(first_state["targets"], {"server": False, "ftp": True})

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            server_second = stack.enter_context(patch.object(main, "_run_send_server_stage"))
            ftp_second = stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            second_status = main.run_with_retries(
                self.logger,
                cycle_date=cycle_date,
                targets={"server": True, "ftp": False},
            )

        self.assertEqual(second_status, 0)
        self.assertFalse(server_second.called)
        self.assertEqual(ftp_second.call_count, 1)
        second_state = self._read_state(cycle_date)
        self.assertEqual(second_state["targets"], {"server": False, "ftp": True})
        self.assertFalse(second_state["stages"]["send_server"]["enabled"])

    def test_h_parser_validates_manual_date_and_target_flags(self) -> None:
        args = main.parse_args(["--run-anytime", "26-02-2026"])
        self.assertEqual(args.run_anytime, date(2026, 2, 26))
        self.assertFalse(args.local)
        self.assertFalse(args.ftp)

        with self.assertRaises(SystemExit):
            main.parse_args(["--run-anytime", "2026-02-26"])

        with self.assertRaises(SystemExit):
            main.parse_args(["--local"])

    def test_i_run_id_fixed_in_cycle_and_prepared_name_reused_on_retry(self) -> None:
        calls = {"server": 0}
        cycle_date = date(2026, 2, 25)

        def flaky_server(logger, state):
            calls["server"] += 1
            if calls["server"] == 1:
                raise RuntimeError("server indisponivel")

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_send_server_stage", side_effect=flaky_server))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            stack.enter_context(patch.object(main, "sleep"))
            status = main.run_with_retries(self.logger, cycle_date=cycle_date, force_run=False)

        self.assertEqual(status, 0)
        state = self._read_state(cycle_date)
        run_id = state.get("run_id")
        self.assertIsInstance(run_id, str)
        self.assertRegex(run_id, re.compile(r"^\d{6}$"))
        prepared_path = Path(state["paths"]["prepared"])
        self.assertEqual(
            prepared_path.name,
            f"LOCAL_TEST_{cycle_date.strftime('%Y%m%d')}_{run_id}.csv",
        )
        self.assertEqual(state["stages"]["prepare"]["tries"], 1)
        prepared_files = list(self.download_dir.glob(f"LOCAL_TEST_{cycle_date.strftime('%Y%m%d')}_*.csv"))
        self.assertEqual(len(prepared_files), 1)

    def test_j_success_initial_uses_normal_template_only(self) -> None:
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            whatsapp_mock = stack.enter_context(
                patch.object(
                    main,
                    "send_whatsapp_messages",
                    return_value={
                        "requested": True,
                        "total": 1,
                        "sent": 1,
                        "failed": 0,
                        "failures": [],
                    },
                )
            )
            chat_mock = stack.enter_context(patch.object(main, "enviar_resumo_google_chat"))
            email_mock = stack.enter_context(patch.object(main, "send_execution_email"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(whatsapp_mock.call_count, 1)
        self.assertEqual(chat_mock.call_count, 1)
        self.assertEqual(email_mock.call_count, 1)

        whatsapp_text = whatsapp_mock.call_args.args[0]
        chat_text = chat_mock.call_args.args[0]
        email_summary = email_mock.call_args.args[0]

        self.assertIn("Status por canal:", whatsapp_text)
        self.assertIn("✅", whatsapp_text)
        self.assertNotIn("Relatorio por etapa:", whatsapp_text)
        self.assertNotIn("Relatorio por etapa:", chat_text)
        self.assertNotIn("Relatorio por etapa:", email_summary.get("notification_text", ""))


    def test_k_multi_source_processes_independently_with_state_per_item(self) -> None:
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download_multi))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            notify_mock = stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 0)
        self.assertEqual(notify_mock.call_count, 2)
        notified_sources = [call.args[0].get("source_id") for call in notify_mock.call_args_list]
        self.assertCountEqual(notified_sources, ["source_a", "source_b"])

        state = self._read_state()
        self.assertIn("items", state)
        self.assertIn("source_a", state["items"])
        self.assertIn("source_b", state["items"])
        self.assertTrue(state["items"]["source_a"]["stages"]["download"]["ok"])
        self.assertTrue(state["items"]["source_b"]["stages"]["download"]["ok"])
        self.assertEqual(state["items"]["source_a"]["stages"]["download"]["tries"], 1)
        self.assertEqual(state["items"]["source_b"]["stages"]["download"]["tries"], 1)
        self.assertEqual(state["items"]["source_a"]["status"], "SUCCESS")
        self.assertEqual(state["items"]["source_b"]["status"], "SUCCESS")
        self.assertIn("paths", state["items"]["source_a"])
        self.assertIn("stages", state["items"]["source_a"])
        self.assertIn("last_error", state["items"]["source_a"])
        self.assertIn("timestamps", state["items"]["source_a"])
        self.assertIsInstance(state["items"]["source_a"]["timestamps"], dict)

    def test_n_multi_source_creates_individual_log_files(self) -> None:
        cycle_date = date(2026, 3, 11)
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download_multi))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(status, 0)
        log_a = self.log_dir / "sources" / f"{cycle_date.isoformat()}_source_a.log"
        log_b = self.log_dir / "sources" / f"{cycle_date.isoformat()}_source_b.log"
        self.assertTrue(log_a.exists())
        self.assertTrue(log_b.exists())
        self.assertGreater(log_a.stat().st_size, 0)
        self.assertGreater(log_b.stat().st_size, 0)

    def test_o_completed_source_is_skipped_on_next_run(self) -> None:
        cycle_date = date(2026, 3, 12)
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        called_sources = []

        def download_marking_source(logger, state, local_cycle_date):
            called_sources.append(state.get("source", {}).get("id"))
            self._success_download_multi(logger, state, local_cycle_date)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_marking_source))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            first_status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(first_status, 0)
        self.assertCountEqual(called_sources, ["source_a", "source_b"])

        state_path = self._state_path(cycle_date)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        for stage in ("download", "prepare", "send_server", "send_ftp"):
            state["items"]["source_a"]["stages"][stage]["ok"] = True
        state["items"]["source_a"]["status"] = "SUCCESS"
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

        second_called_sources = []

        def download_second_run(logger, state, local_cycle_date):
            second_called_sources.append(state.get("source", {}).get("id"))
            self._success_download_multi(logger, state, local_cycle_date)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_second_run))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            second_status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(second_status, 0)
        self.assertEqual(second_called_sources, [])

    def test_l_failure_in_one_source_does_not_stop_other_sources(self) -> None:
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        def download_by_source(logger, state, cycle_date):
            if state.get("source", {}).get("id") == "source_a":
                raise RuntimeError("arquivo nao encontrado")
            self._success_download_multi(logger, state, cycle_date)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_by_source))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            notify_mock = stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False)

        self.assertEqual(status, 1)
        self.assertEqual(notify_mock.call_count, 2)
        state = self._read_state()
        self.assertFalse(state["items"]["source_a"]["stages"]["download"]["ok"])
        self.assertTrue(state["items"]["source_b"]["stages"]["download"]["ok"])
        self.assertTrue(state["items"]["source_b"]["stages"]["prepare"]["ok"])

    def test_m_resume_retries_only_pending_source(self) -> None:
        cycle_date = date(2026, 3, 10)
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        calls = {"ftp_b": 0}

        def ftp_by_source(logger, state):
            if state.get("source", {}).get("id") == "source_b":
                calls["ftp_b"] += 1
                if calls["ftp_b"] == 1:
                    raise RuntimeError("ftp indisponivel")

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download_multi))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage", side_effect=ftp_by_source))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            first_status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(first_status, 1)
        first_state = self._read_state(cycle_date)
        self.assertEqual(first_state["items"]["source_a"]["stages"]["download"]["tries"], 1)
        self.assertEqual(first_state["items"]["source_b"]["stages"]["send_ftp"]["tries"], 1)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            download_mock = stack.enter_context(
                patch.object(main, "_run_download_stage", side_effect=self._success_download_multi)
            )
            prepare_mock = stack.enter_context(
                patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi)
            )
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage", side_effect=ftp_by_source))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            second_status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(second_status, 0)
        second_state = self._read_state(cycle_date)
        self.assertEqual(second_state["items"]["source_a"]["stages"]["download"]["tries"], 1)
        self.assertEqual(second_state["items"]["source_a"]["stages"]["prepare"]["tries"], 1)
        self.assertEqual(second_state["items"]["source_b"]["stages"]["send_ftp"]["tries"], 2)
        self.assertEqual(download_mock.call_count, 0)
        self.assertEqual(prepare_mock.call_count, 0)

    def test_p_cycle_summary_artifact_aggregates_metrics_by_source(self) -> None:
        cycle_date = date(2026, 3, 13)
        sources = [
            {
                "id": "source_a",
                "enabled": True,
                "remote_folder": "Pasta A",
                "filename_template": "A_{date:%Y%m%d}.csv",
                "prepared_prefix": "A_",
                "copy_dir": str(self.copy_dir / "a"),
                "ftp_dir": "/ftp/a",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_b",
                "enabled": True,
                "remote_folder": "Pasta B",
                "filename_template": "B_{date:%Y%m%d}.csv",
                "prepared_prefix": "B_",
                "copy_dir": str(self.copy_dir / "b"),
                "ftp_dir": "/ftp/b",
                "send_to_server": True,
                "send_to_ftp": True,
            },
            {
                "id": "source_c",
                "enabled": False,
                "remote_folder": "Pasta C",
                "filename_template": "C_{date:%Y%m%d}.csv",
                "prepared_prefix": "C_",
                "copy_dir": str(self.copy_dir / "c"),
                "ftp_dir": "/ftp/c",
                "send_to_server": True,
                "send_to_ftp": True,
            },
        ]

        def download_by_source(logger, state, local_cycle_date):
            source_id = state.get("source", {}).get("id")
            if source_id == "source_b":
                state.setdefault("file", {})
                state["file"]["expected_name"] = f"B_{local_cycle_date.strftime('%Y%m%d')}.csv"
                state["file"]["resolved_name"] = None
                state["file"]["found_in_listing"] = False
                raise RuntimeError("arquivo esperado nao encontrado")
            self._success_download_multi(logger, state, local_cycle_date)

        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "DOWNLOAD_SOURCES", sources))
            stack.enter_context(patch.object(main, "RETRY_MAX_ATTEMPTS", 1))
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=download_by_source))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare_multi))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(status, 1)
        summary_path = self._cycle_summary_path(cycle_date)
        self.assertTrue(summary_path.exists())
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["cycle_date"], cycle_date.isoformat())
        self.assertEqual(summary["status"], "FAILED")
        self.assertGreaterEqual(float(summary["duration_seconds"]), 0.0)
        self.assertEqual(summary["stats"]["sources_enabled"], 2)
        self.assertEqual(summary["stats"]["sources_success"], 1)
        self.assertEqual(summary["stats"]["sources_failed"], 0)
        self.assertEqual(summary["stats"]["sources_not_found"], 1)
        self.assertEqual(summary["stats"]["sources_disabled"], 1)

        by_id = {entry["source_id"]: entry for entry in summary["sources"]}
        self.assertEqual(by_id["source_a"]["result_category"], "success")
        self.assertEqual(by_id["source_b"]["result_category"], "not_found")
        self.assertEqual(by_id["source_b"]["final_stage"], "download")
        self.assertEqual(by_id["source_c"]["result_category"], "skipped")
        self.assertFalse(by_id["source_c"]["enabled"])

    def test_q_source_metrics_include_duration_in_state(self) -> None:
        cycle_date = date(2026, 3, 14)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(status, 0)
        state = self._read_state(cycle_date)
        first_item = next(iter(state["items"].values()))
        metrics = first_item.get("metrics", {})
        self.assertTrue(metrics.get("started_at"))
        self.assertTrue(metrics.get("finished_at"))
        self.assertIsNotNone(metrics.get("duration_seconds"))
        self.assertGreaterEqual(float(metrics["duration_seconds"]), 0.0)
        self.assertEqual(metrics.get("result_category"), "success")
        self.assertEqual(metrics.get("final_stage"), "send_ftp")

    def test_r_logs_structured_cycle_summary(self) -> None:
        cycle_date = date(2026, 3, 15)
        with ExitStack() as stack:
            for patcher in self._common_patches():
                stack.enter_context(patcher)
            stack.enter_context(patch.object(main, "_run_download_stage", side_effect=self._success_download))
            stack.enter_context(patch.object(main, "_run_prepare_stage", side_effect=self._success_prepare))
            stack.enter_context(patch.object(main, "_run_send_server_stage"))
            stack.enter_context(patch.object(main, "_run_send_ftp_stage"))
            stack.enter_context(patch.object(main, "emitir_notificacoes"))
            with self.assertLogs("test-revo360-pipeline", level="INFO") as captured:
                status = main.run_with_retries(self.logger, force_run=False, cycle_date=cycle_date)

        self.assertEqual(status, 0)
        joined_logs = "\n".join(captured.output)
        self.assertIn("Resumo final do ciclo por source (padrao observabilidade):", joined_logs)
        self.assertIn("Resumo do ciclo: status=SUCCESS", joined_logs)


if __name__ == "__main__":
    unittest.main()
